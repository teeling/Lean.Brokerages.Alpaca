/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using QuantConnect.Logging;
using QuantConnect.Orders;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Formal state machine for bracket order lifecycle.
    /// Replaces the previous boolean-flag approach (EntryFilled/ExitFilled/IsCancelled/IsComplete).
    /// Each state has a defined set of valid transitions — invalid transitions are logged as errors.
    /// </summary>
    public enum BracketState
    {
        /// <summary>
        /// Entry order submitted, awaiting fill/cancel. No shares held.
        /// Initial state returned by PlaceBracketOrder().
        /// </summary>
        EntryPending,

        /// <summary>
        /// Entry has partial fill(s). Shares are held but exit legs are "held" at Alpaca
        /// — position is unprotected. The plugin does NOT auto-place protective orders here;
        /// it waits for full fill (→ Protected) or cancel from strategy (→ Rescuing).
        /// </summary>
        EntryPartial,

        /// <summary>
        /// Entry fully filled (or rescue OCO placed for partial), stop + target orders
        /// are confirmed live at Alpaca. Position is safe.
        /// </summary>
        Protected,

        /// <summary>
        /// Cancel requested but awaiting confirmation. Has a 30-second timeout —
        /// if still in Cancelling after 30s, queries Alpaca REST directly.
        /// In live, a fill can still arrive while in this state (WS race with cancel ack).
        /// </summary>
        Cancelling,

        /// <summary>
        /// Partial fill detected during cancel. Old bracket being torn down,
        /// standalone OCO being placed for the filled shares. Transitional state with
        /// indefinite retry on OCO placement failure.
        /// </summary>
        Rescuing,

        /// <summary>
        /// Exit order filled (stop hit or target hit). Terminal.
        /// </summary>
        Closed,

        /// <summary>
        /// Bracket cancelled with no remaining shares. Terminal.
        /// </summary>
        Cancelled
    }

    /// <summary>
    /// Tracks a single bracket order group consisting of an entry order,
    /// a stop-loss exit leg, and a take-profit exit leg.
    ///
    /// Uses a formal state machine (<see cref="BracketState"/>) to track lifecycle.
    /// All state transitions are guarded and logged. Per-group locking via
    /// <see cref="StateLock"/> ensures thread-safe multi-field transitions.
    ///
    /// Returned by <see cref="BracketOrderManager.PlaceBracketOrder"/> and
    /// updated internally as order events arrive. The algorithm can query
    /// this object to check bracket state via <see cref="State"/>.
    /// </summary>
    public class BracketGroup
    {
        /// <summary>
        /// Valid state transitions. Used by <see cref="TryTransition"/> to validate requests.
        /// </summary>
        private static readonly Dictionary<BracketState, HashSet<BracketState>> ValidTransitions = new()
        {
            [BracketState.EntryPending] = new HashSet<BracketState>
            {
                BracketState.EntryPartial,  // partial fill
                BracketState.Protected,     // full fill (single fill event)
                BracketState.Cancelling,    // cancel requested (deferred or immediate)
                BracketState.Cancelled      // cancel confirmed immediately (reject, etc.)
            },
            [BracketState.EntryPartial] = new HashSet<BracketState>
            {
                BracketState.Protected,     // full fill arrived
                BracketState.Cancelling     // cancel requested (strategy or Layer 2b timer)
            },
            [BracketState.Protected] = new HashSet<BracketState>
            {
                BracketState.Closed,        // exit fill (stop or target hit)
                BracketState.Cancelled,     // both exit legs cancelled, no shares remaining
                BracketState.Rescuing       // exit legs cancelled but shares remain — rescue via standalone OCO
            },
            [BracketState.Cancelling] = new HashSet<BracketState>
            {
                BracketState.Cancelled,     // cancel confirmed (no fills held, or zero-fill cancel)
                BracketState.Rescuing,      // cancel confirmed but has filled shares → place OCO
                BracketState.EntryPartial,  // partial fill arrives (cancel lost the race, live only)
                BracketState.Protected      // full fill arrives (cancel lost the race, live only)
            },
            [BracketState.Rescuing] = new HashSet<BracketState>
            {
                BracketState.Protected,     // OCO placed and confirmed
                BracketState.Cancelled      // duplicate OCO detected at Alpaca — release tracking
            },
            // Terminal states — no valid transitions out
            [BracketState.Closed] = new HashSet<BracketState>(),
            [BracketState.Cancelled] = new HashSet<BracketState>()
        };

        /// <summary>
        /// Per-group lock for thread-safe state transitions. Multi-field mutations
        /// (State + timer + qty) must be atomic. Granular per-group to avoid blocking
        /// unrelated brackets.
        /// </summary>
        internal readonly object StateLock = new();

        // --- Identity ---

        /// <summary>
        /// Unique identifier for this bracket group.
        /// Generated by BracketOrderManager when the bracket is created.
        /// </summary>
        public string GroupId { get; }

        /// <summary>
        /// The symbol being traded in this bracket.
        /// </summary>
        public Symbol Symbol { get; }

        // --- State ---

        /// <summary>
        /// Current state in the bracket lifecycle state machine.
        /// Use this instead of the removed boolean flags (EntryFilled, ExitFilled, IsCancelled, IsComplete).
        /// </summary>
        public BracketState State { get; internal set; } = BracketState.EntryPending;

        /// <summary>
        /// True if the bracket is fully resolved (Closed or Cancelled). Terminal states.
        /// Replaces the old <c>IsComplete</c> property.
        /// </summary>
        public bool IsTerminal => State is BracketState.Closed or BracketState.Cancelled;

        // --- Quantities ---

        /// <summary>
        /// Signed entry quantity requested. Positive for long brackets, negative for short brackets.
        /// </summary>
        public decimal Quantity { get; }

        /// <summary>
        /// Absolute value of the requested quantity. Convenience for comparisons.
        /// </summary>
        public decimal RequestedQuantity => Math.Abs(Quantity);

        /// <summary>
        /// Accumulative signed fill quantity of the entry order so far.
        /// For partial fills this will be less than <see cref="Quantity"/> until fully filled.
        /// In live trading, Alpaca adjusts exit leg quantities to match this value.
        /// </summary>
        public decimal FilledQuantity { get; internal set; }

        /// <summary>
        /// Cumulative absolute quantity filled across both exit legs (stop + target partial/full fills).
        /// Used to compute remaining unprotected shares when exit legs are cancelled:
        /// remainingQty = Math.Abs(FilledQuantity) - ExitFilledQuantity.
        /// </summary>
        public decimal ExitFilledQuantity { get; internal set; }

        /// <summary>
        /// How many shares have active stop+target orders protecting them.
        /// May differ from FilledQuantity during rescue or Layer 2 re-creation.
        /// </summary>
        public decimal ProtectedQuantity { get; internal set; }

        // --- Prices ---

        /// <summary>
        /// Current stop-loss trigger price. May be updated via UpdateStop().
        /// Single source of truth — Layer 2 reads this for protective order re-creation.
        /// </summary>
        public decimal StopLossPrice { get; internal set; }

        /// <summary>
        /// Current take-profit limit price. May be updated via UpdateTarget().
        /// Single source of truth — Layer 2 reads this for protective order re-creation.
        /// </summary>
        public decimal TakeProfitPrice { get; internal set; }

        /// <summary>
        /// Optional stop-limit price for the stop-loss leg.
        /// If set, the stop-loss leg is a stop-limit order rather than a stop-market order.
        /// </summary>
        public decimal? StopLossLimitPrice { get; }

        /// <summary>
        /// Entry order type: Market, Limit, or StopLimit.
        /// </summary>
        public OrderType EntryType { get; }

        /// <summary>
        /// Stop trigger price for stop-limit entry orders. Null for Market/Limit entries.
        /// Updated by UpdateEntry when the entry stop price is changed.
        /// </summary>
        public decimal? EntryStopPrice { get; internal set; }

        /// <summary>
        /// Actual entry fill price. Set when the entry order fills.
        /// Weighted average for partial fills.
        /// </summary>
        public decimal? FillPrice { get; internal set; }

        /// <summary>
        /// LEAN order ID of the exit leg that filled. Useful for determining
        /// whether the stop or target triggered.
        /// </summary>
        public int? ExitOrderId { get; internal set; }

        /// <summary>
        /// Actual exit fill price. Set when an exit leg fills.
        /// </summary>
        public decimal? ExitPrice { get; internal set; }

        // --- Order tickets ---

        /// <summary>
        /// Entry order ticket. Available immediately after PlaceBracketOrder.
        /// </summary>
        public OrderTicket EntryTicket { get; internal set; }

        /// <summary>
        /// Stop-loss leg ticket. Populated by brokerage via RegisterLegTicket
        /// after the entry order fills (backtesting) or after Alpaca responds (live).
        /// </summary>
        public OrderTicket StopTicket { get; internal set; }

        /// <summary>
        /// Take-profit leg ticket. Populated by brokerage via RegisterLegTicket
        /// after the entry order fills (backtesting) or after Alpaca responds (live).
        /// </summary>
        public OrderTicket TargetTicket { get; internal set; }

        // --- Internal state (not exposed to strategy) ---

        /// <summary>
        /// LEAN order IDs of legs currently undergoing an Alpaca replace (update).
        /// Populated by BracketOrderManager when UpdateStop/UpdateTarget is called,
        /// entries removed when the UpdateSubmitted or terminal event arrives.
        /// Used by CancelBracket to avoid cancelling the sibling of a mid-replace leg.
        /// Uses ConcurrentDictionary for thread safety (accessed from algorithm + WS threads).
        /// </summary>
        internal readonly ConcurrentDictionary<int, byte> PendingUpdateOrderIds = new();

        /// <summary>
        /// True if CancelBracket was called while the entry order was still in "New"
        /// status (not yet ACK'd by the broker). LEAN rejects cancels on "New" orders,
        /// so the cancel is deferred: ProcessOrderEvent will fire CancelBracket again
        /// when the entry transitions to Submitted (or Filled).
        /// This is a sub-detail within the Cancelling state.
        /// </summary>
        internal bool PendingCancel { get; set; }

        /// <summary>
        /// Deferred entry update: set when UpdateEntry() is called while the entry order
        /// is still in "New" status. Replayed when entry transitions to Submitted.
        /// </summary>
        internal UpdateOrderFields PendingEntryUpdate { get; set; }

        // --- Partial fill timeout (Layer 2b) ---

        /// <summary>
        /// How long to wait after the first partial fill before auto-rescuing.
        /// Null means the strategy manages partial fills manually.
        /// </summary>
        internal TimeSpan? PartialFillTimeout { get; set; }

        /// <summary>
        /// When the partial fill timeout timer was started (first partial fill).
        /// </summary>
        internal DateTime? PartialFillTimerStart { get; set; }

        /// <summary>
        /// Number of cancel retry attempts during partial fill timeout rescue.
        /// Max 3 retries before giving up (Layer 1 reconciliation backstop).
        /// </summary>
        internal int PartialFillRetryCount { get; set; }

        /// <summary>
        /// The System.Threading.Timer for partial fill timeout. Stored so it can be
        /// cancelled on full fill or bracket completion.
        /// </summary>
        internal System.Threading.Timer PartialFillTimer { get; set; }

        // --- Layer 2 idempotency ---

        /// <summary>
        /// Client order ID for Layer 2 replacement stop order.
        /// Format: l2-{groupId8}-s-{seq}-{nonce6}
        /// Used to prevent duplicate placement via Alpaca client_order_id lookup.
        /// </summary>
        internal string Layer2StopClientOrderId { get; set; }

        /// <summary>
        /// Client order ID for Layer 2 replacement target order.
        /// Format: l2-{groupId8}-t-{seq}-{nonce6}
        /// </summary>
        internal string Layer2TargetClientOrderId { get; set; }

        /// <summary>
        /// Monotonically increasing counter for Layer 2 replacement orders.
        /// Each replacement attempt for the same leg increments this, ensuring
        /// unique client_order_ids even across multiple replacement cycles.
        /// </summary>
        internal int Layer2ReplacementSeq { get; set; }

        // --- Rescue retry (Rescuing state) ---

        /// <summary>
        /// Number of OCO placement retries in the Rescuing state.
        /// Exponential backoff: 5s, 10s, 20s, 40s, 60s, then every 60s.
        /// </summary>
        internal int RescueRetryCount { get; set; }

        /// <summary>
        /// Timer for rescue retry backoff.
        /// </summary>
        internal System.Threading.Timer RescueRetryTimer { get; set; }

        // --- Cancelling timeout ---

        /// <summary>
        /// When the bracket entered the Cancelling state. Used for the 30-second
        /// timeout that queries Alpaca REST directly if cancel ack never arrives.
        /// </summary>
        internal DateTime? CancellingEnteredAt { get; set; }

        /// <summary>
        /// Timer for the Cancelling state 30-second timeout.
        /// </summary>
        internal System.Threading.Timer CancellingTimeoutTimer { get; set; }

        // --- Timestamp for Layer 2 grace period ---

        /// <summary>
        /// When the bracket entered the Protected state. Used by Layer 2 timer-triggered
        /// checks for the 30-second grace window.
        /// </summary>
        internal DateTime? ProtectedEnteredAt { get; set; }

        /// <summary>
        /// One-shot guard: true if ReconcileProtectiveInvariantAsync has already
        /// attempted a rescue for this bracket. Prevents repeated rescue attempts
        /// on every reconciliation cycle.
        /// </summary>
        internal bool InvariantRescueAttempted { get; set; }

        /// <summary>
        /// True if the bracket entered Protected from Cancelling state (cancel-fill race:
        /// entry filled while CancelBracket was in flight). Used by ProcessExitEvent
        /// fast-path rescue to distinguish cancel-fill race (needs rescue) from EOD
        /// Liquidate (must NOT rescue). Only set at the Cancelling → Protected transition
        /// in ProcessEntryFilled. Never cleared — irrelevant once the group leaves Protected.
        /// </summary>
        internal bool EnteredProtectedFromCancelling { get; set; }

        // --- Constructor ---

        /// <summary>
        /// Creates a new BracketGroup to track a bracket order.
        /// </summary>
        /// <param name="groupId">Unique identifier for this group.</param>
        /// <param name="symbol">The symbol being traded.</param>
        /// <param name="quantity">Signed quantity (positive=long, negative=short).</param>
        /// <param name="stopLossPrice">Initial stop-loss trigger price.</param>
        /// <param name="takeProfitPrice">Initial take-profit limit price.</param>
        /// <param name="stopLossLimitPrice">Optional stop-limit price for the stop-loss leg.</param>
        /// <param name="entryType">Entry order type (Market, Limit, or StopLimit).</param>
        /// <param name="entryStopPrice">Stop trigger price for stop-limit entries. Null otherwise.</param>
        /// <param name="partialFillTimeout">How long to wait after first partial fill before auto-rescue. Null = manual.</param>
        public BracketGroup(
            string groupId,
            Symbol symbol,
            decimal quantity,
            decimal stopLossPrice,
            decimal takeProfitPrice,
            decimal? stopLossLimitPrice = null,
            OrderType entryType = OrderType.Market,
            decimal? entryStopPrice = null,
            TimeSpan? partialFillTimeout = null)
        {
            GroupId = groupId;
            Symbol = symbol;
            Quantity = quantity;
            StopLossPrice = stopLossPrice;
            TakeProfitPrice = takeProfitPrice;
            StopLossLimitPrice = stopLossLimitPrice;
            EntryType = entryType;
            EntryStopPrice = entryStopPrice;
            PartialFillTimeout = partialFillTimeout;
        }

        // --- State Machine ---

        /// <summary>
        /// Attempts a state transition. Validates the transition against the allowed
        /// transition table and logs the result. Must be called inside lock(StateLock).
        /// </summary>
        /// <param name="expectedFrom">The expected current state.</param>
        /// <param name="to">The target state.</param>
        /// <param name="reason">Human-readable reason for the transition (for logging).</param>
        /// <returns>True if the transition succeeded, false if invalid.</returns>
        internal bool TryTransition(BracketState expectedFrom, BracketState to, string reason = null)
        {
            if (State != expectedFrom)
            {
                Log.Error(
                    $"BracketGroup[{GroupId}] {Symbol}: TryTransition failed — " +
                    $"expected state {expectedFrom} but current state is {State}. " +
                    $"Requested transition to {to}. Reason: {reason ?? "none"}");
                return false;
            }

            if (!ValidTransitions.TryGetValue(State, out var allowed) || !allowed.Contains(to))
            {
                Log.Error(
                    $"BracketGroup[{GroupId}] {Symbol}: Invalid transition {State} → {to}. " +
                    $"Reason: {reason ?? "none"}");
                return false;
            }

            var fromState = State;
            State = to;

            if (to == BracketState.Protected)
            {
                ProtectedEnteredAt = DateTime.UtcNow;
            }

            Log.Trace(
                $"[BracketState] {GroupId[..Math.Min(8, GroupId.Length)]} {Symbol}: " +
                $"{fromState} → {to}" +
                (reason != null ? $" ({reason})" : ""));

            return true;
        }

        /// <summary>
        /// Attempts a state transition from the current state (whatever it is) to the target state.
        /// Use when the caller doesn't know or care about the current state but the transition
        /// should still be validated against the transition table.
        /// Must be called inside lock(StateLock).
        /// </summary>
        /// <param name="to">The target state.</param>
        /// <param name="reason">Human-readable reason for the transition (for logging).</param>
        /// <returns>True if the transition succeeded, false if invalid.</returns>
        internal bool TryTransitionFrom(BracketState to, string reason = null)
        {
            return TryTransition(State, to, reason);
        }

        /// <summary>
        /// Forces a state transition without validation. Used only by reconciliation
        /// corrections where the current state may be inconsistent. Logs distinctly.
        /// Must be called inside lock(StateLock).
        /// </summary>
        /// <param name="to">The target state.</param>
        /// <param name="reason">Reason for the forced transition.</param>
        internal void ForceState(BracketState to, string reason)
        {
            var fromState = State;
            State = to;

            if (to == BracketState.Protected)
            {
                ProtectedEnteredAt = DateTime.UtcNow;
            }

            Log.Trace(
                $"[BracketState:FORCED] {GroupId[..Math.Min(8, GroupId.Length)]} {Symbol}: " +
                $"{fromState} → {to} ({reason})");
        }

        /// <summary>
        /// Disposes timers associated with this bracket group.
        /// Should be called when the group reaches a terminal state.
        /// </summary>
        internal void DisposeTimers()
        {
            PartialFillTimer?.Dispose();
            PartialFillTimer = null;

            RescueRetryTimer?.Dispose();
            RescueRetryTimer = null;

            CancellingTimeoutTimer?.Dispose();
            CancellingTimeoutTimer = null;
        }

        /// <summary>
        /// Returns a human-readable summary of this bracket group's state.
        /// </summary>
        public override string ToString()
        {
            var qtyInfo = FilledQuantity != 0 && FilledQuantity != Quantity
                ? $"qty={FilledQuantity}/{Quantity}" : $"qty={Quantity}";
            var entryInfo = EntryType == OrderType.StopLimit
                ? $"entry=stop-limit(stop={EntryStopPrice})"
                : $"entry={EntryType.ToString().ToLower()}";
            var g8 = GroupId.Length >= 8 ? GroupId[..8] : GroupId;
            return $"BracketGroup[{g8}] {Symbol} {qtyInfo} {entryInfo} " +
                   $"stop={StopLossPrice} target={TakeProfitPrice} state={State}";
        }
    }
}
