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
using System.Linq;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Newtonsoft.Json;
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantConnect.Interfaces;
using QuantConnect.Logging;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Algorithm-facing API for bracket orders. Provides a clean single-call
    /// interface for placing bracket orders (entry + stop-loss + take-profit),
    /// and tracks bracket group state via a formal state machine.
    ///
    /// The plugin guarantees:
    /// - Every filled position has active protective orders (Layer 2)
    /// - Missed WebSocket events are reconciled within 60s (Layer 1)
    /// - Partial fill rescue is atomic (Layer 2b)
    ///
    /// The strategy declares intent; the plugin guarantees execution and safety.
    ///
    /// Usage from Python:
    /// <code>
    /// from QuantConnect.Brokerages.Alpaca import BracketOrderManager, BracketState
    /// self.bracket = BracketOrderManager(self)
    /// group = self.bracket.PlaceBracketOrder(symbol, 100, 195.0, 210.0)
    /// if group.State == BracketState.Protected: ...
    /// </code>
    /// </summary>
    public class BracketOrderManager
    {
        /// <summary>
        /// Plugin version. Logged at startup by both live and backtesting brokerages
        /// to identify which code produced a given log/backtest.
        /// </summary>
        public const string PluginVersion = "0.5.1";

        private readonly IAlgorithm _algorithm;

        /// <summary>
        /// Reference to the brokerage, set via <see cref="RegisterBrokerage"/>.
        /// Used for emitting plugin messages via OnMessage.
        /// </summary>
        internal Brokerage Brokerage { get; set; }

        /// <summary>
        /// Delegate to check if an order with a given client_order_id already exists at Alpaca.
        /// Set by the brokerage during manager registration. Used for rescue OCO idempotency:
        /// before placing a rescue OCO, check if a previous attempt already succeeded at Alpaca
        /// (e.g., the POST succeeded but the response was lost due to a network error).
        /// Returns true if ANY order with that client ID exists (open or terminal).
        /// A terminal order still means "don't place a duplicate."
        /// </summary>
        internal Func<string, bool> CheckOrderExistsByClientId { get; set; }

        /// <summary>
        /// All bracket groups, keyed by group ID.
        /// </summary>
        private readonly ConcurrentDictionary<string, BracketGroup> _groups = new();

        /// <summary>
        /// Maps LEAN order ID → bracket group ID for fast event routing.
        /// Contains entries for the entry order AND both exit legs.
        /// </summary>
        private readonly ConcurrentDictionary<int, string> _orderToGroup = new();

        /// <summary>
        /// Maps Symbol → active (non-terminal) bracket group ID for O(1) symbol lookup.
        /// Maintained on place/complete. Only one active bracket per symbol at a time.
        /// </summary>
        private readonly ConcurrentDictionary<Symbol, string> _symbolToGroup = new();

        /// <summary>
        /// Pending partial-fill rescues. Maps old (cancelled) group ID to rescue state.
        /// Populated internally by rescue flows, consumed by <see cref="CheckPendingRescue"/>
        /// when all old bracket orders are confirmed canceled.
        /// </summary>
        private readonly ConcurrentDictionary<string, PendingRescue> _pendingRescues = new();

        /// <summary>
        /// Creates a new BracketOrderManager.
        /// The algorithm should create this in Initialize() and store it as a field.
        /// </summary>
        /// <param name="algorithm">The algorithm instance (pass 'this' from the algorithm).</param>
        public BracketOrderManager(IAlgorithm algorithm)
        {
            _algorithm = algorithm ?? throw new ArgumentNullException(nameof(algorithm));
            Log.Debug("BracketOrderManager: Initialized. Order events will be forwarded by the brokerage.");
        }

        /// <summary>
        /// Called by the brokerage to establish the bidirectional link for plugin messages.
        /// </summary>
        internal void RegisterBrokerage(Brokerage brokerage)
        {
            Brokerage = brokerage;
        }

        #region Public API — Algorithm-Facing Methods

        /// <summary>
        /// Places a bracket order: an entry order with linked stop-loss and take-profit exit legs.
        /// The brokerage layer handles all bracket semantics (leg creation, OCO cancellation).
        ///
        /// Returns a BracketGroup in EntryPending state. The plugin handles the full lifecycle:
        ///   - Entry fills → Protected (exits go live)
        ///   - Partial fill + timeout → automatic rescue (Rescuing → Protected)
        ///   - Missed WS events → reconciliation catches within 60s
        ///   - Missing protective orders → Layer 2 re-creates them
        /// </summary>
        /// <param name="symbol">The symbol to trade.</param>
        /// <param name="quantity">Signed quantity. Positive for long, negative for short.</param>
        /// <param name="stopLossPrice">Stop-loss trigger price.</param>
        /// <param name="takeProfitPrice">Take-profit limit price.</param>
        /// <param name="entryType">Entry order type: Market (default), Limit, or StopLimit.</param>
        /// <param name="entryLimitPrice">Required if entryType is Limit or StopLimit.</param>
        /// <param name="entryStopPrice">Required if entryType is StopLimit.</param>
        /// <param name="stopLossLimitPrice">Optional: makes the stop-loss a stop-limit order.</param>
        /// <param name="partialFillTimeout">How long to wait after first partial fill before auto-rescue. Null = manual.</param>
        /// <param name="tag">Optional order tag.</param>
        /// <returns>A <see cref="BracketGroup"/> in EntryPending state.</returns>
        public BracketGroup PlaceBracketOrder(
            Symbol symbol,
            decimal quantity,
            decimal stopLossPrice,
            decimal takeProfitPrice,
            OrderType entryType = OrderType.Market,
            decimal? entryLimitPrice = null,
            decimal? entryStopPrice = null,
            decimal? stopLossLimitPrice = null,
            TimeSpan? partialFillTimeout = null,
            string tag = "")
        {
            // Round all prices to Alpaca's maximum precision
            stopLossPrice    = RoundPrice(stopLossPrice, nameof(stopLossPrice));
            takeProfitPrice  = RoundPrice(takeProfitPrice, nameof(takeProfitPrice));
            if (entryLimitPrice.HasValue)   entryLimitPrice   = RoundPrice(entryLimitPrice.Value,   nameof(entryLimitPrice));
            if (entryStopPrice.HasValue)    entryStopPrice    = RoundPrice(entryStopPrice.Value,    nameof(entryStopPrice));
            if (stopLossLimitPrice.HasValue) stopLossLimitPrice = RoundPrice(stopLossLimitPrice.Value, nameof(stopLossLimitPrice));

            // Validate all parameters
            ValidateBracketParameters(symbol, quantity, stopLossPrice,
                takeProfitPrice, entryType, entryLimitPrice, entryStopPrice);

            // Guard: reject if symbol already has a bracket in a non-transitional state.
            // Cancelling/Cancelled are allowed — the old bracket is being torn down (e.g., during replace()).
            var existing = GetActiveGroupBySymbol(symbol);
            if (existing != null && existing.State != BracketState.Cancelling)
            {
                Log.Error($"BracketOrderManager.PlaceBracketOrder: Symbol {symbol} already has active bracket " +
                    $"{existing.GroupId} in state {existing.State}. Cannot place a second bracket. " +
                    $"Cancel the existing bracket first.");
                throw new InvalidOperationException(
                    $"Symbol {symbol} already has active bracket {existing.GroupId} in state {existing.State}.");
            }

            // Create the bracket group in EntryPending state
            var groupId = Guid.NewGuid().ToString("N");
            var group = new BracketGroup(groupId, symbol, quantity,
                stopLossPrice, takeProfitPrice, stopLossLimitPrice,
                entryType, entryStopPrice, partialFillTimeout);
            _groups[groupId] = group;
            _symbolToGroup[symbol] = groupId;

            Log.Debug($"BracketOrderManager.PlaceBracketOrder: Created group {groupId} " +
                $"for {symbol} qty={quantity} stop={stopLossPrice} target={takeProfitPrice} " +
                $"entryType={entryType} entryStop={entryStopPrice} stopLimitPrice={stopLossLimitPrice} " +
                $"partialFillTimeout={partialFillTimeout}");

            // Build bracket properties for the brokerage to detect
            var props = new AlpacaBracketOrderProperties
            {
                BracketGroupId = groupId,
                TakeProfitLimitPrice = takeProfitPrice,
                StopLossStopPrice = stopLossPrice,
                StopLossLimitPrice = stopLossLimitPrice,
                OriginatingManager = this,
            };

            // Place the entry order through LEAN's SubmitOrderRequest API
            var entryTag = string.IsNullOrEmpty(tag) ? $"bracket:{groupId}:entry" : tag;
            SubmitOrderRequest request;
            switch (entryType)
            {
                case OrderType.Market:
                    request = new SubmitOrderRequest(OrderType.Market, symbol.SecurityType,
                        symbol, quantity, 0, 0, _algorithm.UtcTime, entryTag, props);
                    break;

                case OrderType.Limit:
                    request = new SubmitOrderRequest(OrderType.Limit, symbol.SecurityType,
                        symbol, quantity, 0, entryLimitPrice.Value, _algorithm.UtcTime, entryTag, props);
                    break;

                case OrderType.StopLimit:
                    request = new SubmitOrderRequest(OrderType.StopLimit, symbol.SecurityType,
                        symbol, quantity, entryStopPrice.Value, entryLimitPrice.Value,
                        _algorithm.UtcTime, entryTag, props);
                    break;

                default:
                    throw new NotSupportedException(
                        $"BracketOrderManager: Entry type {entryType} not supported. Use Market, Limit, or StopLimit.");
            }

            var entryTicket = _algorithm.SubmitOrderRequest(request);

            // Register the entry ticket with the group
            group.EntryTicket = entryTicket;
            _orderToGroup[entryTicket.OrderId] = groupId;

            Log.Debug($"BracketOrderManager.PlaceBracketOrder: Entry ticket ID={entryTicket.OrderId} " +
                $"registered for group {groupId}. Entry status: {entryTicket.Status}");

            // Handle synchronous rejection: if SubmitOrderRequest triggered an Invalid event
            // during processing, ProcessOrderEvent may have missed it because _orderToGroup
            // wasn't populated yet (it's populated at line above, AFTER SubmitOrderRequest).
            // Detect this by checking the ticket status and transition to Cancelled.
            // The !IsTerminal guard prevents double-transition if ProcessOrderEvent did handle it.
            if (entryTicket.Status == OrderStatus.Invalid)
            {
                Log.Error($"BracketOrderManager.PlaceBracketOrder: Entry order rejected for group {groupId}. " +
                    $"Marking bracket as Cancelled.");
                lock (group.StateLock)
                {
                    if (!group.IsTerminal)
                    {
                        group.ForceState(BracketState.Cancelled,
                            $"entry order rejected: {entryTicket.Status}");
                        OnGroupTerminal(group);
                    }
                }
            }

            return group;
        }

        /// <summary>
        /// Cancels a bracket whose entry has not fully filled.
        /// State-guarded:
        /// - EntryPending (no fills): cancels entry → Cancelling → Cancelled
        /// - EntryPartial (has fills): cancels remaining entry → Cancelling → Rescuing → Protected
        /// - Protected: REJECTS (return false). Use UpdateTarget to force exit.
        /// - Cancelling/Rescuing: REJECTS (operation in progress).
        /// - Closed/Cancelled: REJECTS (already terminal).
        /// </summary>
        /// <param name="groupId">The bracket group ID to cancel.</param>
        /// <returns>True if a cancel request was submitted, false if rejected.</returns>
        public bool CancelBracket(string groupId)
        {
            if (!_groups.TryGetValue(groupId, out var group))
            {
                Log.Error($"BracketOrderManager.CancelBracket: Group {groupId} not found.");
                return false;
            }

            lock (group.StateLock)
            {
                switch (group.State)
                {
                    case BracketState.EntryPending:
                    case BracketState.EntryPartial:
                        // Valid — proceed with cancel
                        break;

                    case BracketState.Protected:
                        Log.Error($"BracketOrderManager.CancelBracket: REJECTED for group {groupId} — " +
                            $"bracket is in Protected state. Use UpdateTarget() to force exit on filled brackets.");
                        return false;

                    case BracketState.Cancelling:
                        Log.Error($"BracketOrderManager.CancelBracket: REJECTED for group {groupId} — " +
                            $"cancel already in progress (Cancelling state).");
                        return false;

                    case BracketState.Rescuing:
                        Log.Error($"BracketOrderManager.CancelBracket: REJECTED for group {groupId} — " +
                            $"rescue in progress (Rescuing state).");
                        return false;

                    case BracketState.Closed:
                    case BracketState.Cancelled:
                        Log.Debug($"BracketOrderManager.CancelBracket: Group {groupId} already terminal ({group.State}).");
                        return false;
                }

                // Transition to Cancelling
                if (!group.TryTransition(group.State, BracketState.Cancelling, "CancelBracket requested"))
                {
                    return false;
                }
                group.CancellingEnteredAt = DateTime.UtcNow;

                // Cancel the entry order inside the lock so PendingCancel is set atomically
                // with the state transition. Cancel() just submits to LEAN's transaction queue.
                var entryTicket = group.EntryTicket;
                if (entryTicket == null)
                {
                    Log.Error($"BracketOrderManager.CancelBracket: Entry ticket is null for group {groupId}");
                    return false;
                }

                if (entryTicket.Status == OrderStatus.Invalid)
                {
                    // Entry was already rejected — nothing to cancel, go straight to Cancelled
                    Log.Debug($"BracketOrderManager.CancelBracket: Entry already Invalid for group {groupId}. " +
                        $"Skipping cancel, transitioning directly to Cancelled.");
                    group.TryTransition(BracketState.Cancelling, BracketState.Cancelled,
                        "entry already Invalid, nothing to cancel");
                    OnGroupTerminal(group);
                    return true;
                }
                else if (entryTicket.Status == OrderStatus.New)
                {
                    Log.Debug($"BracketOrderManager.CancelBracket: Entry ticket {entryTicket.OrderId} " +
                        $"still in New status for group {groupId}. Setting PendingCancel for deferred retry.");
                    group.PendingCancel = true;
                }
                else
                {
                    Log.Debug($"BracketOrderManager.CancelBracket: Cancelling entry ticket {entryTicket.OrderId} for group {groupId}");
                    entryTicket.Cancel($"Bracket cancelled: {groupId}");
                }
            }

            // Start a 30-second timeout timer for Cancelling state
            StartCancellingTimeout(group);

            return true;
        }

        /// <summary>
        /// Updates the stop-loss price for an active bracket.
        /// Requires state: Protected. Rejects if not in Protected state.
        /// </summary>
        /// <param name="groupId">The bracket group ID.</param>
        /// <param name="newStopPrice">The new stop trigger price.</param>
        /// <param name="newLimitPrice">Optional: new limit price for stop-limit orders.</param>
        /// <returns>True if the update was submitted successfully.</returns>
        public bool UpdateStop(string groupId, decimal newStopPrice, decimal? newLimitPrice = null)
        {
            if (!_groups.TryGetValue(groupId, out var group))
            {
                Log.Error($"BracketOrderManager.UpdateStop: Group {groupId} not found.");
                return false;
            }

            if (group.State != BracketState.Protected)
            {
                Log.Error($"BracketOrderManager.UpdateStop: Rejected for group {groupId} — " +
                    $"state is {group.State}, requires Protected.");
                return false;
            }

            if (group.StopTicket == null || group.StopTicket.Status.IsClosed())
            {
                Log.Error($"BracketOrderManager.UpdateStop: Stop ticket is null or closed for group {groupId}.");
                return false;
            }

            // Guard: stop must be strictly below the current target price
            if (group.TakeProfitPrice > 0 && newStopPrice >= group.TakeProfitPrice)
            {
                Log.Error($"BracketOrderManager.UpdateStop: Rejected for group {groupId} — " +
                    $"newStopPrice {newStopPrice} >= takeProfitPrice {group.TakeProfitPrice}.");
                return false;
            }

            Log.Debug($"BracketOrderManager.UpdateStop: Updating stop for group {groupId} " +
                $"from {group.StopLossPrice} to {newStopPrice}" +
                (newLimitPrice.HasValue ? $" (limitPrice={newLimitPrice})" : ""));

            var fields = new UpdateOrderFields { StopPrice = newStopPrice };
            if (newLimitPrice.HasValue)
            {
                fields.LimitPrice = newLimitPrice.Value;
            }

            // Track pending replace for sibling-lock avoidance
            group.PendingUpdateOrderIds[group.StopTicket.OrderId] = 0;

            var response = group.StopTicket.Update(fields);

            if (response.IsSuccess)
            {
                group.StopLossPrice = newStopPrice;
                Log.Debug($"BracketOrderManager.UpdateStop: Successfully updated stop for group {groupId} to {newStopPrice}");
            }
            else
            {
                group.PendingUpdateOrderIds.TryRemove(group.StopTicket.OrderId, out _);
                Log.Error($"BracketOrderManager.UpdateStop: Failed for group {groupId}. " +
                    $"Response: {response.ErrorCode} - {response.ErrorMessage}");
            }

            return response.IsSuccess;
        }

        /// <summary>
        /// Updates the take-profit price for an active bracket.
        /// Requires state: Protected. Rejects if not in Protected state.
        /// </summary>
        /// <param name="groupId">The bracket group ID.</param>
        /// <param name="newLimitPrice">The new take-profit limit price.</param>
        /// <returns>True if the update was submitted successfully.</returns>
        public bool UpdateTarget(string groupId, decimal newLimitPrice)
        {
            if (!_groups.TryGetValue(groupId, out var group))
            {
                Log.Error($"BracketOrderManager.UpdateTarget: Group {groupId} not found.");
                return false;
            }

            if (group.State != BracketState.Protected)
            {
                Log.Error($"BracketOrderManager.UpdateTarget: Rejected for group {groupId} — " +
                    $"state is {group.State}, requires Protected.");
                return false;
            }

            if (group.TargetTicket == null || group.TargetTicket.Status.IsClosed())
            {
                Log.Error($"BracketOrderManager.UpdateTarget: Target ticket is null or closed for group {groupId}.");
                return false;
            }

            // Guard: target must be strictly above the current stop price
            if (group.StopLossPrice > 0 && newLimitPrice <= group.StopLossPrice)
            {
                Log.Error($"BracketOrderManager.UpdateTarget: Rejected for group {groupId} — " +
                    $"newTargetPrice {newLimitPrice} <= stopLossPrice {group.StopLossPrice}.");
                return false;
            }

            Log.Debug($"BracketOrderManager.UpdateTarget: Updating target for group {groupId} " +
                $"from {group.TakeProfitPrice} to {newLimitPrice}");

            var fields = new UpdateOrderFields { LimitPrice = newLimitPrice };
            group.PendingUpdateOrderIds[group.TargetTicket.OrderId] = 0;

            var response = group.TargetTicket.Update(fields);

            if (response.IsSuccess)
            {
                group.TakeProfitPrice = newLimitPrice;
                Log.Debug($"BracketOrderManager.UpdateTarget: Successfully updated target for group {groupId} to {newLimitPrice}");
            }
            else
            {
                group.PendingUpdateOrderIds.TryRemove(group.TargetTicket.OrderId, out _);
                Log.Error($"BracketOrderManager.UpdateTarget: Failed for group {groupId}. " +
                    $"Response: {response.ErrorCode} - {response.ErrorMessage}");
            }

            return response.IsSuccess;
        }

        /// <summary>
        /// Updates the entry order price(s) for an active bracket.
        /// Only valid for Limit or StopLimit entry brackets. Defers if entry is in "New" status.
        /// </summary>
        public bool UpdateEntry(string groupId, decimal newLimitPrice, decimal? newEntryStopPrice = null)
        {
            if (!_groups.TryGetValue(groupId, out var group))
            {
                Log.Error($"BracketOrderManager.UpdateEntry: Group {groupId} not found.");
                return false;
            }

            // Only valid in EntryPending or EntryPartial
            if (group.State != BracketState.EntryPending && group.State != BracketState.EntryPartial)
            {
                Log.Error($"BracketOrderManager.UpdateEntry: Rejected for group {groupId} — " +
                    $"state is {group.State}, requires EntryPending or EntryPartial.");
                return false;
            }

            if (group.EntryTicket == null || group.EntryTicket.Status.IsClosed())
            {
                Log.Error($"BracketOrderManager.UpdateEntry: Entry ticket is null or closed for group {groupId}.");
                return false;
            }

            var fields = new UpdateOrderFields { LimitPrice = newLimitPrice };
            if (newEntryStopPrice.HasValue)
                fields.StopPrice = newEntryStopPrice.Value;

            // Defer if entry hasn't been ACK'd yet
            if (group.EntryTicket.Status == OrderStatus.New)
            {
                Log.Debug($"BracketOrderManager.UpdateEntry: Entry still New for group {groupId}. Deferring update.");
                group.PendingEntryUpdate = fields;
                if (newEntryStopPrice.HasValue) group.EntryStopPrice = newEntryStopPrice.Value;
                return true;
            }

            group.PendingEntryUpdate = null;
            var response = group.EntryTicket.Update(fields);
            if (response.IsSuccess)
            {
                if (newEntryStopPrice.HasValue) group.EntryStopPrice = newEntryStopPrice.Value;
            }
            else
            {
                Log.Error($"BracketOrderManager.UpdateEntry: Failed for group {groupId}. " +
                    $"Response: {response.ErrorCode} - {response.ErrorMessage}");
            }
            return response.IsSuccess;
        }

        /// <summary>
        /// FALLBACK EOD method — only for brackets stuck in non-updatable states.
        /// The PRIMARY EOD exit is UpdateTarget (drop target into the money).
        /// This is called AFTER the primary pass as a safety net.
        ///
        /// In live: calls Alpaca's DELETE /v2/positions?cancel_orders=true.
        /// In backtest: iterates all groups and cancels + market sells.
        /// </summary>
        /// <returns>True if the REST call succeeded (live) or all groups processed (backtest).</returns>
        public bool LiquidateAllForEod()
        {
            // Implementation depends on the brokerage — delegate to it
            if (Brokerage is AlpacaBrokerage liveBrokerage)
            {
                return liveBrokerage.LiquidateAllPositionsForEod();
            }

            // Backtest fallback: cancel all non-terminal brackets and market-sell
            foreach (var group in _groups.Values.Where(g => !g.IsTerminal))
            {
                lock (group.StateLock)
                {
                    if (group.State == BracketState.EntryPending || group.State == BracketState.EntryPartial ||
                        group.State == BracketState.Cancelling)
                    {
                        // Cancel unfilled entries
                        if (group.EntryTicket?.Status.IsOpen() == true)
                        {
                            group.EntryTicket.Cancel("EOD liquidation");
                        }
                    }

                    // If shares are held but not in Protected (can't use UpdateTarget), flatten position
                    if (group.FilledQuantity != 0 && group.State != BracketState.Protected &&
                        group.State != BracketState.Closed)
                    {
                        // Negate FilledQuantity to flatten: long (positive) → sell (negative), short (negative) → buy (positive)
                        var exitQty = -group.FilledQuantity;
                        var request = new SubmitOrderRequest(
                            OrderType.Market, group.Symbol.SecurityType,
                            group.Symbol, exitQty, 0, 0,
                            _algorithm.UtcTime, $"eod-liquidation:{group.GroupId}");
                        _algorithm.SubmitOrderRequest(request);
                        Log.Debug($"BracketOrderManager.LiquidateAllForEod: Market sell {exitQty} " +
                            $"for group {group.GroupId} in state {group.State}");
                    }
                }
            }
            return true;
        }

        #endregion

        #region Query Methods

        /// <summary>
        /// Gets a bracket group by its ID. Returns null if not found.
        /// </summary>
        public BracketGroup GetGroup(string groupId) =>
            _groups.TryGetValue(groupId, out var g) ? g : null;

        /// <summary>
        /// Gets the bracket group ID for a given LEAN order ID.
        /// Returns null if the order is not part of a bracket.
        /// (Never returns empty string — PythonNet marshals null to None.)
        /// </summary>
        public string GetGroupIdForOrder(int orderId) =>
            _orderToGroup.TryGetValue(orderId, out var groupId) ? groupId : null;

        /// <summary>
        /// Gets the active (non-terminal) bracket for a symbol, or null.
        /// Backed by ConcurrentDictionary for O(1) lookup.
        /// </summary>
        public BracketGroup GetActiveGroupBySymbol(Symbol symbol)
        {
            if (symbol == null) return null;
            if (!_symbolToGroup.TryGetValue(symbol, out var groupId)) return null;
            if (!_groups.TryGetValue(groupId, out var group)) return null;
            if (group.IsTerminal)
            {
                // Stale entry — clean up
                _symbolToGroup.TryRemove(symbol, out _);
                return null;
            }
            return group;
        }

        /// <summary>
        /// All non-terminal brackets. Returns a materialized snapshot (List), safe to
        /// iterate from any thread — the underlying ConcurrentDictionary is not exposed.
        /// </summary>
        public List<BracketGroup> ActiveBrackets =>
            _groups.Values.Where(g => !g.IsTerminal).ToList();

        /// <summary>
        /// Gets all bracket groups (active and complete). Materialized snapshot.
        /// </summary>
        public List<BracketGroup> GetAllGroups() =>
            _groups.Values.ToList();

        /// <summary>
        /// Gets a pending rescue for a group, if one exists.
        /// Used by ReconcileProtectiveInvariantAsync as a safety net for stale rescues.
        /// </summary>
        internal PendingRescue GetPendingRescue(string groupId)
        {
            _pendingRescues.TryGetValue(groupId, out var rescue);
            return rescue;
        }

        #endregion

        #region Leg Registration (Called by Brokerages)

        /// <summary>
        /// Called by the brokerage to register exit leg tickets with the bracket group.
        /// Links the leg's order ticket and registers the order ID for event routing.
        /// </summary>
        internal void RegisterLegTicket(string groupId, BracketLegType legType, OrderTicket ticket)
        {
            if (!_groups.TryGetValue(groupId, out var group))
            {
                Log.Error($"BracketOrderManager.RegisterLegTicket: Group {groupId} not found.");
                return;
            }

            if (ticket == null)
            {
                Log.Error($"BracketOrderManager.RegisterLegTicket: Null ticket for group {groupId} leg {legType}.");
                return;
            }

            switch (legType)
            {
                case BracketLegType.StopLoss:
                    group.StopTicket = ticket;
                    Log.Debug($"BracketOrderManager.RegisterLegTicket: Registered StopLoss leg " +
                        $"ticket {ticket.OrderId} for group {groupId}");
                    break;

                case BracketLegType.TakeProfit:
                    group.TargetTicket = ticket;
                    Log.Debug($"BracketOrderManager.RegisterLegTicket: Registered TakeProfit leg " +
                        $"ticket {ticket.OrderId} for group {groupId}");
                    break;
            }

            _orderToGroup[ticket.OrderId] = groupId;
        }

        /// <summary>
        /// Pre-registers an entry order ID in the event routing map BEFORE the order is
        /// submitted. Critical for backtest sync fills where ProcessOrderEvent fires
        /// during PlaceOrder before the normal registration completes.
        /// </summary>
        internal void RegisterEntryOrderId(string groupId, int orderId)
        {
            _orderToGroup[orderId] = groupId;
            Log.Trace($"BracketOrderManager.RegisterEntryOrderId: Pre-registered orderId={orderId} for group {groupId}");
        }

        /// <summary>
        /// Internal leg cancellation that bypasses public state guards. Used by the
        /// backtest brokerage for OCO cascade simulation (e.g., stop fills → cancel target).
        /// Does NOT transition the group state — just cancels the LEAN order.
        /// </summary>
        internal void CancelLegInternal(string groupId, BracketLegType legType)
        {
            if (!_groups.TryGetValue(groupId, out var group))
            {
                Log.Error($"BracketOrderManager.CancelLegInternal: Group {groupId} not found.");
                return;
            }

            OrderTicket ticket = legType == BracketLegType.StopLoss ? group.StopTicket : group.TargetTicket;
            if (ticket != null && ticket.Status.IsOpen())
            {
                Log.Debug($"BracketOrderManager.CancelLegInternal: Cancelling {legType} leg " +
                    $"ticket {ticket.OrderId} for group {groupId} (internal cascade)");
                ticket.Cancel($"OCO cascade: {groupId}");
            }
        }

        #endregion

        #region Order Event Processing (State Machine Driven)

        /// <summary>
        /// Processes order events for bracket-related orders. Drives the state machine
        /// transitions based on order status changes.
        ///
        /// MUST be called BEFORE OnOrderEvent fires to downstream consumers (strategy).
        /// This guarantees that when the strategy sees an order event, BracketGroup.State
        /// is already updated.
        /// </summary>
        /// <param name="e">The order event to process.</param>
        public void ProcessOrderEvent(OrderEvent e)
        {
            // Only process events for orders we're tracking
            if (!_orderToGroup.TryGetValue(e.OrderId, out var groupId))
            {
                return;
            }

            if (!_groups.TryGetValue(groupId, out var group))
            {
                Log.Error($"BracketOrderManager.ProcessOrderEvent: Group {groupId} found in _orderToGroup " +
                    $"but missing from _groups. OrderId={e.OrderId}, Status={e.Status}");
                return;
            }

            // Guard: ignore events for brackets that are already terminal.
            // Prevents reconciliation synthetic fills from creating phantom positions.
            if (group.IsTerminal)
            {
                Log.Debug($"BracketOrderManager.ProcessOrderEvent: Ignoring event for terminal group " +
                    $"{groupId} (state={group.State}). OrderId={e.OrderId}, Status={e.Status}");
                return;
            }

            Log.Trace($"BracketOrderManager.ProcessOrderEvent: Group {groupId}, " +
                $"OrderId={e.OrderId}, Status={e.Status}, FillQty={e.FillQuantity}, " +
                $"FillPrice={e.FillPrice}, State={group.State}");

            // --- Entry order events ---
            if (e.OrderId == group.EntryTicket?.OrderId)
            {
                ProcessEntryEvent(group, e);
                return;
            }

            // --- Clear pending update tracking ---
            if (group.PendingUpdateOrderIds.ContainsKey(e.OrderId) &&
                (e.Status == OrderStatus.UpdateSubmitted || e.Status.IsClosed()))
            {
                Log.Debug($"BracketOrderManager.ProcessOrderEvent: Clearing PendingUpdateOrderId={e.OrderId} " +
                    $"for group {groupId} (status={e.Status})");
                group.PendingUpdateOrderIds.TryRemove(e.OrderId, out _);
            }

            // --- Exit leg events ---
            ProcessExitEvent(group, e);

            // --- Check if this event completes a pending rescue ---
            CheckPendingRescue(groupId, group, e);
        }

        /// <summary>
        /// Processes entry order events and drives state machine transitions.
        /// </summary>
        private void ProcessEntryEvent(BracketGroup group, OrderEvent e)
        {
            lock (group.StateLock)
            {
                switch (e.Status)
                {
                    case OrderStatus.Filled:
                        ProcessEntryFilled(group, e);
                        break;

                    case OrderStatus.PartiallyFilled:
                        ProcessEntryPartialFill(group, e);
                        break;

                    case OrderStatus.Canceled:
                    case OrderStatus.Invalid:
                        ProcessEntryCanceled(group, e);
                        break;

                    default:
                        Log.Debug($"BracketOrderManager.ProcessEntryEvent: Entry status update " +
                            $"for group {group.GroupId}: {e.Status}");
                        break;
                }

                // --- Deferred cancel replay ---
                // When CancelBracket was called while entry was in "New" status, PendingCancel
                // was set. Now that the entry has left "New", we need to actually send the cancel.
                // We can't call CancelBracket (it acquires the lock) so we directly cancel the
                // entry ticket here and let ProcessOrderEvent handle the state transition when
                // the cancel confirmation arrives.
                if (group.PendingCancel && e.Status != OrderStatus.New)
                {
                    group.PendingCancel = false;
                    Log.Debug($"BracketOrderManager.ProcessEntryEvent: Executing deferred cancel " +
                        $"for group {group.GroupId} (entry now {e.Status})");
                    // Cancel directly — state is already Cancelling from the original CancelBracket call
                    if (!group.IsTerminal && group.EntryTicket?.Status.IsOpen() == true)
                    {
                        group.EntryTicket.Cancel($"Deferred bracket cancel: {group.GroupId}");
                    }
                }

                // --- Deferred entry update replay ---
                if (group.PendingEntryUpdate != null && e.Status != OrderStatus.New)
                {
                    var pendingFields = group.PendingEntryUpdate;
                    group.PendingEntryUpdate = null;
                    if (group.IsTerminal)
                    {
                        Log.Debug($"BracketOrderManager.ProcessEntryEvent: Discarding deferred entry update " +
                            $"for group {group.GroupId} — entry is terminal.");
                    }
                    else
                    {
                        Log.Debug($"BracketOrderManager.ProcessEntryEvent: Replaying deferred entry update " +
                            $"for group {group.GroupId}");
                        var updateResponse = group.EntryTicket?.Update(pendingFields);
                        if (updateResponse?.IsSuccess == false)
                        {
                            Log.Error($"BracketOrderManager.ProcessEntryEvent: Deferred entry update failed " +
                                $"for group {group.GroupId}. {updateResponse.ErrorCode} - {updateResponse.ErrorMessage}");
                        }
                    }
                }
            }

            // --- Check if this entry event completes a pending rescue ---
            CheckPendingRescue(group.GroupId, group, e);
        }

        /// <summary>
        /// Handles entry order filled event.
        /// </summary>
        private void ProcessEntryFilled(BracketGroup group, OrderEvent e)
        {
            // Must be called inside lock(group.StateLock)
            group.FilledQuantity += e.FillQuantity;
            group.FillPrice = e.FillPrice;

            switch (group.State)
            {
                case BracketState.EntryPending:
                case BracketState.EntryPartial:
                    group.TryTransition(group.State, BracketState.Protected,
                        $"entry filled at {e.FillPrice}, qty={group.FilledQuantity}");
                    // Cancel partial fill timer if it was running
                    group.PartialFillTimer?.Dispose();
                    group.PartialFillTimer = null;
                    break;

                case BracketState.Cancelling:
                    // Fill arrived while cancel was in flight — cancel lost the race.
                    // Keep the position and protect it.
                    group.TryTransition(BracketState.Cancelling, BracketState.Protected,
                        "full fill arrived during Cancelling — cancel lost race, protecting position");
                    group.CancellingTimeoutTimer?.Dispose();
                    group.CancellingTimeoutTimer = null;
                    group.PartialFillTimer?.Dispose();
                    group.PartialFillTimer = null;
                    break;

                default:
                    Log.Error($"BracketOrderManager.ProcessEntryFilled: Unexpected state {group.State} " +
                        $"for group {group.GroupId} on entry fill.");
                    break;
            }

            Log.Debug($"BracketOrderManager.ProcessEntryFilled: Entry FILLED for group {group.GroupId} " +
                $"at price {e.FillPrice}. FilledQty={group.FilledQuantity}/{group.Quantity}. State={group.State}");
        }

        /// <summary>
        /// Handles entry order partial fill event.
        /// </summary>
        private void ProcessEntryPartialFill(BracketGroup group, OrderEvent e)
        {
            // Must be called inside lock(group.StateLock)
            // Compute VWAP for partial fills
            var prevQty = Math.Abs(group.FilledQuantity);
            var newQty = Math.Abs(e.FillQuantity);
            var prevPrice = group.FillPrice ?? 0m;
            group.FilledQuantity += e.FillQuantity;
            var totalQty = Math.Abs(group.FilledQuantity);
            group.FillPrice = totalQty > 0
                ? (prevPrice * prevQty + e.FillPrice * newQty) / totalQty
                : e.FillPrice;

            switch (group.State)
            {
                case BracketState.EntryPending:
                    group.TryTransition(BracketState.EntryPending, BracketState.EntryPartial,
                        $"first partial fill at {e.FillPrice}, qty={e.FillQuantity}");
                    // Start partial fill timeout timer if configured
                    StartPartialFillTimeout(group);
                    break;

                case BracketState.EntryPartial:
                    // More partial fills — accumulate but don't reset timer
                    Log.Debug($"BracketOrderManager.ProcessEntryPartialFill: Additional partial fill " +
                        $"for group {group.GroupId}. Total={group.FilledQuantity}/{group.Quantity}");
                    break;

                case BracketState.Cancelling:
                    // Partial fill arrived while cancel was in flight — cancel lost the race.
                    // Transition back to EntryPartial and protect via OCO.
                    group.TryTransition(BracketState.Cancelling, BracketState.EntryPartial,
                        "partial fill arrived during Cancelling — cancel lost race");
                    group.CancellingTimeoutTimer?.Dispose();
                    group.CancellingTimeoutTimer = null;
                    break;

                default:
                    Log.Error($"BracketOrderManager.ProcessEntryPartialFill: Unexpected state {group.State} " +
                        $"for group {group.GroupId} on partial fill.");
                    break;
            }
        }

        /// <summary>
        /// Handles entry order canceled/invalid event.
        /// </summary>
        private void ProcessEntryCanceled(BracketGroup group, OrderEvent e)
        {
            // Must be called inside lock(group.StateLock)
            group.PendingCancel = false;

            switch (group.State)
            {
                case BracketState.EntryPending:
                    // No fills, clean cancel
                    group.TryTransition(BracketState.EntryPending, BracketState.Cancelled,
                        $"entry {e.Status}");
                    OnGroupTerminal(group);
                    break;

                case BracketState.EntryPartial:
                    // Partial fills exist — shouldn't get cancel without going through Cancelling first,
                    // but handle defensively. Transition to Rescuing (rescue executed outside lock below).
                    if (group.FilledQuantity != 0)
                    {
                        group.ForceState(BracketState.Rescuing,
                            $"entry cancel with fills (qty={group.FilledQuantity}) from {group.State}");
                    }
                    else
                    {
                        group.TryTransitionFrom(BracketState.Cancelled, $"entry {e.Status}, no fills");
                        OnGroupTerminal(group);
                    }
                    break;

                case BracketState.Cancelling:
                    group.CancellingTimeoutTimer?.Dispose();
                    group.CancellingTimeoutTimer = null;

                    if (group.FilledQuantity != 0)
                    {
                        // Has partial fills — need to rescue
                        group.TryTransition(BracketState.Cancelling, BracketState.Rescuing,
                            $"cancel confirmed with fills (qty={group.FilledQuantity}), initiating rescue");
                        // Execute rescue outside the lock (involves REST I/O for OCO)
                    }
                    else
                    {
                        // No fills — clean cancel
                        group.TryTransition(BracketState.Cancelling, BracketState.Cancelled,
                            $"cancel confirmed, no fills");
                        OnGroupTerminal(group);
                    }
                    break;

                default:
                    Log.Error($"BracketOrderManager.ProcessEntryCanceled: Unexpected state {group.State} " +
                        $"for group {group.GroupId} on entry cancel.");
                    break;
            }

            Log.Debug($"BracketOrderManager.ProcessEntryCanceled: Entry {e.Status} for group {group.GroupId}. " +
                $"FilledQty={group.FilledQuantity}. State={group.State}");

            // If transitioned to Rescuing, execute rescue outside the lock
            if (group.State == BracketState.Rescuing)
            {
                ExecuteRescue(group);
            }
        }

        /// <summary>
        /// Processes exit leg events and drives state machine transitions.
        /// </summary>
        private void ProcessExitEvent(BracketGroup group, OrderEvent e)
        {
            var legName = (e.OrderId == group.StopTicket?.OrderId) ? "STOP" : "TARGET";
            decimal rescueQty = 0;

            lock (group.StateLock)
            {
                if (e.Status == OrderStatus.Filled)
                {
                    group.ExitFilledQuantity += Math.Abs(e.FillQuantity);

                    if (group.State == BracketState.Closed)
                    {
                        // Double fill — both OCO legs filled (extreme volatility race)
                        Log.Error($"BracketOrderManager.ProcessExitEvent: OCO DOUBLE FILL for group {group.GroupId}! " +
                            $"{legName} filled at {e.FillPrice} but bracket already Closed.");
                        EmitPluginMessage("[PLUGIN:OCO_DOUBLE_FILL]", new
                        {
                            severity = "critical",
                            group_id = group.GroupId,
                            symbol = group.Symbol.Value,
                            msg = $"Both exit legs filled — {legName} at {e.FillPrice}",
                            stop_qty = group.StopTicket?.OrderId == e.OrderId ? e.FillQuantity : 0,
                            target_qty = group.TargetTicket?.OrderId == e.OrderId ? e.FillQuantity : 0,
                        });
                    }
                    else if (group.State == BracketState.Protected)
                    {
                        group.ExitOrderId = e.OrderId;
                        group.ExitPrice = e.FillPrice;
                        group.TryTransition(BracketState.Protected, BracketState.Closed,
                            $"{legName} filled at {e.FillPrice}");
                        OnGroupTerminal(group);
                    }
                    else
                    {
                        // Exit fill in unexpected state — still record it
                        group.ExitOrderId = e.OrderId;
                        group.ExitPrice = e.FillPrice;
                        group.ForceState(BracketState.Closed,
                            $"{legName} filled at {e.FillPrice} from unexpected state {group.State}");
                        OnGroupTerminal(group);
                    }

                    Log.Debug($"BracketOrderManager.ProcessExitEvent: {legName} leg FILLED for group {group.GroupId} " +
                        $"at price {e.FillPrice}. ExitFilledQty={group.ExitFilledQuantity}. State={group.State}");
                }
                else if (e.Status == OrderStatus.PartiallyFilled)
                {
                    group.ExitFilledQuantity += Math.Abs(e.FillQuantity);
                    Log.Debug($"BracketOrderManager.ProcessExitEvent: {legName} leg PARTIAL FILL for group {group.GroupId} " +
                        $"at price {e.FillPrice}. FillQty={e.FillQuantity}. ExitFilledQty={group.ExitFilledQuantity}");
                }
                else if (e.Status == OrderStatus.Canceled)
                {
                    Log.Debug($"BracketOrderManager.ProcessExitEvent: {legName} leg CANCELLED for group {group.GroupId}.");

                    var stopClosed = IsTicketClosedOrCurrentEvent(group.StopTicket, e);
                    var targetClosed = IsTicketClosedOrCurrentEvent(group.TargetTicket, e);
                    if (stopClosed && targetClosed && group.State != BracketState.Closed
                        && group.State != BracketState.Rescuing
                        && group.State != BracketState.Cancelling
                        && group.State != BracketState.Protected)
                    {
                        var remainingQty = Math.Abs(group.FilledQuantity) - group.ExitFilledQuantity;
                        if (remainingQty > 0)
                        {
                            // Exit legs cancelled but shares remain unprotected — rescue via standalone OCO.
                            // This catches the race where entry fills fully during CancelBracket() and
                            // Alpaca cascades the cancel to the stop+target legs.
                            Log.Error($"BracketOrderManager.ProcessExitEvent: EXIT CANCEL CASCADE RESCUE for group {group.GroupId}. " +
                                $"RemainingQty={remainingQty} (entryFilled={Math.Abs(group.FilledQuantity)}, " +
                                $"exitFilled={group.ExitFilledQuantity}). Transitioning to Rescuing.");
                            EmitPluginMessage("[PLUGIN:EXIT_CANCEL_RESCUE]", new
                            {
                                severity = "critical",
                                group_id = group.GroupId,
                                symbol = group.Symbol.Value,
                                msg = $"Both exit legs cancelled with {remainingQty} shares unprotected. Rescuing.",
                                remaining_qty = remainingQty,
                                entry_filled = Math.Abs(group.FilledQuantity),
                                exit_filled = group.ExitFilledQuantity,
                            });
                            group.ForceState(BracketState.Rescuing,
                                $"exit legs cancelled with {remainingQty} shares unprotected");
                            rescueQty = remainingQty;
                        }
                        else
                        {
                            // All shares already exited via partial/full fills before cancel — clean close
                            group.TryTransitionFrom(BracketState.Cancelled,
                                "both exit legs closed, all shares accounted for");
                            OnGroupTerminal(group);
                        }
                    }
                    else if (stopClosed && targetClosed
                        && group.State == BracketState.Protected
                        && group.ExitFilledQuantity == 0)
                    {
                        // FAST-PATH RESCUE: Both exit legs cancelled in Protected state with
                        // zero exit fills. This is the cancel-fill race: entry filled during
                        // Cancelling → Protected, but Alpaca cascade-cancelled exit legs.
                        // Since no exit leg has filled (ExitFilledQuantity == 0), this is NOT
                        // a normal OCO exit (which would have ExitFilledQuantity > 0).
                        var remainingQty = Math.Abs(group.FilledQuantity) - group.ExitFilledQuantity;
                        if (remainingQty > 0)
                        {
                            Log.Error($"BracketOrderManager.ProcessExitEvent: PROTECTED CANCEL CASCADE RESCUE " +
                                $"for group {group.GroupId}. Both exit legs cancelled with zero exit fills. " +
                                $"RemainingQty={remainingQty} (entryFilled={Math.Abs(group.FilledQuantity)}, " +
                                $"exitFilled={group.ExitFilledQuantity}).");
                            EmitPluginMessage("[PLUGIN:PROTECTED_CANCEL_RESCUE]", new
                            {
                                severity = "critical",
                                group_id = group.GroupId,
                                symbol = group.Symbol.Value,
                                msg = $"Both exit legs cancelled in Protected with zero exit fills. Rescuing {remainingQty} shares.",
                                remaining_qty = remainingQty,
                                entry_filled = Math.Abs(group.FilledQuantity),
                                exit_filled = group.ExitFilledQuantity,
                            });
                            group.TryTransition(BracketState.Protected, BracketState.Rescuing,
                                $"protected cancel cascade: {remainingQty} shares unprotected, zero exit fills");
                            rescueQty = remainingQty;
                        }
                    }
                }
                else
                {
                    Log.Debug($"BracketOrderManager.ProcessExitEvent: {legName} leg status={e.Status} " +
                        $"for group {group.GroupId}");
                }
            }

            // If transitioned to Rescuing, execute rescue outside the lock (involves REST I/O)
            if (rescueQty > 0 && group.State == BracketState.Rescuing)
            {
                ExecuteRescue(group, rescueQty);
            }
        }

        #endregion

        #region Internal — Rescue, OCO, Timers

        /// <summary>
        /// Executes a rescue: places a standalone OCO for unprotected shares.
        /// Called when a bracket transitions to Rescuing state, either from:
        /// - ProcessEntryCanceled: entry cancelled with partial fills (original path)
        /// - ProcessExitEvent: exit legs cancelled but shares remain (cancel cascade race fix)
        /// - ReconcileProtectiveInvariantAsync: invariant checker safety net
        /// </summary>
        /// <param name="group">The bracket group to rescue.</param>
        /// <param name="remainingQtyOverride">
        /// When provided, overrides the rescue quantity (for exit-leg-cancel cascade where
        /// some shares may have already exited via partial fills before the cancel).
        /// When null, uses Math.Abs(group.FilledQuantity) (original behavior for entry-cancel rescues).
        /// </param>
        internal void ExecuteRescue(BracketGroup group, decimal? remainingQtyOverride = null)
        {
            var rescueQty = remainingQtyOverride ?? Math.Abs(group.FilledQuantity);
            Log.Debug($"BracketOrderManager.ExecuteRescue: Initiating rescue for group {group.GroupId}. " +
                $"RescueQty={rescueQty}, FilledQty={group.FilledQuantity}, ExitFilledQty={group.ExitFilledQuantity}, " +
                $"Stop={group.StopLossPrice}, Target={group.TakeProfitPrice}");

            // Register the pending rescue — OCO is placed when all old orders are confirmed closed
            var rescue = new PendingRescue
            {
                Symbol = group.Symbol,
                FilledQuantity = rescueQty,
                StopLossPrice = group.StopLossPrice,
                TakeProfitPrice = group.TakeProfitPrice,
                StopLossLimitPrice = group.StopLossLimitPrice,
                FillPrice = group.FillPrice ?? 0m,
                OriginalGroup = group,
                RequestedAt = DateTime.UtcNow,
                ClientOrderId = ClientOrderIdGenerator.ForRescueOco(group.GroupId, BracketLegType.TakeProfit),
            };
            _pendingRescues[group.GroupId] = rescue;

            // Check if all old orders are already closed (they might be)
            CheckPendingRescue(group.GroupId, group);
        }

        /// <summary>
        /// Returns true if the ticket is closed, OR if the current event targets this ticket
        /// and has a closed status. Accounts for ProcessOrderEvent running before OnOrderEvent
        /// updates the ticket status.
        /// </summary>
        private static bool IsTicketClosedOrCurrentEvent(OrderTicket ticket, OrderEvent currentEvent)
        {
            if (ticket == null) return true;
            if (ticket.Status.IsClosed()) return true;
            if (currentEvent != null
                && currentEvent.OrderId == ticket.OrderId
                && currentEvent.Status.IsClosed())
                return true;
            return false;
        }

        /// <summary>
        /// Checks whether all orders in a bracket group are confirmed closed,
        /// completing a pending rescue. If so, places the OCO order.
        /// </summary>
        /// <param name="groupId">The bracket group ID.</param>
        /// <param name="group">The bracket group.</param>
        /// <param name="currentEvent">
        /// The order event currently being processed (optional). When provided, the event's
        /// order is treated as closed even if the ticket status hasn't been updated yet.
        /// This is necessary because ProcessOrderEvent runs before OnOrderEvent, so the
        /// ticket status is stale for the current event.
        /// </param>
        internal void CheckPendingRescue(string groupId, BracketGroup group, OrderEvent currentEvent = null)
        {
            if (!_pendingRescues.TryGetValue(groupId, out var rescue))
                return;

            var allClosed =
                IsTicketClosedOrCurrentEvent(group.EntryTicket, currentEvent) &&
                IsTicketClosedOrCurrentEvent(group.StopTicket, currentEvent) &&
                IsTicketClosedOrCurrentEvent(group.TargetTicket, currentEvent);

            if (!allClosed)
            {
                Log.Trace($"BracketOrderManager.CheckPendingRescue: Rescue {groupId} waiting. " +
                    $"Entry={group.EntryTicket?.Status.ToString() ?? "null"}, " +
                    $"Stop={group.StopTicket?.Status.ToString() ?? "null"}, " +
                    $"Target={group.TargetTicket?.Status.ToString() ?? "null"}" +
                    (currentEvent != null ? $", currentEvent=OrderId:{currentEvent.OrderId}/{currentEvent.Status}" : ""));
                return;
            }

            // Atomic remove — only one thread wins. Prevents duplicate OCO placement
            // when concurrent threads (WS event + timer callback) both reach this point.
            if (!_pendingRescues.TryRemove(groupId, out rescue))
                return;

            Log.Debug($"BracketOrderManager.CheckPendingRescue: All orders confirmed closed for rescue {groupId}. Placing OCO.");

            // Place OCO for the filled shares
            try
            {
                PlaceRescueOco(rescue);
            }
            catch (Exception ex)
            {
                Log.Error($"BracketOrderManager.CheckPendingRescue: OCO placement failed for group {groupId}: {ex.Message}");
                ScheduleRescueRetry(rescue);
            }
        }

        /// <summary>
        /// Places a rescue OCO order for a partially-filled bracket.
        /// On success, transitions the group to Protected.
        /// </summary>
        private void PlaceRescueOco(PendingRescue rescue)
        {
            var group = rescue.OriginalGroup;

            // Idempotency check: verify no order with this client ID already exists at Alpaca.
            // This prevents duplicate rescue OCOs when concurrent threads or retries race.
            if (CheckOrderExistsByClientId != null && rescue.ClientOrderId != null)
            {
                try
                {
                    if (CheckOrderExistsByClientId(rescue.ClientOrderId))
                    {
                        // The rescue OCO already exists at Alpaca — position is protected
                        // (or already exited if the OCO filled). We can't register the
                        // existing OCO's leg tickets because we don't have the Alpaca order
                        // details needed to create phantom LEAN orders.
                        //
                        // Transition to Cancelled (terminal) to free _symbolToGroup and
                        // allow new brackets. The existing OCO at Alpaca continues to
                        // protect the position independently — when its legs fill, the
                        // WS events will be handled by HandleTradeUpdate's unknown-order
                        // fallback, and LEAN's portfolio model will correctly reflect
                        // the position change.
                        Log.Error($"BracketOrderManager.PlaceRescueOco: Order with ClientId={rescue.ClientOrderId} " +
                            $"already exists at Alpaca. Skipping duplicate rescue for group {group.GroupId}. " +
                            $"Transitioning to Cancelled to free symbol.");

                        lock (group.StateLock)
                        {
                            if (group.State == BracketState.Rescuing)
                            {
                                group.TryTransition(BracketState.Rescuing, BracketState.Cancelled,
                                    $"rescue OCO already exists at Alpaca (clientId={rescue.ClientOrderId}), " +
                                    $"releasing bracket tracking — Alpaca OCO continues independently");
                            }
                        }

                        EmitPluginMessage("[PLUGIN:RESCUE_DUPLICATE_DETECTED]", new
                        {
                            severity = "warning",
                            group_id = group.GroupId,
                            symbol = group.Symbol.Value,
                            msg = $"Rescue OCO already exists at Alpaca (clientId={rescue.ClientOrderId}). " +
                                  $"Bracket tracking released. OCO continues independently at Alpaca.",
                        });
                        return;
                    }
                }
                catch (Exception ex)
                {
                    Log.Debug($"BracketOrderManager.PlaceRescueOco: Client ID pre-flight check failed: {ex.Message}. " +
                        $"Proceeding with placement for group {group.GroupId}.");
                }
            }

            // Use rescue's FilledQuantity (may be overridden for exit-leg-cancel cascade where
            // some shares already exited via partial fills before the cancel).
            // Sign follows the entry: long (positive FilledQuantity) → sell (negative), short → buy.
            var exitQtyAbs = rescue.FilledQuantity;
            var exitQty = group.FilledQuantity > 0 ? -exitQtyAbs : exitQtyAbs;
            var targetPrice = RoundPrice(rescue.TakeProfitPrice, "rescueTarget");
            var stopPrice = RoundPrice(rescue.StopLossPrice, "rescueStop");

            var props = new AlpacaOcoOrderProperties
            {
                OcoGroupId = group.GroupId,
                StopLossStopPrice = stopPrice,
                StopLossLimitPrice = rescue.StopLossLimitPrice.HasValue
                    ? RoundPrice(rescue.StopLossLimitPrice.Value, "rescueStopLimit")
                    : null,
                OriginatingManager = this,
                IsRescueOco = true,
                ClientOrderId = rescue.ClientOrderId,
            };

            var request = new SubmitOrderRequest(
                OrderType.Limit,
                group.Symbol.SecurityType,
                group.Symbol,
                exitQty,
                0m,
                targetPrice,
                _algorithm.UtcTime,
                $"rescue-oco:{group.GroupId}:target",
                props);

            var targetTicket = _algorithm.SubmitOrderRequest(request);

            if (targetTicket.Status == OrderStatus.Invalid)
            {
                Log.Error($"BracketOrderManager.PlaceRescueOco: OCO submission rejected for group {group.GroupId}.");
                ScheduleRescueRetry(rescue);
                return;
            }

            // Register the target ticket
            group.TargetTicket = targetTicket;
            _orderToGroup[targetTicket.OrderId] = group.GroupId;

            Log.Debug($"BracketOrderManager.PlaceRescueOco: Rescue OCO submitted for group {group.GroupId}. " +
                $"TargetTicket={targetTicket.OrderId}, Qty={exitQty}");

            // Transition to Protected and clean up retry timer
            lock (group.StateLock)
            {
                if (group.State == BracketState.Rescuing)
                {
                    group.TryTransition(BracketState.Rescuing, BracketState.Protected,
                        $"rescue OCO placed, target={targetTicket.OrderId}");
                }
            }
            group.RescueRetryTimer?.Dispose();
            group.RescueRetryTimer = null;

            // Emit rescue complete message for entry logger
            EmitPluginMessage("[PLUGIN:RESCUE_COMPLETE]", new
            {
                severity = "info",
                group_id = group.GroupId,
                symbol = group.Symbol.Value,
                msg = $"Rescue OCO placed for {rescue.FilledQuantity} shares",
            });
        }

        /// <summary>
        /// Schedules a retry for rescue OCO placement with exponential backoff.
        /// </summary>
        private void ScheduleRescueRetry(PendingRescue rescue)
        {
            var group = rescue.OriginalGroup;
            group.RescueRetryCount++;

            // Exponential backoff: 5s, 10s, 20s, 40s, 60s, then every 60s
            var delays = new[] { 5000, 10000, 20000, 40000, 60000 };
            var delayMs = group.RescueRetryCount <= delays.Length
                ? delays[group.RescueRetryCount - 1]
                : 60000;

            if (group.RescueRetryCount >= 5)
            {
                EmitPluginMessage("[PLUGIN:RESCUE_STUCK]", new
                {
                    severity = "critical",
                    group_id = group.GroupId,
                    symbol = group.Symbol.Value,
                    msg = $"OCO placement failing repeatedly after {group.RescueRetryCount} retries. Manual intervention may be needed.",
                    retry = group.RescueRetryCount,
                    error = "OCO placement failed"
                });
            }
            else
            {
                EmitPluginMessage("[PLUGIN:RESCUE_RETRY]", new
                {
                    severity = "warning",
                    group_id = group.GroupId,
                    symbol = group.Symbol.Value,
                    msg = $"OCO placement retry #{group.RescueRetryCount} in {delayMs}ms",
                    retry = group.RescueRetryCount,
                    next_attempt_in = $"{delayMs}ms"
                });
            }

            // Re-add to pending rescues for retry
            _pendingRescues[group.GroupId] = rescue;

            group.RescueRetryTimer?.Dispose();
            group.RescueRetryTimer = new System.Threading.Timer(_ =>
            {
                // Check if group is still in Rescuing state before retrying
                if (group.State != BracketState.Rescuing)
                {
                    Log.Debug($"BracketOrderManager.RescueRetry: Group {group.GroupId} no longer in Rescuing " +
                        $"state ({group.State}). Aborting retry.");
                    return;
                }
                try
                {
                    PlaceRescueOco(rescue);
                }
                catch (Exception ex)
                {
                    Log.Error($"BracketOrderManager.RescueRetry: Retry #{group.RescueRetryCount} failed for group {group.GroupId}: {ex.Message}");
                    ScheduleRescueRetry(rescue);
                }
            }, null, delayMs, System.Threading.Timeout.Infinite);
        }

        /// <summary>
        /// Starts the partial fill timeout timer when the first partial fill arrives.
        /// </summary>
        private void StartPartialFillTimeout(BracketGroup group)
        {
            if (!group.PartialFillTimeout.HasValue || group.PartialFillTimer != null)
                return;

            group.PartialFillTimerStart = DateTime.UtcNow;
            var timeoutMs = (int)group.PartialFillTimeout.Value.TotalMilliseconds;

            group.PartialFillTimer = new System.Threading.Timer(_ =>
            {
                OnPartialFillTimeout(group);
            }, null, timeoutMs, System.Threading.Timeout.Infinite);

            Log.Debug($"BracketOrderManager.StartPartialFillTimeout: Timer started for group {group.GroupId}, " +
                $"timeout={group.PartialFillTimeout.Value.TotalSeconds}s");
        }

        /// <summary>
        /// Callback when partial fill timeout fires. Cancels remaining entry qty
        /// and initiates rescue for filled shares.
        /// </summary>
        private void OnPartialFillTimeout(BracketGroup group)
        {
            lock (group.StateLock)
            {
                if (group.State != BracketState.EntryPartial)
                {
                    Log.Debug($"BracketOrderManager.OnPartialFillTimeout: Group {group.GroupId} " +
                        $"no longer in EntryPartial (state={group.State}), aborting timeout.");
                    return;
                }

                Log.Debug($"BracketOrderManager.OnPartialFillTimeout: Timeout fired for group {group.GroupId}. " +
                    $"FilledQty={group.FilledQuantity}/{group.Quantity}. Initiating auto-rescue.");

                EmitPluginMessage("[PLUGIN:PARTIAL_FILL_TIMEOUT]", new
                {
                    severity = "info",
                    group_id = group.GroupId,
                    symbol = group.Symbol.Value,
                    msg = $"Partial fill timeout after {group.PartialFillTimeout?.TotalSeconds}s. " +
                          $"Filled {group.FilledQuantity}/{group.Quantity}. Initiating auto-rescue.",
                    filled_qty = group.FilledQuantity,
                    requested_qty = group.Quantity,
                });
            }

            // Cancel remaining entry via CancelBracket (which transitions EntryPartial → Cancelling)
            var success = CancelBracket(group.GroupId);
            if (!success)
            {
                // Cancel rejected — retry
                group.PartialFillRetryCount++;
                if (group.PartialFillRetryCount <= 3)
                {
                    Log.Debug($"BracketOrderManager.OnPartialFillTimeout: Cancel retry #{group.PartialFillRetryCount} " +
                        $"for group {group.GroupId}");
                    group.PartialFillTimer = new System.Threading.Timer(_ =>
                    {
                        OnPartialFillTimeout(group);
                    }, null, 5000, System.Threading.Timeout.Infinite);
                }
                else
                {
                    Log.Error($"BracketOrderManager.OnPartialFillTimeout: Max retries exceeded for group {group.GroupId}. " +
                        $"Layer 1 reconciliation will backstop.");
                }
            }
        }

        /// <summary>
        /// Starts the 30-second timeout for the Cancelling state. If cancel ack never arrives,
        /// queries Alpaca REST directly to determine actual state.
        /// </summary>
        private void StartCancellingTimeout(BracketGroup group)
        {
            group.CancellingTimeoutTimer?.Dispose();
            group.CancellingTimeoutTimer = new System.Threading.Timer(_ =>
            {
                lock (group.StateLock)
                {
                    if (group.State != BracketState.Cancelling) return;

                    // Only force-terminate if the entry was never accepted by Alpaca (no BrokerId).
                    // If the entry HAS a BrokerId, Alpaca accepted it and the cancel ack may be
                    // legitimately delayed — Layer 1 reconciliation will detect actual state.
                    // Force-terminating a bracket with a BrokerId risks position leaks.
                    var entryOrder = group.EntryTicket?.OrderId > 0
                        ? _algorithm.Transactions.GetOrderById(group.EntryTicket.OrderId)
                        : null;
                    var hasBrokerId = entryOrder?.BrokerId.Count > 0;

                    if (hasBrokerId)
                    {
                        Log.Error($"BracketOrderManager.CancellingTimeout: 30s timeout for group {group.GroupId}. " +
                            $"Entry has BrokerId — deferring to Layer 1 reconciliation.");
                        return;
                    }

                    // Entry was never accepted (rejected or never submitted) — nothing to cancel
                    if (group.FilledQuantity != 0)
                    {
                        Log.Error($"BracketOrderManager.CancellingTimeout: 30s timeout for group {group.GroupId} " +
                            $"with fills (qty={group.FilledQuantity}) but no BrokerId. Forcing rescue.");
                        group.ForceState(BracketState.Rescuing,
                            "cancelling timeout, no BrokerId, has fills — forcing rescue");
                    }
                    else
                    {
                        Log.Error($"BracketOrderManager.CancellingTimeout: 30s timeout for group {group.GroupId}. " +
                            $"No BrokerId, no fills. Forcing Cancelled.");
                        group.ForceState(BracketState.Cancelled,
                            "cancelling timeout, no BrokerId, no fills — forcing terminal");
                        OnGroupTerminal(group);
                    }
                }

                if (group.State == BracketState.Rescuing)
                {
                    ExecuteRescue(group);
                }
            }, null, 30000, System.Threading.Timeout.Infinite);
        }

        /// <summary>
        /// Called when a group reaches a terminal state. Cleans up tracking.
        /// Only removes the symbol mapping if this group still owns it — during
        /// replace(), a new group may have already overwritten the mapping.
        /// </summary>
        private void OnGroupTerminal(BracketGroup group)
        {
            group.DisposeTimers();
            // Only remove if this group still owns the symbol mapping.
            // During replace(), PlaceBracketOrder overwrites _symbolToGroup with the new group.
            // The old group's terminal event should NOT remove the new group's mapping.
            _symbolToGroup.TryRemove(new KeyValuePair<Symbol, string>(group.Symbol, group.GroupId));
        }

        #endregion

        #region Validation

        /// <summary>
        /// Validates bracket order parameters before submission.
        /// </summary>
        private void ValidateBracketParameters(
            Symbol symbol,
            decimal quantity,
            decimal stopLossPrice,
            decimal takeProfitPrice,
            OrderType entryType,
            decimal? entryLimitPrice,
            decimal? entryStopPrice = null)
        {
            if (symbol == null)
                throw new ArgumentException("BracketOrderManager: Symbol cannot be null.");

            if (quantity == 0)
                throw new ArgumentException("BracketOrderManager: Quantity cannot be zero.");

            if (entryType == OrderType.Limit && !entryLimitPrice.HasValue)
                throw new ArgumentException("BracketOrderManager: entryLimitPrice is required for limit entry orders.");

            if (entryType == OrderType.StopLimit)
            {
                if (!entryStopPrice.HasValue)
                    throw new ArgumentException("BracketOrderManager: entryStopPrice is required for stop-limit entry orders.");
                if (!entryLimitPrice.HasValue)
                    throw new ArgumentException("BracketOrderManager: entryLimitPrice is required for stop-limit entry orders.");
            }

            // Long bracket: target must be above stop
            if (quantity > 0 && takeProfitPrice <= stopLossPrice)
                throw new ArgumentException(
                    $"BracketOrderManager: Long bracket: takeProfitPrice ({takeProfitPrice}) " +
                    $"must be greater than stopLossPrice ({stopLossPrice}).");

            // Short bracket: target must be below stop
            if (quantity < 0 && takeProfitPrice >= stopLossPrice)
                throw new ArgumentException(
                    $"BracketOrderManager: Short bracket: takeProfitPrice ({takeProfitPrice}) " +
                    $"must be less than stopLossPrice ({stopLossPrice}).");

            // Alpaca minimum $0.01 separation
            if (Math.Abs(takeProfitPrice - stopLossPrice) < 0.01m)
                throw new ArgumentException(
                    "BracketOrderManager: Take-profit and stop-loss must be at least $0.01 apart.");
        }

        /// <summary>
        /// Rounds a price to Alpaca's maximum allowed precision.
        /// </summary>
        internal static decimal RoundPrice(decimal price, string name)
        {
            var maxDecimals = price >= 1.0m ? 2 : 4;
            var rounded = Math.Round(price, maxDecimals);
            if (rounded != price)
            {
                Log.Debug($"BracketOrderManager: {name} rounded from {price} to {rounded} " +
                    $"(Alpaca max {maxDecimals} decimal places).");
            }
            return rounded;
        }

        #endregion

        #region Plugin Message Emission

        /// <summary>
        /// Emits a structured plugin message via the brokerage's OnMessage.
        /// Standard format: [PLUGIN:TAG] {json}
        /// </summary>
        internal void EmitPluginMessage(string tag, object payload)
        {
            try
            {
                // Add correlation_id to the payload
                var dict = payload as IDictionary<string, object>;
                if (dict == null)
                {
                    // Convert anonymous object to dictionary
                    var json = JsonConvert.SerializeObject(payload);
                    dict = JsonConvert.DeserializeObject<Dictionary<string, object>>(json);
                }
                if (!dict.ContainsKey("correlation_id"))
                {
                    dict["correlation_id"] = Guid.NewGuid().ToString();
                }

                var messageJson = JsonConvert.SerializeObject(dict);
                var fullMessage = $"{tag} {messageJson}";

                Log.Trace($"BracketOrderManager.EmitPluginMessage: {fullMessage}");

                // OnMessage is protected on Brokerage base class — use the forwarding method
                if (Brokerage is AlpacaBrokerage liveBrokerage)
                {
                    liveBrokerage.EmitBrokerageMessage(new BrokerageMessageEvent(BrokerageMessageType.Information, -1, fullMessage));
                }
                else if (Brokerage is AlpacaBacktestingBrokerage backtestBrokerage)
                {
                    backtestBrokerage.EmitBrokerageMessage(new BrokerageMessageEvent(BrokerageMessageType.Information, -1, fullMessage));
                }
                else
                {
                    // Brokerage not yet registered — log directly as fallback
                    Log.Error($"BracketOrderManager.EmitPluginMessage: Brokerage not registered, message not emitted: {fullMessage}");
                }
            }
            catch (Exception ex)
            {
                Log.Error($"BracketOrderManager.EmitPluginMessage: Failed to emit {tag}: {ex.Message}");
            }
        }

        #endregion

        #region Internal Classes

        /// <summary>
        /// State for a pending partial-fill rescue.
        /// </summary>
        internal class PendingRescue
        {
            public Symbol Symbol { get; set; }
            public decimal FilledQuantity { get; set; }
            public decimal StopLossPrice { get; set; }
            public decimal TakeProfitPrice { get; set; }
            public decimal? StopLossLimitPrice { get; set; }
            public decimal FillPrice { get; set; }
            public BracketGroup OriginalGroup { get; set; }
            public DateTime RequestedAt { get; set; }

            /// <summary>
            /// Pre-generated client order ID for rescue OCO idempotency.
            /// Generated once when the rescue is first created, reused across retries
            /// to prevent duplicate OCO placement at Alpaca.
            /// </summary>
            public string ClientOrderId { get; set; }
        }

        #endregion
    }
}
