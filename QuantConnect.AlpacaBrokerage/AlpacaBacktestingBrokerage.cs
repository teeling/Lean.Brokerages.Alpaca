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
using QuantConnect.Brokerages.Backtesting;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Extends LEAN's <see cref="BacktestingBrokerage"/> with bracket order awareness.
    /// This is the backtesting counterpart to <see cref="AlpacaBrokerage"/>'s bracket
    /// handling in live trading. Both brokerages handle the same
    /// <see cref="AlpacaBracketOrderProperties"/> data contract.
    ///
    /// Responsibilities:
    /// 1. Detect bracket entry orders in PlaceOrder and register the bracket group.
    /// 2. Create exit legs when the entry fills, via OnNewBrokerageOrderNotification.
    /// 3. Handle OCO cancellation when an exit leg fills — cancel the sibling.
    /// 4. Handle cancel cascades — if entry is cancelled, cancel legs; if a leg is
    ///    cancelled, cancel the sibling.
    /// 5. Register legs with BracketOrderManager so the manager can track them.
    ///
    /// Architecture note: The hook into fill events uses a deferred event queue.
    /// During base.Scan(), fill events are emitted via OnOrderEvent while the internal
    /// _needsScanLock is held. We queue bracket-related events and process them after
    /// base.Scan() returns and the lock is released. This avoids deadlocks from
    /// calling PlaceOrder/CancelOrder (which also acquire the lock) during event handling.
    ///
    /// Known limitations:
    /// - Partial fills: Alpaca adjusts the sibling leg quantity on partial fills (e.g.,
    ///   target partial fill → reduce stop quantity). This is not modeled in backtesting;
    ///   exit legs use the full entry quantity. In live trading, Alpaca handles this natively.
    /// - UpdateOrder: Stop/target price updates go through base.UpdateOrder which handles
    ///   standard stop/limit updates. No bracket-specific update override is needed since
    ///   the exit legs are standard StopMarket/LimitOrder objects.
    /// </summary>
    public class AlpacaBacktestingBrokerage : BacktestingBrokerage
    {
        /// <summary>
        /// Tracks bracket groups by group ID. Each group knows the entry, stop, and target order IDs.
        /// </summary>
        private readonly ConcurrentDictionary<string, BacktestBracketState> _bracketGroups = new();

        /// <summary>
        /// Maps LEAN order ID → bracket group ID for fast lookup during event processing.
        /// Note: BracketOrderManager also maintains its own _orderToGroup for event routing.
        /// Both are populated through their respective PlaceOrder/RegisterLegTicket paths
        /// and should stay in sync. The duplication is intentional: the brokerage needs
        /// this mapping for OCO cascade logic (which runs before/independently of the manager),
        /// while the manager needs it for state tracking in ProcessOrderEvent.
        /// </summary>
        private readonly ConcurrentDictionary<int, string> _orderToGroup = new();

        /// <summary>
        /// Bracket-related order events that need to be processed after Scan() releases locks.
        /// Events are queued during OnOrderEvent (called within locked Scan) and processed
        /// in our Scan() override after base.Scan() returns.
        /// Uses ConcurrentQueue for thread safety, consistent with other concurrent collections here.
        /// </summary>
        private readonly ConcurrentQueue<OrderEvent> _deferredBracketEvents = new();

        /// <summary>
        /// Flag indicating we're currently inside base.Scan() processing.
        /// When true, bracket events are deferred. When false, they're processed immediately.
        /// </summary>
        private bool _inBaseScan;

        /// <summary>
        /// Flag to prevent re-entrant bracket event processing.
        /// Set to true while ProcessBracketEvent is running to prevent
        /// cascading cancel events from being processed recursively.
        /// </summary>
        private bool _processingBracketEvent;

        /// <summary>
        /// Reference to the BracketOrderManager for leg registration.
        /// Set by the manager during initialization via RegisterManager().
        /// </summary>
        private BracketOrderManager _manager;

        /// <summary>
        /// Creates a new bracket-aware backtesting brokerage.
        /// </summary>
        /// <param name="algorithm">The algorithm under test.</param>
        public AlpacaBacktestingBrokerage(IAlgorithm algorithm)
            : base(algorithm, "Alpaca Backtesting Brokerage")
        {
            Log.Debug("AlpacaBacktestingBrokerage: Initialized bracket-aware backtesting brokerage.");
        }

        /// <summary>
        /// Called by <see cref="BracketOrderManager"/> to register itself with this
        /// brokerage, enabling leg ticket registration when exit legs are created.
        /// </summary>
        /// <param name="manager">The BracketOrderManager instance.</param>
        public void RegisterManager(BracketOrderManager manager)
        {
            _manager = manager;
            Log.Debug("AlpacaBacktestingBrokerage.RegisterManager: BracketOrderManager registered.");
        }

        #region Order Lifecycle Overrides

        /// <summary>
        /// Intercepts bracket entry orders and registers the bracket group state.
        /// Non-bracket orders pass through to the base implementation unchanged.
        ///
        /// This does NOT create exit legs here — that happens when the entry fills
        /// (via ProcessBracketEvent → CreateExitLegs).
        /// </summary>
        public override bool PlaceOrder(Order order)
        {
            // Check if this is a bracket entry order
            if (order.Properties is AlpacaBracketOrderProperties props && props.IsBracketOrder)
            {
                // Auto-register with the BracketOrderManager on first bracket order.
                // This bridges the gap where IAlgorithm doesn't expose BrokerageInstance,
                // so the manager can't discover us during construction.
                if (_manager == null && props.OriginatingManager != null)
                {
                    RegisterManager(props.OriginatingManager);
                }

                Log.Debug($"AlpacaBacktestingBrokerage.PlaceOrder: Detected bracket entry order. " +
                    $"OrderId={order.Id}, GroupId={props.BracketGroupId}, Symbol={order.Symbol}, " +
                    $"Qty={order.Quantity}, Stop={props.StopLossStopPrice}, Target={props.TakeProfitLimitPrice}, " +
                    $"StopLimit={props.StopLossLimitPrice}");

                // Register the bracket state for tracking
                var state = new BacktestBracketState
                {
                    GroupId = props.BracketGroupId,
                    EntryOrderId = order.Id,
                    StopLossPrice = props.StopLossStopPrice.Value,
                    TakeProfitPrice = props.TakeProfitLimitPrice.Value,
                    StopLossLimitPrice = props.StopLossLimitPrice,
                    Quantity = order.Quantity,
                    Symbol = order.Symbol,
                };

                _bracketGroups[props.BracketGroupId] = state;
                _orderToGroup[order.Id] = props.BracketGroupId;

                Log.Debug($"AlpacaBacktestingBrokerage.PlaceOrder: Registered bracket state for group {props.BracketGroupId}.");
            }
            // Check if this is a bracket leg order (created by us via OnNewBrokerageOrderNotification)
            else if (order.Properties is AlpacaBracketOrderProperties legProps
                && !string.IsNullOrEmpty(legProps.BracketGroupId)
                && legProps.LegType.HasValue)
            {
                var groupId = legProps.BracketGroupId;
                Log.Debug($"AlpacaBacktestingBrokerage.PlaceOrder: Bracket leg order placed. " +
                    $"OrderId={order.Id}, GroupId={groupId}, LegType={legProps.LegType}, " +
                    $"Symbol={order.Symbol}, Qty={order.Quantity}");

                // Register this leg order ID with the bracket state
                if (_bracketGroups.TryGetValue(groupId, out var state))
                {
                    switch (legProps.LegType.Value)
                    {
                        case BracketLegType.StopLoss:
                            state.StopOrderId = order.Id;
                            Log.Debug($"AlpacaBacktestingBrokerage.PlaceOrder: Registered stop leg " +
                                $"OrderId={order.Id} for group {groupId}");
                            break;
                        case BracketLegType.TakeProfit:
                            state.TargetOrderId = order.Id;
                            Log.Debug($"AlpacaBacktestingBrokerage.PlaceOrder: Registered target leg " +
                                $"OrderId={order.Id} for group {groupId}");
                            break;
                    }
                    _orderToGroup[order.Id] = groupId;

                    // Register the leg ticket with the BracketOrderManager
                    // Note: The ticket may not be available until after base.PlaceOrder creates it.
                    // We'll register it after base.PlaceOrder via a deferred lookup.
                }
                else
                {
                    Log.Error($"AlpacaBacktestingBrokerage.PlaceOrder: Bracket group {groupId} not found " +
                        $"for leg order {order.Id}. This should not happen.");
                }
            }

            // Always place through base — bracket entry orders are standard Market/Limit orders
            var result = base.PlaceOrder(order);

            // After base.PlaceOrder, the order ticket should be available — register with manager
            if (order.Properties is AlpacaBracketOrderProperties legProps2
                && !string.IsNullOrEmpty(legProps2.BracketGroupId)
                && legProps2.LegType.HasValue
                && _manager != null)
            {
                // The ticket is created by the transaction handler when it processes the order.
                // We get it from the transaction manager.
                var ticket = Algorithm.Transactions.GetOrderTicket(order.Id);
                if (ticket != null)
                {
                    _manager.RegisterLegTicket(legProps2.BracketGroupId, legProps2.LegType.Value, ticket);
                    Log.Debug($"AlpacaBacktestingBrokerage.PlaceOrder: Registered leg ticket {order.Id} " +
                        $"({legProps2.LegType}) with BracketOrderManager for group {legProps2.BracketGroupId}");
                }
                else
                {
                    Log.Error($"AlpacaBacktestingBrokerage.PlaceOrder: Could not get ticket for leg order {order.Id}. " +
                        $"Will try to register later.");
                }
            }

            return result;
        }

        /// <summary>
        /// Intercepts cancel requests for bracket orders.
        /// Cancelling one leg automatically cancels the sibling (OCO behavior).
        /// Cancelling the entry cancels any existing legs.
        ///
        /// Note on OCO flow: When a leg is cancelled here, the sibling is cancelled
        /// explicitly via CancelLegIfOpen. The resulting cancel OrderEvent for the
        /// sibling will hit OnOrderEvent → ProcessBracketEvent, but ProcessBracketEvent
        /// only acts on fills and entry cancels — it does NOT re-cascade leg cancels.
        /// This is intentional: CancelOrder handles the OCO cascade directly, so
        /// ProcessBracketEvent doesn't need to duplicate that logic.
        /// </summary>
        public override bool CancelOrder(Order order)
        {
            Log.Debug($"AlpacaBacktestingBrokerage.CancelOrder: OrderId={order.Id}, Symbol={order.Symbol}");

            var result = base.CancelOrder(order);

            // If this is a bracket-related order, handle cascade cancellation
            if (_orderToGroup.TryGetValue(order.Id, out var groupId)
                && _bracketGroups.TryGetValue(groupId, out var state))
            {
                // Stop leg cancelled → cancel target (OCO)
                if (order.Id == state.StopOrderId && state.TargetOrderId != 0)
                {
                    Log.Debug($"AlpacaBacktestingBrokerage.CancelOrder: Stop leg cancelled for group {groupId}. " +
                        $"Cancelling sibling target leg (OCO). TargetOrderId={state.TargetOrderId}");
                    CancelLegIfOpen(state.TargetOrderId);
                }
                // Target leg cancelled → cancel stop (OCO)
                else if (order.Id == state.TargetOrderId && state.StopOrderId != 0)
                {
                    Log.Debug($"AlpacaBacktestingBrokerage.CancelOrder: Target leg cancelled for group {groupId}. " +
                        $"Cancelling sibling stop leg (OCO). StopOrderId={state.StopOrderId}");
                    CancelLegIfOpen(state.StopOrderId);
                }
                // Entry cancelled → cancel any existing legs
                else if (order.Id == state.EntryOrderId)
                {
                    Log.Debug($"AlpacaBacktestingBrokerage.CancelOrder: Entry cancelled for group {groupId}. " +
                        $"Cancelling any existing exit legs.");
                    CancelLegsIfExist(state);
                }
            }

            return result;
        }

        /// <summary>
        /// Override Scan to process bracket events after the base scan completes.
        ///
        /// Flow:
        /// 1. Set _inBaseScan flag so OnOrderEvent defers bracket events
        /// 2. Call base.Scan() which evaluates fills and fires events
        /// 3. Clear _inBaseScan flag
        /// 4. Process deferred bracket events (entry fills → create legs, exit fills → OCO cancel)
        /// </summary>
        public override void Scan()
        {
            // Step 1: Mark that we're inside base.Scan() so bracket events get deferred
            _inBaseScan = true;

            // Step 2: Run the normal scan — this evaluates fills and fires OnOrderEvent
            base.Scan();

            // Step 3: We're out of the locked scan — safe to call PlaceOrder/CancelOrder
            _inBaseScan = false;

            // Step 4: Process any bracket events that were deferred during the scan
            ProcessDeferredBracketEvents();
        }

        /// <summary>
        /// Intercepts order events to detect bracket-related fills and cancellations.
        /// When called from within Scan() (lock held), events are deferred.
        /// When called from outside Scan(), events are processed immediately.
        ///
        /// Also forwards bracket-related events to the BracketOrderManager for
        /// state tracking (the manager does pure state tracking only).
        /// </summary>
        protected override void OnOrderEvent(OrderEvent e)
        {
            // Always let the event propagate normally first
            base.OnOrderEvent(e);

            // Only process events for orders we're tracking in bracket groups
            if (!_orderToGroup.ContainsKey(e.OrderId))
            {
                return;
            }

            Log.Debug($"AlpacaBacktestingBrokerage.OnOrderEvent: Bracket-related event. " +
                $"OrderId={e.OrderId}, Status={e.Status}, InBaseScan={_inBaseScan}");

            // Forward to BracketOrderManager for state tracking
            _manager?.ProcessOrderEvent(e);

            if (_inBaseScan)
            {
                // Defer processing until after Scan() releases locks
                _deferredBracketEvents.Enqueue(e);
                Log.Debug($"AlpacaBacktestingBrokerage.OnOrderEvent: Deferred bracket event for " +
                    $"OrderId={e.OrderId} Status={e.Status}");
            }
            else if (!_processingBracketEvent)
            {
                // Process immediately (e.g., from CancelOrder outside of Scan)
                ProcessBracketEvent(e);
            }
        }

        #endregion

        #region Bracket Event Processing

        /// <summary>
        /// Processes all bracket events that were deferred during base.Scan().
        /// Called after Scan() releases locks, so it's safe to call
        /// PlaceOrder (via OnNewBrokerageOrderNotification) and CancelOrder here.
        /// </summary>
        private void ProcessDeferredBracketEvents()
        {
            while (_deferredBracketEvents.TryDequeue(out var e))
            {
                Log.Debug($"AlpacaBacktestingBrokerage.ProcessDeferredBracketEvents: Processing deferred event " +
                    $"OrderId={e.OrderId}, Status={e.Status}");
                ProcessBracketEvent(e);
            }
        }

        /// <summary>
        /// Core bracket event processing logic. Handles:
        /// - Entry fill → create exit legs
        /// - Stop fill → cancel target (OCO)
        /// - Target fill → cancel stop (OCO)
        /// - Entry cancel → cancel legs
        /// </summary>
        private void ProcessBracketEvent(OrderEvent e)
        {
            if (!_orderToGroup.TryGetValue(e.OrderId, out var groupId))
            {
                return;
            }

            if (!_bracketGroups.TryGetValue(groupId, out var state))
            {
                return;
            }

            // Set re-entrancy guard to prevent recursive processing
            // (e.g., CancelOrder fires OnOrderEvent which calls ProcessBracketEvent)
            _processingBracketEvent = true;

            try
            {
                // --- Entry filled → create exit legs ---
                if (e.OrderId == state.EntryOrderId && e.Status == OrderStatus.Filled)
                {
                    Log.Debug($"AlpacaBacktestingBrokerage.ProcessBracketEvent: Entry FILLED for group {groupId}. " +
                        $"Creating exit legs. FillPrice={e.FillPrice}");
                    CreateExitLegs(state, e);
                }
                // --- Stop filled → cancel target (OCO) ---
                else if (e.OrderId == state.StopOrderId && e.Status == OrderStatus.Filled)
                {
                    Log.Debug($"AlpacaBacktestingBrokerage.ProcessBracketEvent: Stop leg FILLED for group {groupId}. " +
                        $"Cancelling target leg (OCO). TargetOrderId={state.TargetOrderId}");
                    CancelLegIfOpen(state.TargetOrderId);
                }
                // --- Target filled → cancel stop (OCO) ---
                else if (e.OrderId == state.TargetOrderId && e.Status == OrderStatus.Filled)
                {
                    Log.Debug($"AlpacaBacktestingBrokerage.ProcessBracketEvent: Target leg FILLED for group {groupId}. " +
                        $"Cancelling stop leg (OCO). StopOrderId={state.StopOrderId}");
                    CancelLegIfOpen(state.StopOrderId);
                }
                // --- Entry cancelled → cancel legs ---
                else if (e.OrderId == state.EntryOrderId && e.Status == OrderStatus.Canceled)
                {
                    Log.Debug($"AlpacaBacktestingBrokerage.ProcessBracketEvent: Entry CANCELLED for group {groupId}. " +
                        $"Cancelling any exit legs.");
                    CancelLegsIfExist(state);
                }
                else
                {
                    Log.Debug($"AlpacaBacktestingBrokerage.ProcessBracketEvent: No action for " +
                        $"OrderId={e.OrderId}, Status={e.Status} in group {groupId}");
                }
            }
            finally
            {
                _processingBracketEvent = false;
            }
        }

        /// <summary>
        /// Creates exit legs (stop-loss and take-profit) for a bracket group
        /// after the entry order fills. Uses OnNewBrokerageOrderNotification
        /// to create phantom LEAN orders that the engine will track.
        ///
        /// The exit quantity is the negative of the entry quantity
        /// (e.g., bought 100 → sell 100 for both stop and target).
        /// </summary>
        private void CreateExitLegs(BacktestBracketState state, OrderEvent entryFill)
        {
            // Exit legs close the position, so quantity is negated
            var exitQty = -state.Quantity;
            var now = Algorithm.UtcTime;

            Log.Debug($"AlpacaBacktestingBrokerage.CreateExitLegs: Creating exit legs for group {state.GroupId}. " +
                $"ExitQty={exitQty}, StopPrice={state.StopLossPrice}, TargetPrice={state.TakeProfitPrice}, " +
                $"StopLimitPrice={state.StopLossLimitPrice}");

            // --- Create stop-loss leg ---
            Order stopOrder;
            if (state.StopLossLimitPrice.HasValue)
            {
                // Stop-limit order
                stopOrder = new StopLimitOrder(
                    state.Symbol,
                    exitQty,
                    state.StopLossPrice,
                    state.StopLossLimitPrice.Value,
                    now,
                    tag: $"bracket:{state.GroupId}:stop");

                Log.Debug($"AlpacaBacktestingBrokerage.CreateExitLegs: Created StopLimitOrder for stop leg. " +
                    $"StopPrice={state.StopLossPrice}, LimitPrice={state.StopLossLimitPrice}");
            }
            else
            {
                // Stop-market order
                stopOrder = new StopMarketOrder(
                    state.Symbol,
                    exitQty,
                    state.StopLossPrice,
                    now,
                    tag: $"bracket:{state.GroupId}:stop");

                Log.Debug($"AlpacaBacktestingBrokerage.CreateExitLegs: Created StopMarketOrder for stop leg. " +
                    $"StopPrice={state.StopLossPrice}");
            }

            // Tag with bracket group for identification in PlaceOrder
            stopOrder.Properties = new AlpacaBracketOrderProperties
            {
                BracketGroupId = state.GroupId,
                LegType = BracketLegType.StopLoss
            };

            // Submit as a brokerage-originated order that LEAN will track.
            // The engine creates a proper LEAN order with an ID and calls our PlaceOrder.
            Log.Debug($"AlpacaBacktestingBrokerage.CreateExitLegs: Submitting stop leg via OnNewBrokerageOrderNotification for group {state.GroupId}");
            OnNewBrokerageOrderNotification(new NewBrokerageOrderNotificationEventArgs(stopOrder));

            // --- Create take-profit leg ---
            var targetOrder = new LimitOrder(
                state.Symbol,
                exitQty,
                state.TakeProfitPrice,
                now,
                tag: $"bracket:{state.GroupId}:target");

            targetOrder.Properties = new AlpacaBracketOrderProperties
            {
                BracketGroupId = state.GroupId,
                LegType = BracketLegType.TakeProfit
            };

            Log.Debug($"AlpacaBacktestingBrokerage.CreateExitLegs: Submitting target leg via OnNewBrokerageOrderNotification for group {state.GroupId}");
            OnNewBrokerageOrderNotification(new NewBrokerageOrderNotificationEventArgs(targetOrder));

            Log.Debug($"AlpacaBacktestingBrokerage.CreateExitLegs: Both exit legs submitted for group {state.GroupId}. " +
                $"They will be placed on the next Scan() cycle.");
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Cancels a bracket leg order if it exists and is still open.
        /// </summary>
        private void CancelLegIfOpen(int orderId)
        {
            if (orderId == 0)
            {
                Log.Debug("AlpacaBacktestingBrokerage.CancelLegIfOpen: OrderId is 0, skipping.");
                return;
            }

            var order = Algorithm.Transactions.GetOrderById(orderId);
            if (order == null)
            {
                Log.Debug($"AlpacaBacktestingBrokerage.CancelLegIfOpen: Order {orderId} not found in transaction manager.");
                return;
            }

            if (order.Status.IsClosed())
            {
                Log.Debug($"AlpacaBacktestingBrokerage.CancelLegIfOpen: Order {orderId} already closed (Status={order.Status}). Skipping cancel.");
                return;
            }

            Log.Debug($"AlpacaBacktestingBrokerage.CancelLegIfOpen: Cancelling order {orderId} (Status={order.Status})");
            base.CancelOrder(order);
        }

        /// <summary>
        /// Cancels both exit legs of a bracket group if they exist and are open.
        /// </summary>
        private void CancelLegsIfExist(BacktestBracketState state)
        {
            Log.Debug($"AlpacaBacktestingBrokerage.CancelLegsIfExist: Group {state.GroupId}. " +
                $"StopOrderId={state.StopOrderId}, TargetOrderId={state.TargetOrderId}");
            CancelLegIfOpen(state.StopOrderId);
            CancelLegIfOpen(state.TargetOrderId);
        }

        #endregion
    }

    /// <summary>
    /// Internal state tracking for a bracket group during backtesting.
    /// Tracks the entry, stop, and target order IDs along with prices.
    /// </summary>
    internal class BacktestBracketState
    {
        /// <summary>Bracket group unique identifier.</summary>
        public string GroupId { get; set; }

        /// <summary>The symbol being traded.</summary>
        public Symbol Symbol { get; set; }

        /// <summary>Signed entry quantity.</summary>
        public decimal Quantity { get; set; }

        /// <summary>LEAN order ID of the entry order.</summary>
        public int EntryOrderId { get; set; }

        /// <summary>LEAN order ID of the stop-loss exit leg. 0 if not yet created.</summary>
        public int StopOrderId { get; set; }

        /// <summary>LEAN order ID of the take-profit exit leg. 0 if not yet created.</summary>
        public int TargetOrderId { get; set; }

        /// <summary>Stop-loss trigger price.</summary>
        public decimal StopLossPrice { get; set; }

        /// <summary>Take-profit limit price.</summary>
        public decimal TakeProfitPrice { get; set; }

        /// <summary>Optional stop-limit price for the stop-loss leg.</summary>
        public decimal? StopLossLimitPrice { get; set; }

        public override string ToString()
        {
            return $"BacktestBracketState[{GroupId}] Entry={EntryOrderId}, Stop={StopOrderId}, " +
                $"Target={TargetOrderId}, StopPrice={StopLossPrice}, TargetPrice={TakeProfitPrice}";
        }
    }
}
