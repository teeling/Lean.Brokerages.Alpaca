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
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantConnect.Interfaces;
using QuantConnect.Logging;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Algorithm-facing API for bracket orders. Provides a clean single-call
    /// interface for placing bracket orders (entry + stop-loss + take-profit),
    /// and tracks bracket group state from order events.
    ///
    /// This class contains NO brokerage logic — it does not create exit legs,
    /// does not handle OCO cancellation, and has zero environment branching
    /// (no LiveMode checks). The active brokerage (either
    /// <see cref="AlpacaBacktestingBrokerage"/> or <see cref="AlpacaBrokerage"/>)
    /// handles all bracket semantics internally.
    ///
    /// Usage from Python:
    /// <code>
    /// from QuantConnect.Brokerages.Alpaca import BracketOrderManager
    /// self.bracket = BracketOrderManager(self)
    /// group = self.bracket.PlaceBracketOrder(symbol, 100, 195.0, 210.0)
    /// </code>
    ///
    /// Usage from C#:
    /// <code>
    /// var bracket = new BracketOrderManager(this);
    /// var group = bracket.PlaceBracketOrder(Symbol("AAPL"), 100, 195m, 210m);
    /// </code>
    /// </summary>
    public class BracketOrderManager
    {
        private readonly IAlgorithm _algorithm;

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
        /// Creates a new BracketOrderManager.
        /// The algorithm should create this in Initialize() and store it as a field.
        ///
        /// Order events are forwarded to this manager by the brokerage layer
        /// (both AlpacaBacktestingBrokerage and AlpacaBrokerage call ProcessOrderEvent
        /// directly). This avoids relying on an algorithm-level event subscription
        /// which is not available on IAlgorithm.
        /// </summary>
        /// <param name="algorithm">The algorithm instance (pass 'this' from the algorithm).</param>
        public BracketOrderManager(IAlgorithm algorithm)
        {
            _algorithm = algorithm ?? throw new ArgumentNullException(nameof(algorithm));

            Log.Debug("BracketOrderManager: Initialized. Order events will be forwarded by the brokerage.");
        }

        #region Public API — Algorithm-Facing Methods

        /// <summary>
        /// Places a bracket order: an entry order with linked stop-loss and take-profit exit legs.
        /// The brokerage layer handles all bracket semantics (leg creation, OCO cancellation).
        ///
        /// This method validates parameters, creates a BracketGroup for state tracking,
        /// and submits the entry order with <see cref="AlpacaBracketOrderProperties"/>
        /// so the brokerage can detect it as a bracket order.
        /// </summary>
        /// <param name="symbol">The symbol to trade.</param>
        /// <param name="quantity">Signed quantity. Positive for long brackets (buy entry, sell exits).
        /// Negative for short brackets (sell entry, buy exits).</param>
        /// <param name="stopLossPrice">Stop-loss trigger price.</param>
        /// <param name="takeProfitPrice">Take-profit limit price.</param>
        /// <param name="entryType">Entry order type: Market (default), Limit, or StopLimit.</param>
        /// <param name="entryLimitPrice">Required if entryType is Limit or StopLimit (fill cap price).</param>
        /// <param name="entryStopPrice">Required if entryType is StopLimit (trigger price).</param>
        /// <param name="stopLossLimitPrice">Optional: makes the stop-loss a stop-limit order.</param>
        /// <param name="tag">Optional order tag for identification.</param>
        /// <returns>A <see cref="BracketGroup"/> for tracking the bracket's lifecycle.</returns>
        /// <exception cref="ArgumentException">If parameters fail validation.</exception>
        /// <exception cref="NotSupportedException">If entryType is not Market, Limit, or StopLimit.</exception>
        public BracketGroup PlaceBracketOrder(
            Symbol symbol,
            decimal quantity,
            decimal stopLossPrice,
            decimal takeProfitPrice,
            OrderType entryType = OrderType.Market,
            decimal? entryLimitPrice = null,
            decimal? entryStopPrice = null,
            decimal? stopLossLimitPrice = null,
            string tag = "")
        {
            // --- Round all prices to Alpaca's maximum precision ---
            // Matches LEAN's BrokerageTransactionHandler.RoundOrderPrices convention:
            // silently round and log a warning rather than throw. The algorithm should
            // not need to pre-round prices before calling PlaceBracketOrder.
            stopLossPrice    = RoundPrice(stopLossPrice, nameof(stopLossPrice));
            takeProfitPrice  = RoundPrice(takeProfitPrice, nameof(takeProfitPrice));
            if (entryLimitPrice.HasValue)   entryLimitPrice   = RoundPrice(entryLimitPrice.Value,   nameof(entryLimitPrice));
            if (entryStopPrice.HasValue)    entryStopPrice    = RoundPrice(entryStopPrice.Value,    nameof(entryStopPrice));
            if (stopLossLimitPrice.HasValue) stopLossLimitPrice = RoundPrice(stopLossLimitPrice.Value, nameof(stopLossLimitPrice));

            // --- Validate all parameters before doing anything ---
            ValidateBracketParameters(symbol, quantity, stopLossPrice,
                takeProfitPrice, entryType, entryLimitPrice, entryStopPrice);

            // --- Create the bracket group for state tracking ---
            var groupId = Guid.NewGuid().ToString("N");
            var group = new BracketGroup(groupId, symbol, quantity,
                stopLossPrice, takeProfitPrice, stopLossLimitPrice,
                entryType, entryStopPrice);
            _groups[groupId] = group;

            Log.Debug($"BracketOrderManager.PlaceBracketOrder: Created group {groupId} " +
                $"for {symbol} qty={quantity} stop={stopLossPrice} target={takeProfitPrice} " +
                $"entryType={entryType} entryStop={entryStopPrice} stopLimitPrice={stopLossLimitPrice}");

            // --- Build bracket properties for the brokerage to detect ---
            // This is the data contract between the manager and the brokerage.
            // The brokerage reads these properties to know this is a bracket entry.
            var props = new AlpacaBracketOrderProperties
            {
                BracketGroupId = groupId,
                TakeProfitLimitPrice = takeProfitPrice,
                StopLossStopPrice = stopLossPrice,
                StopLossLimitPrice = stopLossLimitPrice,
                OriginatingManager = this,
            };

            // --- Place the entry order through LEAN's SubmitOrderRequest API ---
            // IAlgorithm exposes SubmitOrderRequest (not MarketOrder/LimitOrder which
            // live on the concrete QCAlgorithm class). The brokerage intercepts this
            // in PlaceOrder, detects the AlpacaBracketOrderProperties, and handles
            // bracket creation.
            var entryTag = string.IsNullOrEmpty(tag) ? $"bracket:{groupId}:entry" : tag;
            SubmitOrderRequest request;
            switch (entryType)
            {
                case OrderType.Market:
                    Log.Debug($"BracketOrderManager.PlaceBracketOrder: Placing market entry for group {groupId}");
                    request = new SubmitOrderRequest(OrderType.Market, symbol.SecurityType,
                        symbol, quantity, 0, 0, _algorithm.UtcTime, entryTag, props);
                    break;

                case OrderType.Limit:
                    Log.Debug($"BracketOrderManager.PlaceBracketOrder: Placing limit entry at {entryLimitPrice} for group {groupId}");
                    request = new SubmitOrderRequest(OrderType.Limit, symbol.SecurityType,
                        symbol, quantity, 0, entryLimitPrice.Value, _algorithm.UtcTime, entryTag, props);
                    break;

                case OrderType.StopLimit:
                    Log.Debug($"BracketOrderManager.PlaceBracketOrder: Placing stop-limit entry " +
                        $"stop={entryStopPrice} limit={entryLimitPrice} for group {groupId}");
                    request = new SubmitOrderRequest(OrderType.StopLimit, symbol.SecurityType,
                        symbol, quantity, entryStopPrice.Value, entryLimitPrice.Value,
                        _algorithm.UtcTime, entryTag, props);
                    break;

                default:
                    throw new NotSupportedException(
                        $"BracketOrderManager: Entry type {entryType} not supported. Use Market, Limit, or StopLimit.");
            }

            var entryTicket = _algorithm.SubmitOrderRequest(request);

            // --- Register the entry ticket with the group ---
            group.EntryTicket = entryTicket;
            _orderToGroup[entryTicket.OrderId] = groupId;

            Log.Debug($"BracketOrderManager.PlaceBracketOrder: Entry ticket ID={entryTicket.OrderId} " +
                $"registered for group {groupId}. Entry status: {entryTicket.Status}");

            return group;
        }

        /// <summary>
        /// Cancels an entire bracket order group.
        /// If the entry hasn't filled, cancels the entry (brokerage cascades to legs).
        /// If the entry has filled, cancels one exit leg (brokerage handles OCO cascade).
        /// </summary>
        /// <param name="groupId">The bracket group ID to cancel.</param>
        /// <returns>True if a cancel request was submitted, false if group not found or already complete.</returns>
        public bool CancelBracket(string groupId)
        {
            if (!_groups.TryGetValue(groupId, out var group))
            {
                Log.Error($"BracketOrderManager.CancelBracket: Group {groupId} not found.");
                return false;
            }

            if (group.IsComplete)
            {
                Log.Debug($"BracketOrderManager.CancelBracket: Group {groupId} already complete, nothing to cancel.");
                return false;
            }

            Log.Debug($"BracketOrderManager.CancelBracket: Cancelling group {groupId}. " +
                $"EntryFilled={group.EntryFilled}");

            // Record that a cancel was requested regardless of current state.
            // The backtesting brokerage checks this after CreateExitLegs to handle the race
            // where the entry fills after CancelBracket was called (cancel-after-update race).
            group.CancelRequested = true;

            // Do NOT set IsCancelled here eagerly — it will be set by ProcessOrderEvent
            // when the actual Canceled OrderEvent arrives from the brokerage. Setting it
            // eagerly would cause IsComplete to return true even if the cancel is rejected.

            if (!group.EntryFilled)
            {
                // Cancel entry — brokerage handles cascade to legs.
                // IMPORTANT: If the entry is still in "New" status (not yet ACK'd by the
                // broker), LEAN will silently reject the cancel. In that case we set
                // PendingCancel so ProcessOrderEvent retries when the order transitions
                // to Submitted or Filled.
                var entryTicket = group.EntryTicket;
                if (entryTicket == null)
                {
                    Log.Error($"BracketOrderManager.CancelBracket: Entry ticket is null for group {groupId}");
                    return false;
                }

                if (entryTicket.Status == OrderStatus.New)
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
            else
            {
                // Cancel an active exit leg — brokerage handles OCO cascade.
                // IMPORTANT: If one leg has a pending Alpaca replace (update), we MUST
                // cancel THAT leg, not the sibling. Alpaca locks sibling orders during
                // a replace and rejects cancel attempts with "pending replacement".
                // Cancelling the leg that is itself mid-replace is allowed by Alpaca.
                OrderTicket activeLeg;
                if (group.PendingUpdateOrderIds.Count > 0)
                {
                    // Prefer a leg that has a pending update — Alpaca allows cancelling
                    // an order that is itself mid-replace, but rejects sibling cancels.
                    activeLeg = null;
                    if (group.StopTicket?.Status.IsOpen() == true &&
                        group.PendingUpdateOrderIds.Contains(group.StopTicket.OrderId))
                        activeLeg = group.StopTicket;
                    else if (group.TargetTicket?.Status.IsOpen() == true &&
                        group.PendingUpdateOrderIds.Contains(group.TargetTicket.OrderId))
                        activeLeg = group.TargetTicket;

                    // Fallback if the pending leg is already closed
                    activeLeg ??= group.StopTicket?.Status.IsOpen() == true ? group.StopTicket : group.TargetTicket;

                    Log.Debug($"BracketOrderManager.CancelBracket: Pending updates on orders [{string.Join(",", group.PendingUpdateOrderIds)}], " +
                        $"preferring pending leg for cancel. Selected: {activeLeg?.OrderId}");
                }
                else
                {
                    activeLeg = group.StopTicket?.Status.IsOpen() == true
                        ? group.StopTicket
                        : group.TargetTicket;
                }

                if (activeLeg != null && activeLeg.Status.IsOpen())
                {
                    Log.Debug($"BracketOrderManager.CancelBracket: Cancelling exit leg ticket {activeLeg.OrderId} for group {groupId}");
                    activeLeg.Cancel($"Bracket cancelled: {groupId}");
                }
                else
                {
                    Log.Error($"BracketOrderManager.CancelBracket: No active exit leg found for group {groupId}. " +
                        $"StopTicket={group.StopTicket?.OrderId}({group.StopTicket?.Status}), " +
                        $"TargetTicket={group.TargetTicket?.OrderId}({group.TargetTicket?.Status})");
                }
            }

            return true;
        }

        /// <summary>
        /// Updates the stop-loss price for an active bracket.
        /// Only works after the entry has filled and the stop leg is open.
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

            if (!group.EntryFilled)
            {
                Log.Error($"BracketOrderManager.UpdateStop: Entry not yet filled for group {groupId}. Cannot update stop.");
                return false;
            }

            if (group.StopTicket == null || group.StopTicket.Status.IsClosed())
            {
                Log.Error($"BracketOrderManager.UpdateStop: Stop ticket is null or closed for group {groupId}. " +
                    $"StopTicket={group.StopTicket?.OrderId}({group.StopTicket?.Status})");
                return false;
            }

            // Guard: stop must be strictly below the current target price.
            // Alpaca rejects brackets where stop >= take_profit.
            if (group.TakeProfitPrice > 0 && newStopPrice >= group.TakeProfitPrice)
            {
                Log.Error($"BracketOrderManager.UpdateStop: Rejected update for group {groupId} — " +
                    $"newStopPrice {newStopPrice} >= takeProfitPrice {group.TakeProfitPrice}. " +
                    $"Stop must be strictly below the target.");
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

            // Track that this leg has a pending replace so CancelBracket can
            // avoid cancelling the sibling (Alpaca locks siblings during replace).
            group.PendingUpdateOrderIds.Add(group.StopTicket.OrderId);

            var response = group.StopTicket.Update(fields);

            if (response.IsSuccess)
            {
                group.StopLossPrice = newStopPrice;
                Log.Debug($"BracketOrderManager.UpdateStop: Successfully updated stop for group {groupId} to {newStopPrice}");
            }
            else
            {
                group.PendingUpdateOrderIds.Remove(group.StopTicket.OrderId);
                Log.Error($"BracketOrderManager.UpdateStop: Failed to update stop for group {groupId}. " +
                    $"Response: {response.ErrorCode} - {response.ErrorMessage}");
            }

            return response.IsSuccess;
        }

        /// <summary>
        /// Updates the take-profit price for an active bracket.
        /// Only works after the entry has filled and the target leg is open.
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

            if (!group.EntryFilled)
            {
                Log.Error($"BracketOrderManager.UpdateTarget: Entry not yet filled for group {groupId}. Cannot update target.");
                return false;
            }

            if (group.TargetTicket == null || group.TargetTicket.Status.IsClosed())
            {
                Log.Error($"BracketOrderManager.UpdateTarget: Target ticket is null or closed for group {groupId}. " +
                    $"TargetTicket={group.TargetTicket?.OrderId}({group.TargetTicket?.Status})");
                return false;
            }

            // Guard: target must be strictly above the current stop price.
            // Alpaca rejects brackets where take_profit <= stop_loss.
            if (group.StopLossPrice > 0 && newLimitPrice <= group.StopLossPrice)
            {
                Log.Error($"BracketOrderManager.UpdateTarget: Rejected update for group {groupId} — " +
                    $"newTargetPrice {newLimitPrice} <= stopLossPrice {group.StopLossPrice}. " +
                    $"Target must be strictly above the stop.");
                return false;
            }

            Log.Debug($"BracketOrderManager.UpdateTarget: Updating target for group {groupId} " +
                $"from {group.TakeProfitPrice} to {newLimitPrice}");

            var fields = new UpdateOrderFields { LimitPrice = newLimitPrice };

            // Track that this leg has a pending replace so CancelBracket can
            // avoid cancelling the sibling (Alpaca locks siblings during replace).
            group.PendingUpdateOrderIds.Add(group.TargetTicket.OrderId);

            var response = group.TargetTicket.Update(fields);

            if (response.IsSuccess)
            {
                group.TakeProfitPrice = newLimitPrice;
                Log.Debug($"BracketOrderManager.UpdateTarget: Successfully updated target for group {groupId} to {newLimitPrice}");
            }
            else
            {
                group.PendingUpdateOrderIds.Remove(group.TargetTicket.OrderId);
                Log.Error($"BracketOrderManager.UpdateTarget: Failed to update target for group {groupId}. " +
                    $"Response: {response.ErrorCode} - {response.ErrorMessage}");
            }

            return response.IsSuccess;
        }

        /// <summary>
        /// Updates the entry order price(s) for an active bracket.
        /// Safe to call immediately after PlaceBracketOrder — if the entry order is still
        /// in "New" state (not yet ACK'd by the broker), the update is deferred and replayed
        /// automatically by ProcessOrderEvent once the order transitions to Submitted.
        /// Only valid for Limit or StopLimit entry brackets (market entries cannot be updated).
        /// For stop-limit entries, pass both newLimitPrice and newEntryStopPrice to move
        /// the trigger and fill cap together.
        /// </summary>
        /// <param name="groupId">The bracket group ID.</param>
        /// <param name="newLimitPrice">The new entry limit (fill cap) price.</param>
        /// <param name="newEntryStopPrice">Optional new stop trigger price (stop-limit entries only).</param>
        /// <returns>True if the update was submitted (or deferred) successfully.</returns>
        public bool UpdateEntry(string groupId, decimal newLimitPrice, decimal? newEntryStopPrice = null)
        {
            if (!_groups.TryGetValue(groupId, out var group))
            {
                Log.Error($"BracketOrderManager.UpdateEntry: Group {groupId} not found.");
                return false;
            }

            if (group.EntryTicket == null || group.EntryTicket.Status.IsClosed())
            {
                Log.Error($"BracketOrderManager.UpdateEntry: Entry ticket is null or closed for group {groupId}. " +
                    $"EntryTicket={group.EntryTicket?.OrderId}({group.EntryTicket?.Status})");
                return false;
            }

            var fields = new UpdateOrderFields { LimitPrice = newLimitPrice };
            if (newEntryStopPrice.HasValue)
                fields.StopPrice = newEntryStopPrice.Value;

            // If the entry order hasn't been ACK'd yet (still New), LEAN will reject the
            // Update() call before it reaches our brokerage plugin. Defer it: store the
            // requested fields and ProcessOrderEvent will replay when status transitions.
            if (group.EntryTicket.Status == OrderStatus.New)
            {
                Log.Debug($"BracketOrderManager.UpdateEntry: Entry still New for group {groupId}. " +
                    $"Deferring update to limit={newLimitPrice}" +
                    (newEntryStopPrice.HasValue ? $" stop={newEntryStopPrice}" : "") +
                    " until Submitted.");
                group.PendingEntryUpdate = fields;
                if (newEntryStopPrice.HasValue) group.EntryStopPrice = newEntryStopPrice.Value;
                return true;
            }

            group.PendingEntryUpdate = null;
            Log.Debug($"BracketOrderManager.UpdateEntry: Updating entry for group {groupId} " +
                $"to limit={newLimitPrice}" + (newEntryStopPrice.HasValue ? $" stop={newEntryStopPrice}" : ""));
            var response = group.EntryTicket.Update(fields);
            if (response.IsSuccess)
            {
                if (newEntryStopPrice.HasValue) group.EntryStopPrice = newEntryStopPrice.Value;
            }
            else
            {
                Log.Error($"BracketOrderManager.UpdateEntry: Failed to update entry for group {groupId}. " +
                    $"Response: {response.ErrorCode} - {response.ErrorMessage}");
            }
            return response.IsSuccess;
        }

        #endregion

        #region Query Methods

        /// <summary>
        /// Gets a bracket group by its ID. Returns null if not found.
        /// </summary>
        public BracketGroup GetGroup(string groupId) =>
            _groups.TryGetValue(groupId, out var g) ? g : null;

        /// <summary>
        /// Gets all active (not yet complete) bracket groups.
        /// </summary>
        public IEnumerable<BracketGroup> GetActiveGroups() =>
            _groups.Values.Where(g => !g.IsComplete);

        /// <summary>
        /// Gets all bracket groups (active and complete).
        /// </summary>
        public IEnumerable<BracketGroup> GetAllGroups() =>
            _groups.Values;

        /// <summary>
        /// Gets the bracket group ID for a given LEAN order ID.
        /// Returns null if the order is not part of a bracket.
        /// </summary>
        public string GetGroupIdForOrder(int orderId) =>
            _orderToGroup.TryGetValue(orderId, out var groupId) ? groupId : null;

        #endregion

        #region Leg Registration (Called by Brokerages)

        /// <summary>
        /// Called by the brokerage to register phantom LEAN orders for bracket
        /// exit legs. Both <see cref="AlpacaBacktestingBrokerage"/> and
        /// <see cref="AlpacaBrokerage"/> call this when they create exit leg orders.
        ///
        /// This links the leg's order ticket to the bracket group and registers
        /// the leg's order ID in the event routing map.
        /// </summary>
        /// <param name="groupId">The bracket group ID this leg belongs to.</param>
        /// <param name="legType">Whether this is a stop-loss or take-profit leg.</param>
        /// <param name="ticket">The LEAN order ticket for the leg.</param>
        public void RegisterLegTicket(string groupId, BracketLegType legType, OrderTicket ticket)
        {
            if (!_groups.TryGetValue(groupId, out var group))
            {
                Log.Error($"BracketOrderManager.RegisterLegTicket: Group {groupId} not found. " +
                    $"Cannot register {legType} leg with ticket {ticket?.OrderId}.");
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

        #endregion

        #region Order Event Processing (Pure State Tracking)

        /// <summary>
        /// Processes order events for bracket-related orders. This is pure state tracking —
        /// no brokerage logic, no order placement, no OCO cancellation.
        /// The brokerage handles all of that independently.
        ///
        /// Called by the brokerage layer (both AlpacaBacktestingBrokerage and AlpacaBrokerage)
        /// whenever a bracket-related order event occurs.
        /// </summary>
        /// <param name="e">The order event to process.</param>
        public void ProcessOrderEvent(OrderEvent e)
        {
            // Only process events for orders we're tracking
            if (!_orderToGroup.TryGetValue(e.OrderId, out var groupId))
            {
                Log.Trace($"BracketOrderManager.ProcessOrderEvent: OrderId={e.OrderId} not in _orderToGroup (count={_orderToGroup.Count}). Ignoring.");
                return;
            }

            if (!_groups.TryGetValue(groupId, out var group))
            {
                Log.Error($"BracketOrderManager.ProcessOrderEvent: Group {groupId} found in _orderToGroup " +
                    $"but missing from _groups. OrderId={e.OrderId}, Status={e.Status}");
                return;
            }

            Log.Trace($"BracketOrderManager.ProcessOrderEvent: Group {groupId}, " +
                $"OrderId={e.OrderId}, Status={e.Status}, FillQty={e.FillQuantity}, " +
                $"FillPrice={e.FillPrice}, EntryTicketId={group.EntryTicket?.OrderId ?? -1}");

            // --- Entry order events ---
            if (e.OrderId == group.EntryTicket?.OrderId)
            {
                if (e.Status == OrderStatus.Filled)
                {
                    group.EntryFilled = true;
                    group.FilledQuantity += e.FillQuantity;
                    group.FillPrice = e.FillPrice;
                    Log.Debug($"BracketOrderManager.OnOrderEvent: Entry FILLED for group {groupId} " +
                        $"at price {e.FillPrice}. FilledQty={group.FilledQuantity}/{group.Quantity}. " +
                        $"Waiting for brokerage to create exit legs.");
                }
                else if (e.Status == OrderStatus.PartiallyFilled)
                {
                    // Track cumulative fill quantity. In live trading, Alpaca automatically
                    // adjusts exit leg quantities to match the filled entry quantity.
                    // We track this so the algorithm can see accurate state.
                    group.FilledQuantity += e.FillQuantity;
                    group.FillPrice = e.FillPrice;
                    Log.Debug($"BracketOrderManager.OnOrderEvent: Entry PARTIAL FILL for group {groupId} " +
                        $"at price {e.FillPrice}. FilledQty={group.FilledQuantity}/{group.Quantity}. " +
                        $"Alpaca will adjust exit leg quantities to match.");
                }
                else if (e.Status == OrderStatus.Canceled || e.Status == OrderStatus.Invalid)
                {
                    group.PendingCancel = false; // no longer needed
                    group.IsCancelled = true;
                    Log.Debug($"BracketOrderManager.OnOrderEvent: Entry CANCELLED/INVALID for group {groupId}. " +
                        $"Status={e.Status}. Bracket is now complete.");
                }
                else
                {
                    Log.Debug($"BracketOrderManager.OnOrderEvent: Entry status update for group {groupId}: {e.Status}");
                }

                // --- Deferred cancel: retry CancelBracket now that the order has left "New" status ---
                // This handles the race where CancelBracket was called before the broker ACK'd the order.
                // Now that LEAN has a non-New status, the cancel will be accepted.
                if (group.PendingCancel && e.Status != OrderStatus.New)
                {
                    group.PendingCancel = false;
                    Log.Debug($"BracketOrderManager.ProcessOrderEvent: Executing deferred CancelBracket " +
                        $"for group {groupId} (entry now {e.Status})");
                    CancelBracket(groupId);
                }

                // --- Deferred entry update: replay UpdateEntry now that the order has left "New" status ---
                // This handles the race where UpdateEntry() was called before the broker ACK'd the order.
                // LEAN hard-rejects updates on "New" orders, so UpdateEntry stored the fields here.
                // If the entry was cancelled/invalid (e.g. CancelBracket raced ahead), discard silently.
                if (group.PendingEntryUpdate != null && e.Status != OrderStatus.New)
                {
                    var pendingFields = group.PendingEntryUpdate;
                    group.PendingEntryUpdate = null;
                    if (group.IsCancelled)
                    {
                        Log.Debug($"BracketOrderManager.ProcessOrderEvent: Discarding deferred entry update " +
                            $"for group {groupId} — entry was cancelled before replay.");
                    }
                    else
                    {
                        Log.Debug($"BracketOrderManager.ProcessOrderEvent: Replaying deferred entry update " +
                            $"for group {groupId} to {pendingFields.LimitPrice} (entry now {e.Status})");
                        var updateResponse = group.EntryTicket?.Update(pendingFields);
                        if (updateResponse?.IsSuccess == false)
                        {
                            Log.Error($"BracketOrderManager.ProcessOrderEvent: Deferred entry update failed for group {groupId}. " +
                                $"{updateResponse.ErrorCode} - {updateResponse.ErrorMessage}");
                        }
                    }
                }

                return;
            }

            // --- Clear pending update tracking ---
            // When the update completes (UpdateSubmitted) or the order reaches any
            // terminal/closed state, clear the pending flag so CancelBracket reverts
            // to default leg selection.
            if (group.PendingUpdateOrderIds.Contains(e.OrderId) &&
                (e.Status == OrderStatus.UpdateSubmitted || e.Status.IsClosed()))
            {
                Log.Debug($"BracketOrderManager.ProcessOrderEvent: Clearing PendingUpdateOrderId={e.OrderId} " +
                    $"for group {groupId} (status={e.Status})");
                group.PendingUpdateOrderIds.Remove(e.OrderId);
            }

            // --- Exit leg events ---
            if (e.Status == OrderStatus.Filled)
            {
                group.ExitFilled = true;
                group.ExitOrderId = e.OrderId;
                group.ExitPrice = e.FillPrice;

                var legName = (e.OrderId == group.StopTicket?.OrderId) ? "STOP" : "TARGET";
                Log.Debug($"BracketOrderManager.OnOrderEvent: {legName} leg FILLED for group {groupId} " +
                    $"at price {e.FillPrice}. Bracket is now complete. " +
                    $"Brokerage should cancel the sibling leg (OCO).");
            }
            else if (e.Status == OrderStatus.PartiallyFilled)
            {
                // In live trading, Alpaca handles partial exit fills by adjusting the
                // sibling leg quantity. We log this for visibility but don't mark ExitFilled
                // until we get the final Filled event.
                var legName = (e.OrderId == group.StopTicket?.OrderId) ? "STOP" : "TARGET";
                Log.Debug($"BracketOrderManager.OnOrderEvent: {legName} leg PARTIAL FILL for group {groupId} " +
                    $"at price {e.FillPrice}. FillQty={e.FillQuantity}. " +
                    $"Alpaca will adjust sibling leg quantity accordingly.");
            }
            else if (e.Status == OrderStatus.Canceled)
            {
                // Could be OCO cascade (brokerage cancelled sibling) or user cancel.
                // Just track it — the brokerage already handled the sibling cancellation.
                var legName = (e.OrderId == group.StopTicket?.OrderId) ? "STOP" : "TARGET";
                Log.Debug($"BracketOrderManager.OnOrderEvent: {legName} leg CANCELLED for group {groupId}. " +
                    $"(OCO cascade or user cancel)");

                // If both legs are now closed and none filled, mark as cancelled
                var stopClosed = group.StopTicket == null || group.StopTicket.Status.IsClosed();
                var targetClosed = group.TargetTicket == null || group.TargetTicket.Status.IsClosed();
                if (stopClosed && targetClosed && !group.ExitFilled)
                {
                    group.IsCancelled = true;
                    Log.Debug($"BracketOrderManager.OnOrderEvent: Both exit legs closed without fill " +
                        $"for group {groupId}. Bracket marked as cancelled.");
                }
            }
            else
            {
                var legName = (e.OrderId == group.StopTicket?.OrderId) ? "STOP" : "TARGET";
                Log.Debug($"BracketOrderManager.OnOrderEvent: {legName} leg status update for group {groupId}: {e.Status}");
            }
        }

        #endregion

        #region Validation

        /// <summary>
        /// Validates bracket order parameters before submission.
        /// Throws <see cref="ArgumentException"/> for invalid parameters.
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
            {
                throw new ArgumentException("BracketOrderManager: Symbol cannot be null.");
            }

            if (quantity == 0)
            {
                throw new ArgumentException("BracketOrderManager: Quantity cannot be zero.");
            }

            if (entryType == OrderType.Limit && !entryLimitPrice.HasValue)
            {
                throw new ArgumentException(
                    "BracketOrderManager: entryLimitPrice is required for limit entry orders.");
            }

            if (entryType == OrderType.StopLimit)
            {
                if (!entryStopPrice.HasValue)
                    throw new ArgumentException(
                        "BracketOrderManager: entryStopPrice is required for stop-limit entry orders.");
                if (!entryLimitPrice.HasValue)
                    throw new ArgumentException(
                        "BracketOrderManager: entryLimitPrice is required for stop-limit entry orders.");
            }

            // Long bracket: target must be above stop
            if (quantity > 0 && takeProfitPrice <= stopLossPrice)
            {
                throw new ArgumentException(
                    $"BracketOrderManager: Long bracket: takeProfitPrice ({takeProfitPrice}) " +
                    $"must be greater than stopLossPrice ({stopLossPrice}).");
            }

            // Short bracket: target must be below stop
            if (quantity < 0 && takeProfitPrice >= stopLossPrice)
            {
                throw new ArgumentException(
                    $"BracketOrderManager: Short bracket: takeProfitPrice ({takeProfitPrice}) " +
                    $"must be less than stopLossPrice ({stopLossPrice}).");
            }

            // Alpaca minimum $0.01 separation between stop and target
            if (Math.Abs(takeProfitPrice - stopLossPrice) < 0.01m)
            {
                throw new ArgumentException(
                    "BracketOrderManager: Take-profit and stop-loss must be at least $0.01 apart.");
            }

        }

        /// <summary>
        /// Rounds a price to Alpaca's maximum allowed precision and logs a warning if
        /// rounding was applied. Matches the convention of LEAN's BrokerageTransactionHandler
        /// which rounds silently rather than rejecting the order.
        ///
        /// Alpaca precision rules:
        ///   Prices >= $1.00 → max 2 decimal places (cent precision)
        ///   Prices  < $1.00 → max 4 decimal places (sub-penny stocks)
        /// </summary>
        private static decimal RoundPrice(decimal price, string name)
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
    }
}
