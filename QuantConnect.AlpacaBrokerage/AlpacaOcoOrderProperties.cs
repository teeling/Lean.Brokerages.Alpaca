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

using Newtonsoft.Json;
using QuantConnect.Orders;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Extends <see cref="AlpacaOrderProperties"/> with OCO (One-Cancels-Other) order parameters.
    /// This is the data contract between <see cref="BracketOrderManager"/> and the brokerage layer
    /// for OCO orders (a linked pair of sell orders: limit take-profit + stop-loss).
    ///
    /// Unlike bracket orders, OCO orders have no entry leg — they protect an existing position.
    /// The primary order submitted through LEAN's pipeline is the take-profit (Limit) leg.
    /// The stop-loss leg is created by the brokerage as a phantom LEAN order (same as bracket legs).
    /// </summary>
    public class AlpacaOcoOrderProperties : AlpacaOrderProperties
    {
        /// <summary>
        /// Unique identifier linking both orders in the OCO group.
        /// Maps to a <see cref="BracketGroup"/> with EntryFilled = true and no EntryTicket.
        /// </summary>
        public string OcoGroupId { get; set; }

        /// <summary>
        /// Stop-loss trigger price for the stop leg of the OCO.
        /// </summary>
        public decimal? StopLossStopPrice { get; set; }

        /// <summary>
        /// Optional stop-limit price for the stop-loss leg.
        /// If set, the stop leg is a stop-limit order instead of stop-market.
        /// </summary>
        public decimal? StopLossLimitPrice { get; set; }

        /// <summary>
        /// Reference to the <see cref="BracketOrderManager"/> that created this OCO order.
        /// Used by the brokerage to auto-register the manager on first OCO order.
        /// </summary>
        [JsonIgnore]
        internal BracketOrderManager OriginatingManager { get; set; }

        /// <summary>
        /// True if this order has valid OCO parameters (group ID + stop price).
        /// </summary>
        public bool IsOcoOrder =>
            !string.IsNullOrEmpty(OcoGroupId) &&
            StopLossStopPrice.HasValue;
    }
}
