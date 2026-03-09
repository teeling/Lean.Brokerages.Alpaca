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

using QuantConnect.Orders;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Extends <see cref="AlpacaOrderProperties"/> with bracket order parameters.
    /// This is the data contract between <see cref="BracketOrderManager"/> and
    /// the brokerage layer. Both <see cref="AlpacaBacktestingBrokerage"/> and
    /// <see cref="AlpacaBrokerage"/> read these properties to detect and handle
    /// bracket orders.
    ///
    /// For bracket ENTRY orders, all price fields are set (IsBracketOrder == true).
    /// For bracket LEG orders (exit legs created by the brokerage), only BracketGroupId
    /// and optionally LegType are set (IsBracketOrder == false).
    /// </summary>
    public class AlpacaBracketOrderProperties : AlpacaOrderProperties
    {
        /// <summary>
        /// Unique identifier linking all orders in a bracket group.
        /// Set on both entry orders and exit leg orders.
        /// </summary>
        public string BracketGroupId { get; set; }

        /// <summary>
        /// The limit price for the take-profit exit leg.
        /// Only set on bracket entry orders.
        /// </summary>
        public decimal? TakeProfitLimitPrice { get; set; }

        /// <summary>
        /// The stop trigger price for the stop-loss exit leg.
        /// Only set on bracket entry orders.
        /// </summary>
        public decimal? StopLossStopPrice { get; set; }

        /// <summary>
        /// Optional limit price for the stop-loss leg (makes it a stop-limit order).
        /// If null, the stop-loss leg is a stop-market order.
        /// Only set on bracket entry orders.
        /// </summary>
        public decimal? StopLossLimitPrice { get; set; }

        /// <summary>
        /// Identifies what type of leg this order represents within a bracket group.
        /// Only set on bracket leg orders (not on entry orders).
        /// </summary>
        public BracketLegType? LegType { get; set; }

        /// <summary>
        /// True if this is a bracket entry order (has all required bracket prices).
        /// False for bracket leg orders or non-bracket orders.
        /// </summary>
        public bool IsBracketOrder =>
            !string.IsNullOrEmpty(BracketGroupId) &&
            TakeProfitLimitPrice.HasValue &&
            StopLossStopPrice.HasValue;
    }

    /// <summary>
    /// Identifies the type of exit leg within a bracket order group.
    /// </summary>
    public enum BracketLegType
    {
        /// <summary>
        /// Take-profit limit order leg.
        /// </summary>
        TakeProfit,

        /// <summary>
        /// Stop-loss order leg (stop-market or stop-limit).
        /// </summary>
        StopLoss
    }
}
