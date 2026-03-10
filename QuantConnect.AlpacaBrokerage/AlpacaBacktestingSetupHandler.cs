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

using QuantConnect.Brokerages.Backtesting;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Lean.Engine.Setup;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Setup handler that creates <see cref="AlpacaBacktestingBrokerage"/> for
    /// backtesting with bracket order support.
    ///
    /// This handler extends LEAN's standard <see cref="BacktestingSetupHandler"/>
    /// and overrides brokerage creation to use our bracket-aware backtesting brokerage.
    /// Everything else (algorithm initialization, data setup, etc.) remains unchanged.
    ///
    /// Configure in config.json:
    /// <code>
    /// {
    ///     "environment": "backtesting",
    ///     "setup-handler": "QuantConnect.Brokerages.Alpaca.AlpacaBacktestingSetupHandler"
    /// }
    /// </code>
    ///
    /// LEAN's Composer (reflection-based DI) loads setup handlers by type name.
    /// The plugin assembly must be in the engine's probing path, which is already
    /// the case for live trading. For backtesting, ensure the assembly is in the
    /// engine's bin directory or referenced in the LEAN solution.
    /// </summary>
    public class AlpacaBacktestingSetupHandler : BacktestingSetupHandler
    {
        /// <summary>
        /// Creates the bracket-aware backtesting brokerage instead of the default
        /// <see cref="QuantConnect.Brokerages.Backtesting.BacktestingBrokerage"/>.
        /// </summary>
        /// <param name="algorithmNodePacket">The algorithm node packet.</param>
        /// <param name="uninitializedAlgorithm">The uninitialized algorithm instance.</param>
        /// <param name="factory">Output: the brokerage factory (null for backtesting).</param>
        /// <returns>A new <see cref="AlpacaBacktestingBrokerage"/> instance.</returns>
        public override IBrokerage CreateBrokerage(AlgorithmNodePacket algorithmNodePacket, IAlgorithm uninitializedAlgorithm, out IBrokerageFactory factory)
        {
            factory = new BacktestingBrokerageFactory();
            Log.Debug("AlpacaBacktestingSetupHandler.CreateBrokerage: Creating AlpacaBacktestingBrokerage " +
                "with bracket order support.");
            return new AlpacaBacktestingBrokerage(uninitializedAlgorithm);
        }
    }
}
