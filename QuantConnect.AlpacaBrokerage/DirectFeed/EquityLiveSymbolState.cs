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

using Alpaca.Markets;
using NodaTime;

namespace QuantConnect.Brokerages.Alpaca.DirectFeed;

/// <summary>
/// Tracks per-symbol upstream Alpaca channel state for the direct-feed path.
/// One instance per equity symbol that has at least one active direct-feed subscription.
/// </summary>
internal sealed class EquityLiveSymbolState
{
    /// <summary>
    /// The LEAN symbol.
    /// </summary>
    public Symbol LeanSymbol { get; init; }

    /// <summary>
    /// The Alpaca brokerage symbol string (e.g. "SPY").
    /// </summary>
    public string AlpacaSymbol { get; init; }

    /// <summary>
    /// The exchange timezone for this symbol, used for UTC-to-exchange time conversion.
    /// </summary>
    public DateTimeZone ExchangeTimeZone { get; init; }

    /// <summary>
    /// Number of active LEAN subscriptions consuming minute trade bars for this symbol.
    /// </summary>
    public int TradeBarSubscribers;

    /// <summary>
    /// Number of active LEAN subscriptions consuming minute quote bars for this symbol.
    /// </summary>
    public int QuoteBarSubscribers;

    /// <summary>
    /// The upstream Alpaca minute bar subscription, or null if not currently subscribed.
    /// </summary>
    public IAlpacaDataSubscription<IBar> BarSubscription;

    /// <summary>
    /// The upstream Alpaca raw quote subscription, or null if not currently subscribed.
    /// </summary>
    public IAlpacaDataSubscription<IQuote> QuoteSubscription;

    /// <summary>
    /// Builds minute quote bars from incoming raw quotes.
    /// </summary>
    public MinuteQuoteBarBuilder QuoteBuilder;
}
