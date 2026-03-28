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
using System.Threading;
using Alpaca.Markets;
using NodaTime;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Brokerages.Alpaca.DirectFeed;

namespace QuantConnect.Brokerages.Alpaca;

public partial class AlpacaBrokerage
{
    /// <summary>
    /// Lookup from normalized subscription key to the direct-feed pipe LEAN consumes.
    /// </summary>
    private readonly ConcurrentDictionary<SubscriptionKey, DirectSubscriptionFeed> _directFeeds = new();

    /// <summary>
    /// Per-symbol upstream Alpaca channel state for symbols using the direct-feed path.
    /// </summary>
    private readonly ConcurrentDictionary<Symbol, EquityLiveSymbolState> _equityLiveState = new();

    /// <summary>
    /// Timer that flushes quote builders slightly before each minute boundary so that
    /// QuoteBars are available before the corresponding Alpaca TradeBar arrives.
    /// </summary>
    private Timer _quoteFlushTimer;

    /// <summary>
    /// Number of active direct-feed quote subscriptions. The flush timer runs only when > 0.
    /// </summary>
    private int _directQuoteSubscriberCount;

    // ──────────────────────────────────────────────────────────────────
    //  Eligibility
    // ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Determines whether a subscription should use the direct bar publishing path
    /// instead of the legacy aggregator pipeline.
    /// </summary>
    private static bool UseDirectBarPublishing(SubscriptionDataConfig config)
    {
        return config.Symbol.SecurityType == SecurityType.Equity
            && config.Resolution == Resolution.Minute
            && (config.TickType == TickType.Trade || config.TickType == TickType.Quote)
            && !config.Symbol.IsCanonical()
            && !config.IsCustomData;
    }

    // ──────────────────────────────────────────────────────────────────
    //  Upstream interest management (reference counting)
    // ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Register upstream Alpaca channel interest for a direct-feed subscription.
    /// On the first subscriber for a given tick type, subscribes the Alpaca channel.
    /// </summary>
    private void RegisterUpstreamInterest(SubscriptionDataConfig config)
    {
        var symbol = config.Symbol;
        var state = _equityLiveState.GetOrAdd(symbol, _ => CreateSymbolState(symbol));

        lock (state)
        {
            if (config.TickType == TickType.Trade)
            {
                state.TradeBarSubscribers++;
                if (state.TradeBarSubscribers == 1)
                {
                    SubscribeUpstreamBars(state);
                }
            }
            else if (config.TickType == TickType.Quote)
            {
                state.QuoteBarSubscribers++;
                if (state.QuoteBarSubscribers == 1)
                {
                    state.QuoteBuilder = new MinuteQuoteBarBuilder(symbol);
                    SubscribeUpstreamQuotes(state);

                    if (Interlocked.Increment(ref _directQuoteSubscriberCount) == 1)
                    {
                        StartQuoteFlushTimer();
                    }
                }
            }
        }
    }

    /// <summary>
    /// Remove upstream Alpaca channel interest for a direct-feed subscription.
    /// On the last subscriber for a given tick type, unsubscribes the Alpaca channel.
    /// </summary>
    private void RemoveUpstreamInterest(SubscriptionDataConfig config)
    {
        var symbol = config.Symbol;
        if (!_equityLiveState.TryGetValue(symbol, out var state))
        {
            return;
        }

        bool removeState;
        lock (state)
        {
            if (config.TickType == TickType.Trade)
            {
                if (state.TradeBarSubscribers > 0)
                {
                    state.TradeBarSubscribers--;
                }
                if (state.TradeBarSubscribers == 0)
                {
                    UnsubscribeUpstreamBars(state);
                }
            }
            else if (config.TickType == TickType.Quote)
            {
                if (state.QuoteBarSubscribers > 0)
                {
                    state.QuoteBarSubscribers--;
                }
                if (state.QuoteBarSubscribers == 0)
                {
                    UnsubscribeUpstreamQuotes(state);
                    state.QuoteBuilder = null;

                    if (Interlocked.Decrement(ref _directQuoteSubscriberCount) == 0)
                    {
                        StopQuoteFlushTimer();
                    }
                }
            }

            removeState = state.TradeBarSubscribers == 0 && state.QuoteBarSubscribers == 0;
        }

        if (removeState)
        {
            _equityLiveState.TryRemove(symbol, out _);
        }
    }

    private EquityLiveSymbolState CreateSymbolState(Symbol symbol)
    {
        var brokerageSymbol = _symbolMapper.GetBrokerageSymbol(symbol);
        var exchangeTimeZone = MarketHoursDatabase.FromDataFolder()
            .GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType)
            .TimeZone;

        // Ensure the reverse-lookup mapping is populated so handlers can find the LEAN symbol
        _dataSubscriptionByBrokerageSymbol[brokerageSymbol] = new()
        {
            Symbol = symbol,
            ExchangeTimeZone = exchangeTimeZone
        };

        return new EquityLiveSymbolState
        {
            LeanSymbol = symbol,
            AlpacaSymbol = brokerageSymbol,
            ExchangeTimeZone = exchangeTimeZone,
        };
    }

    // ──────────────────────────────────────────────────────────────────
    //  Upstream Alpaca minute bar subscriptions
    // ──────────────────────────────────────────────────────────────────

    private void SubscribeUpstreamBars(EquityLiveSymbolState state)
    {
        var streamingClient = GetStreamingDataClient(SecurityType.Equity);
        var barSubscription = streamingClient.GetMinuteBarSubscription(state.AlpacaSymbol);
        barSubscription.Received += HandleMinuteBarReceived;
        streamingClient.SubscribeAsync(barSubscription).ConfigureAwait(false).GetAwaiter().GetResult();
        state.BarSubscription = barSubscription;

        Log.Trace($"AlpacaBrokerage.LiveBars: subscribed upstream minute bars for {state.AlpacaSymbol}");
    }

    private void UnsubscribeUpstreamBars(EquityLiveSymbolState state)
    {
        if (state.BarSubscription == null) return;

        try
        {
            state.BarSubscription.Received -= HandleMinuteBarReceived;
            var streamingClient = GetStreamingDataClient(SecurityType.Equity);
            streamingClient.UnsubscribeAsync(state.BarSubscription).ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            Log.Error(ex, $"AlpacaBrokerage.LiveBars: error unsubscribing minute bars for {state.AlpacaSymbol}");
        }
        state.BarSubscription = null;
    }

    // ──────────────────────────────────────────────────────────────────
    //  Upstream Alpaca raw quote subscriptions
    // ──────────────────────────────────────────────────────────────────

    private void SubscribeUpstreamQuotes(EquityLiveSymbolState state)
    {
        var streamingClient = GetStreamingDataClient(SecurityType.Equity);
        var quoteSubscription = streamingClient.GetQuoteSubscription(state.AlpacaSymbol);
        quoteSubscription.Received += OnAlpacaQuote;
        streamingClient.SubscribeAsync(quoteSubscription).ConfigureAwait(false).GetAwaiter().GetResult();
        state.QuoteSubscription = quoteSubscription;

        Log.Trace($"AlpacaBrokerage.LiveBars: subscribed upstream quotes for {state.AlpacaSymbol}");
    }

    private void UnsubscribeUpstreamQuotes(EquityLiveSymbolState state)
    {
        if (state.QuoteSubscription == null) return;

        try
        {
            state.QuoteSubscription.Received -= OnAlpacaQuote;
            var streamingClient = GetStreamingDataClient(SecurityType.Equity);
            streamingClient.UnsubscribeAsync(state.QuoteSubscription).ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            Log.Error(ex, $"AlpacaBrokerage.LiveBars: error unsubscribing quotes for {state.AlpacaSymbol}");
        }
        state.QuoteSubscription = null;
    }

    // ──────────────────────────────────────────────────────────────────
    //  Alpaca minute bar handler → LEAN TradeBar
    // ──────────────────────────────────────────────────────────────────

    private void HandleMinuteBarReceived(global::Alpaca.Markets.IBar bar)
    {
        if (!_dataSubscriptionByBrokerageSymbol.TryGetValue(bar.Symbol, out var subscriptionData))
        {
            return;
        }

        var leanSymbol = subscriptionData.Symbol;
        var exchangeTime = bar.TimeUtc.ConvertFromUtc(subscriptionData.ExchangeTimeZone);

        var tradeBar = new TradeBar
        {
            Symbol = leanSymbol,
            Time = exchangeTime,
            EndTime = exchangeTime + TimeSpan.FromMinutes(1),
            Open = bar.Open,
            High = bar.High,
            Low = bar.Low,
            Close = bar.Close,
            Volume = bar.Volume,
            Period = TimeSpan.FromMinutes(1),
        };

        var key = new SubscriptionKey(leanSymbol, Resolution.Minute, TickType.Trade);
        if (_directFeeds.TryGetValue(key, out var feed))
        {
            feed.Queue.Enqueue(tradeBar);
            feed.NewDataAvailableHandler?.Invoke(this, EventArgs.Empty);

            if (Log.DebuggingEnabled)
            {
                Log.Debug($"AlpacaBrokerage.LiveBars: published TradeBar {leanSymbol.Value} {exchangeTime:yyyy-MM-dd HH:mm}");
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────
    //  Alpaca raw quote handler → MinuteQuoteBarBuilder → LEAN QuoteBar
    // ──────────────────────────────────────────────────────────────────

    private void OnAlpacaQuote(IQuote quote)
    {
        if (!_dataSubscriptionByBrokerageSymbol.TryGetValue(quote.Symbol, out var subscriptionData))
        {
            return;
        }

        var leanSymbol = subscriptionData.Symbol;

        if (!_equityLiveState.TryGetValue(leanSymbol, out var state))
        {
            return;
        }

        // Capture locally to avoid TOCTOU race — RemoveUpstreamInterest can null this field
        var builder = state.QuoteBuilder;
        if (builder == null)
        {
            return;
        }

        var exchangeTime = quote.TimestampUtc.ConvertFromUtc(subscriptionData.ExchangeTimeZone);
        var exchangeMinuteStart = exchangeTime.RoundDown(TimeSpan.FromMinutes(1));

        var completedBar = builder.Update(
            quote.BidPrice, quote.BidSize, quote.AskPrice, quote.AskSize,
            exchangeMinuteStart);

        if (completedBar != null)
        {
            PublishQuoteBar(leanSymbol, completedBar);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    //  Timer-based early flush (QuoteBar before TradeBar guarantee)
    // ──────────────────────────────────────────────────────────────────

    private void StartQuoteFlushTimer()
    {
        StopQuoteFlushTimer();

        // Calculate delay to next minute minus 500ms
        var now = DateTime.UtcNow;
        var nextMinute = now.RoundUp(TimeSpan.FromMinutes(1));
        var firstDelay = nextMinute - now - TimeSpan.FromMilliseconds(500);
        if (firstDelay < TimeSpan.Zero)
        {
            firstDelay += TimeSpan.FromMinutes(1);
        }

        // Use Timeout.InfiniteTimeSpan for period — we reschedule after each callback
        // to avoid accumulated drift over long sessions.
        _quoteFlushTimer = new Timer(_ => FlushAllQuoteBuilders(),
            null, firstDelay, Timeout.InfiniteTimeSpan);

        Log.Trace($"AlpacaBrokerage.LiveBars: quote flush timer started, first fire in {firstDelay.TotalSeconds:F1}s");
    }

    private void StopQuoteFlushTimer()
    {
        var timer = Interlocked.Exchange(ref _quoteFlushTimer, null);
        timer?.Dispose();
    }

    private void FlushAllQuoteBuilders()
    {
        try
        {
            foreach (var kvp in _equityLiveState)
            {
                // Capture locally to avoid TOCTOU race
                var builder = kvp.Value.QuoteBuilder;
                var completedBar = builder?.FlushCurrentMinute();
                if (completedBar != null)
                {
                    PublishQuoteBar(kvp.Key, completedBar);
                }
            }
        }
        catch (Exception ex)
        {
            Log.Error(ex, "AlpacaBrokerage.LiveBars: error in FlushAllQuoteBuilders");
        }

        // Reschedule for the next minute boundary minus 500ms to avoid drift
        RescheduleFlushTimer();
    }

    private void RescheduleFlushTimer()
    {
        var timer = _quoteFlushTimer;
        if (timer == null) return;

        try
        {
            var now = DateTime.UtcNow;
            var nextMinute = now.RoundUp(TimeSpan.FromMinutes(1));
            var delay = nextMinute - now - TimeSpan.FromMilliseconds(500);
            if (delay < TimeSpan.Zero)
            {
                delay += TimeSpan.FromMinutes(1);
            }

            timer.Change(delay, Timeout.InfiniteTimeSpan);
        }
        catch (ObjectDisposedException)
        {
            // Timer was disposed between the null check and Change() — harmless
        }
    }

    // ──────────────────────────────────────────────────────────────────
    //  Helpers
    // ──────────────────────────────────────────────────────────────────

    private void PublishQuoteBar(Symbol leanSymbol, QuoteBar quoteBar)
    {
        var key = new SubscriptionKey(leanSymbol, Resolution.Minute, TickType.Quote);
        if (_directFeeds.TryGetValue(key, out var feed))
        {
            feed.Queue.Enqueue(quoteBar);
            feed.NewDataAvailableHandler?.Invoke(this, EventArgs.Empty);

            if (Log.DebuggingEnabled)
            {
                Log.Debug($"AlpacaBrokerage.LiveBars: published QuoteBar {leanSymbol.Value} {quoteBar.Time:yyyy-MM-dd HH:mm}");
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────
    //  Reconnection for direct-feed channels
    // ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Called from ReconnectionLogic() after the legacy _subscriptionManager path
    /// has been resubscribed. Handles reconnection for all direct-feed channels.
    /// </summary>
    private void ReconnectDirectFeeds()
    {
        if (_equityLiveState.IsEmpty)
        {
            return;
        }

        foreach (var kvp in _equityLiveState)
        {
            var state = kvp.Value;

            // Reset partial quote builder state — do not carry partial data across outage
            state.QuoteBuilder?.Reset();

            // Detach old handlers before resubscribing to prevent leaks/duplicates.
            // Old subscription objects may be invalid after reconnect but we still
            // detach handlers defensively.
            DetachOldHandlers(state);

            // Resubscribe upstream Alpaca channels
            if (state.TradeBarSubscribers > 0)
            {
                try
                {
                    SubscribeUpstreamBars(state);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, $"AlpacaBrokerage.LiveBars: reconnect failed for minute bars {state.AlpacaSymbol}");
                }
            }
            if (state.QuoteBarSubscribers > 0)
            {
                try
                {
                    SubscribeUpstreamQuotes(state);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, $"AlpacaBrokerage.LiveBars: reconnect failed for quotes {state.AlpacaSymbol}");
                }
            }
        }

        // Restart flush timer aligned to current minute
        if (_directQuoteSubscriberCount > 0)
        {
            StartQuoteFlushTimer();
        }

        Log.Trace($"AlpacaBrokerage.LiveBars: direct-feed reconnection complete, {_equityLiveState.Count} symbols");
    }

    /// <summary>
    /// Detach event handlers from old subscription objects before reconnection.
    /// Does not attempt to UnsubscribeAsync (the old streaming client is dead).
    /// </summary>
    private void DetachOldHandlers(EquityLiveSymbolState state)
    {
        if (state.BarSubscription != null)
        {
            try { state.BarSubscription.Received -= HandleMinuteBarReceived; }
            catch { /* old subscription may be in a broken state */ }
            state.BarSubscription = null;
        }
        if (state.QuoteSubscription != null)
        {
            try { state.QuoteSubscription.Received -= OnAlpacaQuote; }
            catch { /* old subscription may be in a broken state */ }
            state.QuoteSubscription = null;
        }
    }
}
