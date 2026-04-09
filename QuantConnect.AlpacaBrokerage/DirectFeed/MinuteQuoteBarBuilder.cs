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
using QuantConnect.Data.Market;

namespace QuantConnect.Brokerages.Alpaca.DirectFeed;

/// <summary>
/// Aggregates raw Alpaca quotes into minute-resolution <see cref="QuoteBar"/> objects.
/// Thread-safe: all public methods are synchronized via a lock.
/// </summary>
internal sealed class MinuteQuoteBarBuilder
{
    private static readonly TimeSpan MinutePeriod = TimeSpan.FromMinutes(1);

    private readonly Symbol _symbol;
    private readonly object _lock = new();

    private DateTime? _currentMinuteStart;
    private bool _currentMinuteEmitted;

    // Bid OHLC
    private decimal? _bidOpen, _bidHigh, _bidLow, _bidClose;
    // Ask OHLC
    private decimal? _askOpen, _askHigh, _askLow, _askClose;
    // Last seen sizes in the current bucket
    private decimal _lastBidSize;
    private decimal _lastAskSize;

    public MinuteQuoteBarBuilder(Symbol symbol)
    {
        _symbol = symbol;
    }

    /// <summary>
    /// Feed a quote into the builder. If the quote belongs to a new minute, the previous
    /// minute's <see cref="QuoteBar"/> is finalized and returned. Otherwise returns null.
    /// </summary>
    /// <param name="bidPrice">The bid price from the quote.</param>
    /// <param name="bidSize">The bid size from the quote.</param>
    /// <param name="askPrice">The ask price from the quote.</param>
    /// <param name="askSize">The ask size from the quote.</param>
    /// <param name="exchangeMinuteStart">The quote timestamp rounded down to the minute in exchange time.</param>
    /// <returns>A completed <see cref="QuoteBar"/> for the previous minute, or null.</returns>
    public QuoteBar Update(decimal bidPrice, decimal bidSize, decimal askPrice, decimal askSize, DateTime exchangeMinuteStart)
    {
        lock (_lock)
        {
            QuoteBar completedBar = null;

            if (_currentMinuteStart.HasValue && exchangeMinuteStart != _currentMinuteStart.Value)
            {
                // Minute rolled over — finalize the previous bucket unless already flushed by timer
                if (!_currentMinuteEmitted)
                {
                    completedBar = BuildCurrentBar();
                }
                StartNewBucket(exchangeMinuteStart);
            }
            else if (!_currentMinuteStart.HasValue)
            {
                StartNewBucket(exchangeMinuteStart);
            }

            // Update bid OHLC
            if (!_bidOpen.HasValue)
            {
                _bidOpen = bidPrice;
                _bidHigh = bidPrice;
                _bidLow = bidPrice;
            }
            else
            {
                if (bidPrice > _bidHigh.Value) _bidHigh = bidPrice;
                if (bidPrice < _bidLow.Value) _bidLow = bidPrice;
            }
            _bidClose = bidPrice;
            _lastBidSize = bidSize;

            // Update ask OHLC
            if (!_askOpen.HasValue)
            {
                _askOpen = askPrice;
                _askHigh = askPrice;
                _askLow = askPrice;
            }
            else
            {
                if (askPrice > _askHigh.Value) _askHigh = askPrice;
                if (askPrice < _askLow.Value) _askLow = askPrice;
            }
            _askClose = askPrice;
            _lastAskSize = askSize;

            return completedBar;
        }
    }

    /// <summary>
    /// Force-emit the current bucket. Used by the timer-based early flush.
    /// Returns null if the bucket is empty or has already been emitted.
    /// </summary>
    public QuoteBar FlushCurrentMinute()
    {
        lock (_lock)
        {
            if (!_currentMinuteStart.HasValue || _currentMinuteEmitted || !_bidOpen.HasValue)
            {
                return null;
            }

            var bar = BuildCurrentBar();
            // Mark as emitted so the quote-driven rollover doesn't double-emit
            _currentMinuteEmitted = true;
            return bar;
        }
    }

    /// <summary>
    /// Clear all state. Used after reconnect to avoid mixing pre/post-outage data.
    /// </summary>
    public void Reset()
    {
        lock (_lock)
        {
            _currentMinuteStart = null;
            _currentMinuteEmitted = false;
            ClearOhlc();
        }
    }

    private QuoteBar BuildCurrentBar()
    {
        if (!_bidOpen.HasValue || !_askOpen.HasValue || !_currentMinuteStart.HasValue)
        {
            return null;
        }

        var bar = new QuoteBar
        {
            Symbol = _symbol,
            Time = _currentMinuteStart.Value,
            EndTime = _currentMinuteStart.Value + MinutePeriod,
            Period = MinutePeriod,
            Bid = new Bar(_bidOpen.Value, _bidHigh.Value, _bidLow.Value, _bidClose.Value),
            Ask = new Bar(_askOpen.Value, _askHigh.Value, _askLow.Value, _askClose.Value),
            LastBidSize = _lastBidSize,
            LastAskSize = _lastAskSize,
        };

        return bar;
    }

    private void StartNewBucket(DateTime minuteStart)
    {
        _currentMinuteStart = minuteStart;
        _currentMinuteEmitted = false;
        ClearOhlc();
    }

    private void ClearOhlc()
    {
        _bidOpen = _bidHigh = _bidLow = _bidClose = null;
        _askOpen = _askHigh = _askLow = _askClose = null;
        _lastBidSize = 0;
        _lastAskSize = 0;
    }
}
