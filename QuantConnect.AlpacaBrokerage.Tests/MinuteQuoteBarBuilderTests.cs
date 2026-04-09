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
using NUnit.Framework;
using QuantConnect.Tests;
using QuantConnect.Brokerages.Alpaca.DirectFeed;

namespace QuantConnect.Brokerages.Alpaca.Tests
{
    [TestFixture]
    public class MinuteQuoteBarBuilderTests
    {
        private MinuteQuoteBarBuilder _builder;
        private readonly Symbol _symbol = Symbols.SPY;
        private readonly DateTime _minute1 = new DateTime(2026, 3, 27, 10, 31, 0);
        private readonly DateTime _minute2 = new DateTime(2026, 3, 27, 10, 32, 0);

        [SetUp]
        public void SetUp()
        {
            _builder = new MinuteQuoteBarBuilder(_symbol);
        }

        [Test]
        public void FirstQuote_ReturnsNull()
        {
            var result = _builder.Update(100m, 10m, 101m, 5m, _minute1);
            Assert.IsNull(result);
        }

        [Test]
        public void SameMinute_ReturnsNull()
        {
            _builder.Update(100m, 10m, 101m, 5m, _minute1);
            var result = _builder.Update(100.5m, 10m, 101.5m, 5m, _minute1);
            Assert.IsNull(result);
        }

        [Test]
        public void MinuteRollover_ReturnsCompletedBar()
        {
            // Feed quotes into minute 1
            _builder.Update(100m, 10m, 101m, 5m, _minute1);
            _builder.Update(102m, 20m, 103m, 15m, _minute1);
            _builder.Update(99m, 30m, 100m, 25m, _minute1);
            _builder.Update(101m, 40m, 102m, 35m, _minute1);

            // Trigger rollover with first quote in minute 2
            var bar = _builder.Update(105m, 50m, 106m, 45m, _minute2);

            Assert.IsNotNull(bar);
            Assert.AreEqual(_symbol, bar.Symbol);
            Assert.AreEqual(_minute1, bar.Time);
            Assert.AreEqual(_minute1.AddMinutes(1), bar.EndTime);
            Assert.AreEqual(TimeSpan.FromMinutes(1), bar.Period);

            // Bid OHLC: open=100, high=102, low=99, close=101
            Assert.AreEqual(100m, bar.Bid.Open);
            Assert.AreEqual(102m, bar.Bid.High);
            Assert.AreEqual(99m, bar.Bid.Low);
            Assert.AreEqual(101m, bar.Bid.Close);

            // Ask OHLC: open=101, high=103, low=100, close=102
            Assert.AreEqual(101m, bar.Ask.Open);
            Assert.AreEqual(103m, bar.Ask.High);
            Assert.AreEqual(100m, bar.Ask.Low);
            Assert.AreEqual(102m, bar.Ask.Close);

            // Last sizes
            Assert.AreEqual(40m, bar.LastBidSize);
            Assert.AreEqual(35m, bar.LastAskSize);
        }

        [Test]
        public void FlushCurrentMinute_EmitsCurrentBucket()
        {
            _builder.Update(100m, 10m, 101m, 5m, _minute1);
            _builder.Update(102m, 20m, 103m, 15m, _minute1);

            var bar = _builder.FlushCurrentMinute();

            Assert.IsNotNull(bar);
            Assert.AreEqual(_minute1, bar.Time);
            Assert.AreEqual(100m, bar.Bid.Open);
            Assert.AreEqual(102m, bar.Bid.Close);
        }

        [Test]
        public void FlushCurrentMinute_ReturnsNull_WhenEmpty()
        {
            var bar = _builder.FlushCurrentMinute();
            Assert.IsNull(bar);
        }

        [Test]
        public void FlushCurrentMinute_ReturnsNull_WhenAlreadyFlushed()
        {
            _builder.Update(100m, 10m, 101m, 5m, _minute1);

            var first = _builder.FlushCurrentMinute();
            Assert.IsNotNull(first);

            // Second flush of same minute should return null (no double-emit)
            var second = _builder.FlushCurrentMinute();
            Assert.IsNull(second);
        }

        [Test]
        public void QuoteRollover_AfterFlush_ReturnsNull()
        {
            // Flush minute 1 via timer
            _builder.Update(100m, 10m, 101m, 5m, _minute1);
            var flushed = _builder.FlushCurrentMinute();
            Assert.IsNotNull(flushed);

            // Quote arrives in minute 2 — should NOT re-emit minute 1
            var bar = _builder.Update(105m, 50m, 106m, 45m, _minute2);
            Assert.IsNull(bar, "Should not double-emit minute 1 after timer flush");
        }

        [Test]
        public void Reset_ClearsAllState()
        {
            _builder.Update(100m, 10m, 101m, 5m, _minute1);

            _builder.Reset();

            // After reset, flush should return null
            var bar = _builder.FlushCurrentMinute();
            Assert.IsNull(bar);

            // New quotes should start fresh
            var result = _builder.Update(200m, 10m, 201m, 5m, _minute2);
            Assert.IsNull(result, "First quote after reset should not produce a bar");
        }

        [Test]
        public void SingleQuote_ProducesCorrectOhlc()
        {
            _builder.Update(100m, 10m, 101m, 5m, _minute1);

            var bar = _builder.FlushCurrentMinute();

            // With only one quote, all OHLC values should be the same
            Assert.AreEqual(100m, bar.Bid.Open);
            Assert.AreEqual(100m, bar.Bid.High);
            Assert.AreEqual(100m, bar.Bid.Low);
            Assert.AreEqual(100m, bar.Bid.Close);

            Assert.AreEqual(101m, bar.Ask.Open);
            Assert.AreEqual(101m, bar.Ask.High);
            Assert.AreEqual(101m, bar.Ask.Low);
            Assert.AreEqual(101m, bar.Ask.Close);
        }

        [Test]
        public void BarTimestamps_AreCorrectFullMinute()
        {
            _builder.Update(100m, 10m, 101m, 5m, _minute1);

            var bar = _builder.FlushCurrentMinute();

            Assert.AreEqual(_minute1, bar.Time);
            Assert.AreEqual(_minute1 + TimeSpan.FromMinutes(1), bar.EndTime);
            Assert.AreEqual(TimeSpan.FromMinutes(1), bar.Period);
        }
    }
}
