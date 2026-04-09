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
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Tests;
using QuantConnect.Data.Market;
using QuantConnect.Brokerages.Alpaca.DirectFeed;

namespace QuantConnect.Brokerages.Alpaca.Tests
{
    [TestFixture]
    public class DirectFeedSubscriptionTests
    {
        [Test]
        public void DirectSubscriptionFeed_CreatesWorkingEnumerator()
        {
            EventHandler handler = (s, e) => { };
            var config = CreateConfig(Symbols.SPY, Resolution.Minute, TickType.Trade);
            var feed = new DirectSubscriptionFeed(config, handler);

            // Enumerator should be usable
            Assert.IsNotNull(feed.Enumerator);
            Assert.IsTrue(feed.Enumerator.MoveNext());
            Assert.IsNull(feed.Enumerator.Current);
        }

        [Test]
        public void DirectSubscriptionFeed_EnqueueAndConsume()
        {
            var handlerCalled = false;
            EventHandler handler = (s, e) => { handlerCalled = true; };
            var config = CreateConfig(Symbols.SPY, Resolution.Minute, TickType.Trade);
            var feed = new DirectSubscriptionFeed(config, handler);

            var bar = new TradeBar
            {
                Symbol = Symbols.SPY,
                Time = new DateTime(2026, 3, 27, 10, 31, 0),
                Close = 500m
            };

            feed.Queue.Enqueue(bar);
            feed.NewDataAvailableHandler?.Invoke(null, EventArgs.Empty);

            Assert.IsTrue(handlerCalled);
            Assert.IsTrue(feed.Enumerator.MoveNext());
            Assert.AreEqual(bar, feed.Enumerator.Current);

            // Queue should now be empty
            Assert.IsTrue(feed.Enumerator.MoveNext());
            Assert.IsNull(feed.Enumerator.Current);
        }

        [Test]
        public void SubscriptionKey_EqualityWorks()
        {
            var key1 = new SubscriptionKey(Symbols.SPY, Resolution.Minute, TickType.Trade);
            var key2 = new SubscriptionKey(Symbols.SPY, Resolution.Minute, TickType.Trade);
            var key3 = new SubscriptionKey(Symbols.SPY, Resolution.Minute, TickType.Quote);
            var key4 = new SubscriptionKey(Symbols.AAPL, Resolution.Minute, TickType.Trade);

            Assert.AreEqual(key1, key2);
            Assert.AreNotEqual(key1, key3);
            Assert.AreNotEqual(key1, key4);
        }

        [Test]
        public void SubscriptionKey_ToKey_FromConfig()
        {
            var config = CreateConfig(Symbols.SPY, Resolution.Minute, TickType.Trade);
            var key = config.ToKey();

            Assert.AreEqual(Symbols.SPY, key.Symbol);
            Assert.AreEqual(Resolution.Minute, key.Resolution);
            Assert.AreEqual(TickType.Trade, key.TickType);
        }

        [Test]
        public void SubscriptionKey_WorksAsDictionaryKey()
        {
            var dict = new ConcurrentDictionary<SubscriptionKey, string>();

            var key1 = new SubscriptionKey(Symbols.SPY, Resolution.Minute, TickType.Trade);
            var key2 = new SubscriptionKey(Symbols.SPY, Resolution.Minute, TickType.Quote);

            dict[key1] = "trade";
            dict[key2] = "quote";

            Assert.AreEqual(2, dict.Count);
            Assert.AreEqual("trade", dict[key1]);
            Assert.AreEqual("quote", dict[key2]);

            // Same key should overwrite
            var key3 = new SubscriptionKey(Symbols.SPY, Resolution.Minute, TickType.Trade);
            dict[key3] = "trade2";
            Assert.AreEqual(2, dict.Count);
            Assert.AreEqual("trade2", dict[key1]);
        }

        [Test]
        public void DirectSubscriptionFeed_DisposeStopsEnumerator()
        {
            var config = CreateConfig(Symbols.SPY, Resolution.Minute, TickType.Trade);
            var feed = new DirectSubscriptionFeed(config, null);

            feed.Enumerator.Dispose();

            Assert.IsFalse(feed.Enumerator.MoveNext());
        }

        private static SubscriptionDataConfig CreateConfig(Symbol symbol, Resolution resolution, TickType tickType)
        {
            return new SubscriptionDataConfig(
                typeof(TradeBar),
                symbol,
                resolution,
                TimeZones.NewYork,
                TimeZones.NewYork,
                false,
                false,
                false,
                tickType: tickType);
        }
    }
}
