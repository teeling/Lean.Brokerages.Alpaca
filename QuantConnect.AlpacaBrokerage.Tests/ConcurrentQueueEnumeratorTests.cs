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
using System.Threading.Tasks;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Tests;
using QuantConnect.Data.Market;
using QuantConnect.Brokerages.Alpaca.DirectFeed;

namespace QuantConnect.Brokerages.Alpaca.Tests
{
    [TestFixture]
    public class ConcurrentQueueEnumeratorTests
    {
        [Test]
        public void MoveNext_ReturnsTrue_WhenQueueIsEmpty()
        {
            var queue = new ConcurrentQueue<BaseData>();
            using var enumerator = new ConcurrentQueueEnumerator(queue);

            Assert.IsTrue(enumerator.MoveNext());
            Assert.IsNull(enumerator.Current);
        }

        [Test]
        public void MoveNext_ReturnsTrue_WhenQueueHasData()
        {
            var queue = new ConcurrentQueue<BaseData>();
            var bar = new TradeBar { Symbol = Symbols.SPY, Close = 100m };
            queue.Enqueue(bar);

            using var enumerator = new ConcurrentQueueEnumerator(queue);

            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual(bar, enumerator.Current);
        }

        [Test]
        public void MoveNext_AlwaysReturnsTrue_OnRepeatedEmptyCalls()
        {
            var queue = new ConcurrentQueue<BaseData>();
            using var enumerator = new ConcurrentQueueEnumerator(queue);

            for (var i = 0; i < 100; i++)
            {
                Assert.IsTrue(enumerator.MoveNext(), $"MoveNext returned false on iteration {i}");
                Assert.IsNull(enumerator.Current);
            }
        }

        [Test]
        public void MoveNext_ReturnsFalse_AfterDispose()
        {
            var queue = new ConcurrentQueue<BaseData>();
            var enumerator = new ConcurrentQueueEnumerator(queue);

            enumerator.Dispose();

            Assert.IsFalse(enumerator.MoveNext());
            Assert.IsNull(enumerator.Current);
        }

        [Test]
        public void MoveNext_IgnoresQueuedData_AfterDispose()
        {
            var queue = new ConcurrentQueue<BaseData>();
            var enumerator = new ConcurrentQueueEnumerator(queue);

            queue.Enqueue(new TradeBar { Symbol = Symbols.SPY, Close = 100m });
            enumerator.Dispose();

            Assert.IsFalse(enumerator.MoveNext());
            Assert.IsNull(enumerator.Current);
        }

        [Test]
        public void MultipleEnqueueDequeue_WorksCorrectly()
        {
            var queue = new ConcurrentQueue<BaseData>();
            using var enumerator = new ConcurrentQueueEnumerator(queue);

            // Enqueue 3 items
            for (var i = 0; i < 3; i++)
            {
                queue.Enqueue(new TradeBar { Symbol = Symbols.SPY, Close = i });
            }

            // Dequeue all 3
            for (var i = 0; i < 3; i++)
            {
                Assert.IsTrue(enumerator.MoveNext());
                Assert.IsNotNull(enumerator.Current);
                Assert.AreEqual(i, enumerator.Current.Value);
            }

            // Next call should return true with null Current
            Assert.IsTrue(enumerator.MoveNext());
            Assert.IsNull(enumerator.Current);
        }

        [Test]
        public void ConcurrentEnqueueAndDequeue_IsThreadSafe()
        {
            var queue = new ConcurrentQueue<BaseData>();
            using var enumerator = new ConcurrentQueueEnumerator(queue);
            var itemCount = 10000;
            var dequeued = 0;

            // Producer thread
            var producer = Task.Run(() =>
            {
                for (var i = 0; i < itemCount; i++)
                {
                    queue.Enqueue(new TradeBar { Symbol = Symbols.SPY, Close = i });
                }
            });

            // Consumer thread
            var consumer = Task.Run(() =>
            {
                while (Volatile.Read(ref dequeued) < itemCount)
                {
                    if (enumerator.MoveNext() && enumerator.Current != null)
                    {
                        Interlocked.Increment(ref dequeued);
                    }
                }
            });

            Assert.IsTrue(Task.WaitAll(new[] { producer, consumer }, TimeSpan.FromSeconds(10)),
                "Concurrent test timed out");
            Assert.AreEqual(itemCount, dequeued);
        }
    }
}
