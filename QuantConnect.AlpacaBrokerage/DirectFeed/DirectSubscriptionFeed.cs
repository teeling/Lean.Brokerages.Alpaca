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
using QuantConnect.Data;

namespace QuantConnect.Brokerages.Alpaca.DirectFeed;

/// <summary>
/// Represents a single LEAN subscription that receives data directly from the plugin
/// rather than through the aggregator pipeline.
/// </summary>
internal sealed class DirectSubscriptionFeed
{
    /// <summary>
    /// The LEAN subscription configuration this feed serves.
    /// </summary>
    public SubscriptionDataConfig Config { get; }

    /// <summary>
    /// The queue where finished bars are placed for LEAN to consume.
    /// </summary>
    public ConcurrentQueue<BaseData> Queue { get; } = new();

    /// <summary>
    /// The handler LEAN provided to be notified when new data is available.
    /// </summary>
    public EventHandler NewDataAvailableHandler { get; }

    /// <summary>
    /// The enumerator LEAN will call MoveNext() on to consume data.
    /// </summary>
    public ConcurrentQueueEnumerator Enumerator { get; }

    public DirectSubscriptionFeed(SubscriptionDataConfig config, EventHandler newDataAvailableHandler)
    {
        Config = config;
        NewDataAvailableHandler = newDataAvailableHandler;
        Enumerator = new ConcurrentQueueEnumerator(Queue);
    }
}
