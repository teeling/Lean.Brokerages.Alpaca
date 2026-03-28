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
using QuantConnect.Data;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Interfaces;
using System.Collections.Generic;
using QuantConnect.Brokerages.Alpaca.DirectFeed;

namespace QuantConnect.Brokerages.Alpaca;

public partial class AlpacaBrokerage : IDataQueueHandler
{
    /// <summary>
    /// Subscribe to the specified configuration
    /// </summary>
    /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
    /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
    /// <returns>The new enumerator for this subscription request</returns>
    public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
    {
        if (!CanSubscribe(dataConfig.Symbol))
        {
            return null;
        }

        if (UseDirectBarPublishing(dataConfig))
        {
            var key = dataConfig.ToKey();

            // Guard against double-subscribe: dispose old feed if it exists
            if (_directFeeds.TryRemove(key, out var oldFeed))
            {
                oldFeed.Enumerator.Dispose();
                RemoveUpstreamInterest(dataConfig);
                Log.Trace($"AlpacaBrokerage.Subscribe(): replacing existing DIRECT FEED for {dataConfig.Symbol.Value} {dataConfig.Resolution} {dataConfig.TickType}");
            }

            var feed = new DirectSubscriptionFeed(dataConfig, newDataAvailableHandler);
            _directFeeds[key] = feed;

            // Upstream Alpaca channels managed directly — NOT via _subscriptionManager
            RegisterUpstreamInterest(dataConfig);

            Log.Trace($"AlpacaBrokerage.Subscribe(): DIRECT FEED enabled for {dataConfig.Symbol.Value} {dataConfig.Resolution} {dataConfig.TickType}");
            return feed.Enumerator;
        }

        // Legacy path — unchanged
        var enumerator = _aggregator.Add(dataConfig, newDataAvailableHandler);
        _subscriptionManager.Subscribe(dataConfig);

        Log.Trace($"AlpacaBrokerage.Subscribe(): LEGACY AGGREGATOR path for {dataConfig.Symbol.Value} {dataConfig.Resolution} {dataConfig.TickType}");
        return enumerator;
    }

    /// <summary>
    /// Removes the specified configuration
    /// </summary>
    /// <param name="dataConfig">Subscription config to be removed</param>
    public void Unsubscribe(SubscriptionDataConfig dataConfig)
    {
        var key = dataConfig.ToKey();
        if (_directFeeds.TryRemove(key, out var feed))
        {
            // Direct-feed path
            feed.Enumerator.Dispose();
            RemoveUpstreamInterest(dataConfig);
            // Do NOT call _aggregator.Remove — we never registered with it
            return;
        }

        // Legacy path
        _subscriptionManager.Unsubscribe(dataConfig);
        _aggregator.Remove(dataConfig);
    }

    /// <summary>
    /// Sets the job we're subscribing for
    /// </summary>
    /// <param name="job">Job we're subscribing for</param>
    public void SetJob(LiveNodePacket job)
    {
        // used for data
        job.BrokerageData.TryGetValue("alpaca-api-key", out var apiKey);
        job.BrokerageData.TryGetValue("alpaca-api-secret", out var secretKey);

        // required for trading
        job.BrokerageData.TryGetValue("alpaca-access-token", out var accessToken);

        var usePaperTrading = false;
        // might not be there if only used as a data source
        if (job.BrokerageData.TryGetValue("alpaca-paper-trading", out var usePaper))
        {
            usePaperTrading = Convert.ToBoolean(usePaper);
        }

        Initialize(apiKey, secretKey, accessToken, usePaperTrading, null, null);
        if (!IsConnected)
        {
            Connect();
        }
    }
}
