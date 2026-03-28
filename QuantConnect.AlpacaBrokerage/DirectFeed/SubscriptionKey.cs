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

using QuantConnect.Data;

namespace QuantConnect.Brokerages.Alpaca.DirectFeed;

/// <summary>
/// A normalized key for looking up direct subscription feeds, avoiding dependence on
/// <see cref="SubscriptionDataConfig"/> reference equality.
/// </summary>
internal readonly record struct SubscriptionKey(Symbol Symbol, Resolution Resolution, TickType TickType);

internal static class SubscriptionKeyExtensions
{
    public static SubscriptionKey ToKey(this SubscriptionDataConfig config)
        => new(config.Symbol, config.Resolution, config.TickType);
}
