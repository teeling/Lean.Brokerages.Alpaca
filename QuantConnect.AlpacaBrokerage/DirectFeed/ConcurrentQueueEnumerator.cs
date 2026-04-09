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

using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using QuantConnect.Data;

namespace QuantConnect.Brokerages.Alpaca.DirectFeed;

/// <summary>
/// An enumerator backed by a <see cref="ConcurrentQueue{T}"/> for delivering live data to LEAN.
/// <para>
/// CRITICAL: <see cref="MoveNext"/> always returns <c>true</c> unless the enumerator has been
/// disposed. LEAN's live pipeline wraps this in a <c>FrontierAwareEnumerator</c> that permanently
/// stops pulling data if <c>MoveNext()</c> returns <c>false</c>. The existing
/// <c>ScannableEnumerator</c> used by the aggregator path follows the same contract.
/// </para>
/// </summary>
internal sealed class ConcurrentQueueEnumerator : IEnumerator<BaseData>
{
    private readonly ConcurrentQueue<BaseData> _queue;
    private BaseData _current;
    private volatile bool _disposed;

    public ConcurrentQueueEnumerator(ConcurrentQueue<BaseData> queue)
    {
        _queue = queue;
    }

    public BaseData Current => _current;

    object IEnumerator.Current => _current;

    public bool MoveNext()
    {
        if (_disposed)
        {
            _current = null;
            return false;
        }

        if (!_queue.TryDequeue(out _current))
        {
            _current = null;
        }

        // Always return true — the live subscription is never "done"
        // until explicitly disposed. Returning false kills the subscription.
        return true;
    }

    public void Reset() { }

    public void Dispose()
    {
        _disposed = true;
    }
}
