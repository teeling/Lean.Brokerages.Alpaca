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
using Alpaca.Markets;
using System.Threading;
using QuantConnect.Util;
using System.Threading.Tasks;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Helper class for alpaca streaming clients, handling paid/free subscriptions
    /// </summary>
    public class AlpacaStreamingClientWrapper : IStreamingClient
    {
        private readonly SecurityKey _securityKey;
        private readonly SecurityType _securityType;
        private readonly string _customStreamingUrl;
        private IEnvironment[] _environments = new[] { Environments.Live, Environments.Paper };

        public IStreamingDataClient StreamingClient { get; set; }

        public bool IsOpenAndAuthorized { get; private set; }
        public event Action<AuthStatus> Connected;
        public event Action SocketOpened;
        public event Action SocketClosed;
        public event Action<Exception> OnError;
        public event Action<string> OnWarning;

        public event Action<string> EnviromentFailure;

        /// <summary>
        /// Creates a new instance using the target security key and security type
        /// </summary>
        /// <param name="securityKey">The security key for authentication</param>
        /// <param name="securityType">The security type (Equity, Crypto, Option)</param>
        /// <param name="customStreamingUrl">Optional custom WebSocket URL (e.g., for proxy support)</param>
        public AlpacaStreamingClientWrapper(SecurityKey securityKey, SecurityType securityType, string customStreamingUrl = null)
        {
            _securityKey = securityKey;
            _securityType = securityType;
            _customStreamingUrl = customStreamingUrl;

            if (!string.IsNullOrEmpty(_customStreamingUrl))
            {
                Logging.Log.Trace($"AlpacaStreamingClientWrapper: Using custom streaming URL: {_customStreamingUrl}");
            }
        }

        public async Task<AuthStatus> ConnectAndAuthenticateAsync(CancellationToken cancellationToken = default)
        {
            var result = AuthStatus.Unauthorized;

            var failureMessage = "";
            // we first try with live environment which uses paid subscriptions, if it fails try free paper environment
            foreach (var environment in _environments)
            {
                if (StreamingClient != null)
                {
                    StreamingClient.Connected -= HandleConnected;
                    StreamingClient.OnWarning -= OnWarning;
                    StreamingClient.SocketOpened -= SocketOpened;
                    StreamingClient.SocketClosed -= HandleSocketClosed;
                    StreamingClient.OnError -= OnError;
                    StreamingClient.DisposeSafely();
                }

                // If custom URL is provided, use it instead of default Alpaca endpoints
                if (!string.IsNullOrEmpty(_customStreamingUrl))
                {
                    Logging.Log.Trace($"AlpacaStreamingClientWrapper.ConnectAndAuthenticateAsync({_securityType}): using custom URL");

                    // Get default configuration for the environment, then override the WebSocket URL
                    if (_securityType == SecurityType.Crypto)
                    {
                        var config = EnvironmentExtensions.GetAlpacaCryptoStreamingClientConfiguration(environment, _securityKey);
                        config.WebSocketUrl = new Uri(_customStreamingUrl);
                        StreamingClient = config.GetClient();
                    }
                    else if (_securityType == SecurityType.Equity)
                    {
                        var config = EnvironmentExtensions.GetAlpacaDataStreamingClientConfiguration(environment, _securityKey);
                        config.WebSocketUrl = new Uri(_customStreamingUrl);
                        StreamingClient = config.GetClient();
                    }
                    else if (_securityType.IsOption())
                    {
                        var config = EnvironmentExtensions.GetAlpacaOptionsStreamingClientConfiguration(environment, _securityKey);
                        config.WebSocketUrl = new Uri(_customStreamingUrl);
                        StreamingClient = config.GetClient();
                    }
                    else
                    {
                        throw new NotImplementedException();
                    }
                    
                    // Skip environment retry loop when using custom URL
                    _environments = new[] { environment };
                }
                else
                {
                    // Use default Alpaca endpoints (original code)
                    var feedType = environment == Environments.Live ? "paid" : "free";
                    Logging.Log.Trace($"AlpacaStreamingClientWrapper.ConnectAndAuthenticateAsync({_securityType}): try connecting {feedType} feed");
                    if (_securityType == SecurityType.Crypto)
                    {
                        StreamingClient = EnvironmentExtensions.GetAlpacaCryptoStreamingClient(environment, _securityKey);
                    }
                    else if (_securityType == SecurityType.Equity)
                    {
                        var feed = $"'{MarketDataFeed.Iex}'";
                        if (environment == Environments.Live)
                        {
                            feed = $"'{MarketDataFeed.Sip}', will retry with free feed";
                        }
                        failureMessage = $"{_securityType} failed to connect to live feed {feed}";
                        StreamingClient = EnvironmentExtensions.GetAlpacaDataStreamingClient(environment, _securityKey);
                    }
                    else if (_securityType.IsOption())
                    {
                        var feed = $"'{OptionsFeed.Indicative}'";
                        if (environment == Environments.Live)
                        {
                            feed = $"'{OptionsFeed.Opra}', will retry with free feed";
                        }
                        failureMessage = $"{_securityType} failed to connect to live feed {feed}";
                        StreamingClient = EnvironmentExtensions.GetAlpacaOptionsStreamingClient(environment, _securityKey);
                    }
                    else
                    {
                        throw new NotImplementedException();
                    }
                }

                StreamingClient.Connected += HandleConnected;
                StreamingClient.OnWarning += OnWarning;
                StreamingClient.SocketOpened += SocketOpened;
                StreamingClient.SocketClosed += HandleSocketClosed;
                StreamingClient.OnError += OnError;

                result = await StreamingClient.ConnectAndAuthenticateAsync();
                if (result == AuthStatus.Authorized)
                {
                    // once connected we will just keep this environment, if we need to reconnect, due to internet issues, we don't want to retry all
                    _environments = new[] { environment };
                    Logging.Log.Trace($"AlpacaStreamingClientWrapper.ConnectAndAuthenticateAsync({_securityType}): connection succeeded");
                    // we got what we wanted
                    break;
                }
                else
                {
                    if (!string.IsNullOrEmpty(failureMessage))
                    {
                        EnviromentFailure?.Invoke(failureMessage);
                    }
                }
            }
            return result;
        }

        public Task DisconnectAsync(CancellationToken cancellationToken = default)
        {
            return StreamingClient?.DisconnectAsync();
        }

        public void Dispose()
        {
            StreamingClient.DisposeSafely();
        }

        public Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        private void HandleSocketClosed()
        {
            IsOpenAndAuthorized = false;
            SocketClosed?.Invoke();
        }

        private void HandleConnected(AuthStatus authStatus)
        {
            IsOpenAndAuthorized = authStatus == AuthStatus.Authorized;
            Connected?.Invoke(authStatus);
        }
    }
}
