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
using System.Linq;
using Alpaca.Markets;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Orders;
using QuantConnect.Logging;
using QuantConnect.Interfaces;
using QuantConnect.Securities;
using QuantConnect.Orders.Fees;
using System.Collections.Generic;
using AlpacaMarket = Alpaca.Markets;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using QuantConnect.Api;
using QuantConnect.Data.Market;
using System.IO;
using System.Net.NetworkInformation;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using QuantConnect.Configuration;
using QuantConnect.Brokerages.CrossZero;
using System.Collections.Concurrent;
using System.Threading;
using System.Runtime.CompilerServices;
using System.Net.Http;

[assembly: InternalsVisibleTo("QuantConnect.Brokerages.Alpaca.Tests")]

namespace QuantConnect.Brokerages.Alpaca
{
    [BrokerageFactory(typeof(AlpacaBrokerageFactory))]
    public partial class AlpacaBrokerage : Brokerage
    {
        private IDataAggregator _aggregator;

        private IOrderProvider _orderProvider;
        private ISecurityProvider _securityProvider;

        private EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager;

        private ConcurrentDictionary<int, decimal> _orderIdToFillQuantity = new();
        private BrokerageConcurrentMessageHandler<ITradeUpdate> _messageHandler;

        /// <summary>
        /// Provisional mapping of Alpaca brokerage order ID → LEAN Order for bracket legs.
        /// Bracket legs are created via OnNewBrokerageOrderNotification which is processed
        /// asynchronously by LEAN's engine. WebSocket trade updates may arrive before the
        /// engine registers the order, so we store the order here for immediate lookup.
        /// Follows the same pattern as LeanOrderByZeroCrossBrokerageOrderId for cross-zero orders.
        /// </summary>
        private readonly ConcurrentDictionary<string, Order> _provisionalBracketLegOrders = new();

        /// <summary>
        /// Stores pending PATCH requests for bracket legs rejected by Alpaca with "pending_new".
        /// Keyed by the original Alpaca order ID (Guid). The retry is replayed in
        /// HandleTradeUpdate when TradeEvent.New arrives for that order.
        /// </summary>
        private readonly ConcurrentDictionary<Guid, ChangeOrderRequest> _pendingLegReplaces = new();

        private AlpacaBrokerageSymbolMapper _symbolMapper;

        private IAlpacaTradingClient _tradingClient;

        private IAlpacaDataClient _equityHistoricalDataClient;
        private IAlpacaCryptoDataClient _cryptoHistoricalDataClient;
        private IAlpacaOptionsDataClient _optionsHistoricalDataClient;

        private IAlpacaStreamingClient _orderStreamingClient;
        private AlpacaStreamingClientWrapper _equityStreamingClient;
        private AlpacaStreamingClientWrapper _optionsStreamingClient;
        private AlpacaStreamingClientWrapper _cryptoStreamingClient;

        private bool _isInitialized;
        private bool _connected;

        private readonly ManualResetEvent _reconnectionResetEvent = new(false);
        private readonly CancellationTokenSource _cancellationTokenSource = new();

        /// <summary>
        /// Provides user-facing reason messages for specific trade events.
        /// Used when emitting order events.
        /// </summary>
        private static readonly Dictionary<TradeEvent, string> _tradeEventReason = new()
        {
            { TradeEvent.Expired,  "The order was canceled by the brokerage." }
        };

        /// <summary>
        /// Maps each brokerage order ID to a set of execution IDs, used to detect and skip duplicate trade updates.
        /// </summary>
        internal readonly ConcurrentDictionary<Guid, HashSet<Guid>> _duplicationExecutionOrderIdByBrokerageOrderId = new();

        /// <summary>
        /// Tracks unsupported TimeInForce values detected during order conversion.
        /// This allows the system to issue a warning for each unsupported TIF only once,
        /// preventing duplicate messages when processing multiple orders.
        /// </summary>
        private readonly HashSet<AlpacaMarket.TimeInForce> _unsupportedTimeInForce = [];

        // --- Bracket order support ---

        /// <summary>
        /// Reconciliation timer — polls Alpaca REST every 60s to detect and correct
        /// state divergence from missed WebSocket events. Live only.
        /// </summary>
        private Timer _reconciliationTimer;

        /// <summary>
        /// Maps Alpaca leg order GUIDs to bracket leg info for routing trade updates
        /// to the correct LEAN orders and managing leg lifecycle.
        /// </summary>
        private readonly ConcurrentDictionary<Guid, BracketLegInfo> _bracketLegMapping = new();

        /// <summary>
        /// Reference to the BracketOrderManager for leg registration.
        /// Set by the manager during initialization via RegisterManager().
        /// </summary>
        private BracketOrderManager _bracketManager;

        /// <summary>
        /// Called by <see cref="BracketOrderManager"/> to register itself with this
        /// brokerage, enabling leg ticket registration when exit legs are created.
        /// </summary>
        /// <param name="manager">The BracketOrderManager instance.</param>
        public void RegisterManager(BracketOrderManager manager)
        {
            _bracketManager = manager;
            manager.RegisterBrokerage(this);
            Log.Debug($"{nameof(AlpacaBrokerage)}.RegisterManager: BracketOrderManager registered.");
        }

        /// <summary>
        /// Internal forwarding method for BracketOrderManager to emit brokerage messages.
        /// OnMessage is protected on the Brokerage base class, so external callers need this.
        /// </summary>
        internal void EmitBrokerageMessage(BrokerageMessageEvent messageEvent)
        {
            OnMessage(messageEvent);
        }

        /// <summary>
        /// Returns true if we're currently connected to the broker
        /// </summary>
        public override bool IsConnected => _connected;

        /// <summary>
        /// Enables concurrent order requests processing
        /// </summary>
        public override bool ConcurrencyEnabled => true;

        /// <summary>
        /// Parameterless constructor for brokerage
        /// </summary>
        public AlpacaBrokerage() : base("Alpaca")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AlpacaBrokerage"/> class.
        /// </summary>
        /// <param name="apiKey">The API key for authentication with Alpaca.</param>
        /// <param name="apiKeySecret">The secret key for authentication with Alpaca.</param>
        /// <param name="isPaperTrading">Indicates whether the brokerage should use the paper trading environment.</param>
        /// <remarks>
        /// This constructor initializes a new instance of the <see cref="AlpacaBrokerage"/> class with the specified API key,
        /// API secret key, and a flag indicating whether to use paper trading. It also retrieves an instance of <see cref="IDataAggregator"/>
        /// from the <see cref="Composer"/>. This constructor is required for brokerages implementing <see cref="IDataQueueHandler"/>.
        /// </remarks>
        public AlpacaBrokerage(string apiKey, string apiKeySecret, string accessToken, bool isPaperTrading, IAlgorithm algorithm)
            : this(apiKey, apiKeySecret, accessToken, isPaperTrading, algorithm?.Portfolio?.Transactions, algorithm?.Portfolio)
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="AlpacaBrokerage"/> class.
        /// </summary>
        /// <param name="apiKey">The API key for authentication with Alpaca.</param>
        /// <param name="apiKeySecret">The secret key for authentication with Alpaca.</param>
        /// <param name="isPaperTrading">Indicates whether the brokerage should use the paper trading environment.</param>
        /// <param name="securityProvider">The type capable of fetching the holdings for the specified symbol</param>
        /// <remarks>
        /// This constructor initializes a new instance of the <see cref="AlpacaBrokerage"/> class with the specified API key,
        /// API secret key, a flag indicating whether to use paper trading, and an instance of <see cref="IDataAggregator"/>.
        /// </remarks>
        public AlpacaBrokerage(string apiKey, string apiKeySecret, string accessToken, bool isPaperTrading, IOrderProvider orderProvider, ISecurityProvider securityProvider) : base("Alpaca")
        {
            Initialize(apiKey, apiKeySecret, accessToken, isPaperTrading, orderProvider, securityProvider);
        }

        /// <summary>
        /// Initializes this instance
        /// </summary>
        private void Initialize(string apiKey, string apiKeySecret, string accessToken, bool isPaperTrading, IOrderProvider orderProvider, ISecurityProvider securityProvider)
        {
            if (_isInitialized)
            {
                return;
            }
            _isInitialized = true;
            ValidateSubscription();

            SecurityKey tradingSecretKey = null;
            if (!string.IsNullOrEmpty(accessToken))
            {
                tradingSecretKey = new OAuthKey(accessToken);
            }
            SecretKey secretKey = null;
            if (!string.IsNullOrEmpty(apiKeySecret))
            {
                secretKey = new SecretKey(apiKey, apiKeySecret);
            }

            if (secretKey == null && tradingSecretKey == null)
            {
                // shouldn't happen
                throw new ArgumentException("No valid Alpaca brokerage credentials were provided!");
            }

            _orderProvider = orderProvider;
            _securityProvider = securityProvider;

            var environment = isPaperTrading ? Environments.Paper : Environments.Live;
            // trading api client
            _tradingClient = EnvironmentExtensions.GetAlpacaTradingClient(environment, tradingSecretKey ?? secretKey);
            // order updates
            _orderStreamingClient = EnvironmentExtensions.GetAlpacaStreamingClient(environment, tradingSecretKey ?? secretKey);

            // if we are used as a data queue handler ignore order updates
            if (_orderProvider != null)
            {
                _orderStreamingClient.OnTradeUpdate += (message) => _messageHandler.HandleNewMessage(message);
                WireStreamingClientEvents(_orderStreamingClient);
            }
            _messageHandler = new(HandleTradeUpdate, ConcurrencyEnabled);
            _symbolMapper = new AlpacaBrokerageSymbolMapper(_tradingClient);

            // historical equity
            _equityHistoricalDataClient = EnvironmentExtensions.GetAlpacaDataClient(environment, tradingSecretKey ?? secretKey);

            // historical options
            _optionsHistoricalDataClient = EnvironmentExtensions.GetAlpacaOptionsDataClient(environment, tradingSecretKey ?? secretKey);

            // historical crypto
            _cryptoHistoricalDataClient = EnvironmentExtensions.GetAlpacaCryptoDataClient(environment, tradingSecretKey ?? secretKey);

            if (secretKey != null)
            {
                // Read custom data streaming URL from configuration (for proxy support)
                var customDataStreamingUrl = Config.Get("alpaca-data-streaming-url", string.Empty);
                if (!string.IsNullOrEmpty(customDataStreamingUrl))
                {
                    Log.Trace($"AlpacaBrokerage.Initialize(): Using custom data streaming URL: {customDataStreamingUrl}");
                }

                // equity streaming client
                _equityStreamingClient = new AlpacaStreamingClientWrapper(secretKey, SecurityType.Equity, customDataStreamingUrl);

                // streaming crypto
                _cryptoStreamingClient = new AlpacaStreamingClientWrapper(secretKey, SecurityType.Crypto, customDataStreamingUrl);

                // streaming options
                _optionsStreamingClient = new AlpacaStreamingClientWrapper(secretKey, SecurityType.Option, customDataStreamingUrl);

                foreach (var streamingClient in new IStreamingClient[] { _cryptoStreamingClient, _optionsStreamingClient, _equityStreamingClient })
                {
                    WireStreamingClientEvents(streamingClient);
                }

                _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
                _subscriptionManager.SubscribeImpl += (s, t) => Subscribe(s);
                _subscriptionManager.UnsubscribeImpl += (s, t) => Unsubscribe(s);

                _aggregator = Composer.Instance.GetPart<IDataAggregator>();
                if (_aggregator == null)
                {
                    // toolbox downloader case
                    var aggregatorName = Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager");
                    Log.Trace($"AlpacaBrokerage.AlpacaBrokerage(): found no data aggregator instance, creating {aggregatorName}");
                    _aggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(aggregatorName);
                }
            }
            ReconnectionLogic();
        }

        private void WireStreamingClientEvents(IStreamingClient streamingClient)
        {
            streamingClient.Connected += (obj) => StreamingClient_Connected(streamingClient, obj);
            streamingClient.OnWarning += (obj) => StreamingClient_OnWarning(streamingClient, obj);
            streamingClient.SocketOpened += () => StreamingClient_SocketOpened(streamingClient);
            streamingClient.SocketClosed += () => StreamingClient_SocketClosed(streamingClient);
            streamingClient.OnError += (obj) => StreamingClient_OnError(streamingClient, obj);

            if (streamingClient is AlpacaStreamingClientWrapper wrapper)
            {
                wrapper.EnviromentFailure += (message) => Log.Trace($"AlpacaBrokerage.Initialize(): {message}");
            }
        }

        private void StreamingClient_OnError(IStreamingClient client, Exception obj)
        {
            Log.Trace($"{nameof(StreamingClient_OnError)}({client.GetStreamingClientName()}): {obj}");
        }

        private void StreamingClient_SocketClosed(IStreamingClient client)
        {
            Log.Trace($"{nameof(StreamingClient_SocketClosed)}({client.GetStreamingClientName()}): SocketClosed");
            if (_connected)
            {
                _connected = false;
                // let consumers know, we will try to reconnect internally, if we can't lean will kill us
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Disconnect, "Disconnected", "Brokerage Disconnected"));
                _reconnectionResetEvent.Set();
            }
        }

        private void StreamingClient_SocketOpened(IStreamingClient client)
        {
            Log.Trace($"{nameof(StreamingClient_SocketOpened)}({client.GetStreamingClientName()}): SocketOpened");
        }

        private void StreamingClient_OnWarning(IStreamingClient client, string obj)
        {
            Log.Trace($"{nameof(StreamingClient_OnWarning)}({client.GetStreamingClientName()}): {obj}");
        }

        private void StreamingClient_Connected(IStreamingClient client, AuthStatus obj)
        {
            Log.Trace($"{nameof(StreamingClient_Connected)}({client.GetStreamingClientName()}): {obj}");
        }

        #region Brokerage

        /// <summary>
        /// Gets all open orders on the account.
        /// NOTE: The order objects returned do not have QC order IDs.
        /// </summary>
        /// <returns>The open orders returned from IB</returns>
        public override List<Order> GetOpenOrders()
        {
            // RollUpNestedOrders groups bracket/OCO/OTO child orders under the parent's
            // Legs property, giving us the full parent→child structure needed to
            // reconstruct bracket groups after a restart or reconciliation pass.
            var orders = _tradingClient.ListOrdersAsync(new ListOrdersRequest()
            {
                OrderStatusFilter = OrderStatusFilter.Open,
                RollUpNestedOrders = true,
            }).SynchronouslyAwaitTaskResult();

            var leanOrders = new List<Order>();
            foreach (var brokerageOrder in orders)
            {
                if (!TryConvertToLeanOrder(brokerageOrder, out var leanOrder))
                {
                    continue;
                }

                leanOrders.Add(leanOrder);

                // Bracket parents have legs nested under them with RollUpNestedOrders.
                // We need to flatten them into separate top-level LEAN orders so LEAN's
                // reconciliation can track each leg independently. Also register the
                // leg mappings so HandleTradeUpdate can route events correctly.
                if (brokerageOrder.OrderClass == AlpacaMarket.OrderClass.Bracket
                    && brokerageOrder.Legs.Count > 0)
                {
                    // Use the Alpaca parent order ID as a recovery group ID.
                    // The original BracketOrderManager group ID is lost on restart
                    // (it's in-memory only), so we derive one from the parent.
                    var recoveryGroupId = $"recovery:{brokerageOrder.OrderId}";

                    Log.Debug($"{nameof(AlpacaBrokerage)}.GetOpenOrders: Bracket parent " +
                        $"AlpacaId={brokerageOrder.OrderId} has {brokerageOrder.Legs.Count} legs. " +
                        $"Flattening for reconciliation. RecoveryGroupId={recoveryGroupId}");

                    foreach (var leg in brokerageOrder.Legs)
                    {
                        if (!TryConvertToLeanOrder(leg, out var legLeanOrder))
                        {
                            Log.Error($"{nameof(AlpacaBrokerage)}.GetOpenOrders: Failed to convert bracket leg " +
                                $"AlpacaId={leg.OrderId} for parent {brokerageOrder.OrderId}");
                            continue;
                        }

                        // Determine leg type from Alpaca order type
                        var legType = leg.OrderType switch
                        {
                            AlpacaMarket.OrderType.Limit => BracketLegType.TakeProfit,
                            AlpacaMarket.OrderType.Stop => BracketLegType.StopLoss,
                            AlpacaMarket.OrderType.StopLimit => BracketLegType.StopLoss,
                            _ => (BracketLegType?)null
                        };

                        if (legType.HasValue)
                        {
                            // Re-convert with bracket properties so they're set via
                            // the constructor (Order.Properties has a private setter)
                            var bracketLegProps = new AlpacaBracketOrderProperties
                            {
                                BracketGroupId = recoveryGroupId,
                                LegType = legType.Value
                            };
                            if (!TryConvertToLeanOrder(leg, out legLeanOrder, bracketLegProps))
                            {
                                continue;
                            }

                            // Register in _bracketLegMapping so HandleTradeUpdate can
                            // route subsequent trade events for these legs correctly
                            _bracketLegMapping.TryAdd(leg.OrderId, new BracketLegInfo
                            {
                                BracketGroupId = recoveryGroupId,
                                AlpacaLegOrderId = leg.OrderId,
                                LegType = legType.Value,
                            });

                            Log.Debug($"{nameof(AlpacaBrokerage)}.GetOpenOrders: Flattened bracket leg " +
                                $"AlpacaId={leg.OrderId}, Type={legType}, " +
                                $"OrderType={leg.OrderType}, RecoveryGroupId={recoveryGroupId}");
                        }

                        leanOrders.Add(legLeanOrder);
                    }
                }

                // OCO parents also have legs nested under them.
                // Handle the same flattening for OCO orders so we recover
                // both legs on restart.
                if (brokerageOrder.OrderClass == AlpacaMarket.OrderClass.OneCancelsOther
                    && brokerageOrder.Legs.Count > 0)
                {
                    var recoveryGroupId = $"recovery-oco:{brokerageOrder.OrderId}";

                    Log.Debug($"{nameof(AlpacaBrokerage)}.GetOpenOrders: OCO parent " +
                        $"AlpacaId={brokerageOrder.OrderId} has {brokerageOrder.Legs.Count} legs. " +
                        $"Flattening for reconciliation. RecoveryGroupId={recoveryGroupId}");

                    foreach (var leg in brokerageOrder.Legs)
                    {
                        var legType = leg.OrderType switch
                        {
                            AlpacaMarket.OrderType.Limit => BracketLegType.TakeProfit,
                            AlpacaMarket.OrderType.Stop => BracketLegType.StopLoss,
                            AlpacaMarket.OrderType.StopLimit => BracketLegType.StopLoss,
                            _ => (BracketLegType?)null
                        };

                        if (legType.HasValue)
                        {
                            var ocoLegProps = new AlpacaBracketOrderProperties
                            {
                                BracketGroupId = recoveryGroupId,
                                LegType = legType.Value
                            };
                            if (!TryConvertToLeanOrder(leg, out var legLeanOrder, ocoLegProps))
                            {
                                continue;
                            }

                            _bracketLegMapping.TryAdd(leg.OrderId, new BracketLegInfo
                            {
                                BracketGroupId = recoveryGroupId,
                                AlpacaLegOrderId = leg.OrderId,
                                LegType = legType.Value,
                            });

                            Log.Debug($"{nameof(AlpacaBrokerage)}.GetOpenOrders: Flattened OCO leg " +
                                $"AlpacaId={leg.OrderId}, Type={legType}, " +
                                $"OrderType={leg.OrderType}, RecoveryGroupId={recoveryGroupId}");

                            leanOrders.Add(legLeanOrder);
                        }
                        else
                        {
                            if (!TryConvertToLeanOrder(leg, out var legLeanOrder))
                            {
                                continue;
                            }
                            leanOrders.Add(legLeanOrder);
                        }
                    }
                }
            }

            return leanOrders;
        }

        private bool TryConvertToLeanOrder(IOrder brokerageOrder, out Order leanOrder,
            AlpacaOrderProperties overrideProperties = null)
        {
            leanOrder = null;

            // Bracket parent orders have Legs pointing to child stop/target orders.
            // Convert the parent as its base order type (Market/Limit) — the bracket
            // legs are tracked separately by BracketOrderManager and the brokerage.
            // Only reject true multi-leg option orders (ComboMarket/ComboLimit).
            if (brokerageOrder.Legs.Count > 1
                && brokerageOrder.OrderClass != AlpacaMarket.OrderClass.Bracket
                && brokerageOrder.OrderClass != AlpacaMarket.OrderClass.OneCancelsOther
                && brokerageOrder.OrderClass != AlpacaMarket.OrderClass.OneTriggersOther)
            {
                // TODO: Implement OrderType.ComboMarket and OrderType.ComboLimit
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "NotSupportedOrderType", "Multi-leg orders are not currently supported."));
                return false;
            }

            var orderProperties = overrideProperties ?? new AlpacaOrderProperties();
            if (!orderProperties.TryGetLeanTimeInForceByAlpacaTimeInForce(brokerageOrder.TimeInForce))
            {
                if (_unsupportedTimeInForce.Add(brokerageOrder.TimeInForce))
                {
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, -1, $"Detected unsupported Lean TimeInForce of '{brokerageOrder.TimeInForce}', ignoring. Using default: TimeInForce.GoodTilCanceled"));
                }
            }

            var leanSymbol = _symbolMapper.GetLeanSymbol(brokerageOrder.AssetClass, brokerageOrder.Symbol);
            var quantity = (brokerageOrder.OrderSide == OrderSide.Buy ? brokerageOrder.Quantity : decimal.Negate(brokerageOrder.Quantity.Value)).Value;
            switch (brokerageOrder.OrderType)
            {
                case AlpacaMarket.OrderType.Market:

                    switch (brokerageOrder.TimeInForce)
                    {
                        case AlpacaMarket.TimeInForce.Opg:
                            leanOrder = new MarketOnOpenOrder(leanSymbol, quantity, brokerageOrder.SubmittedAtUtc.Value, properties: orderProperties);
                            break;
                        case AlpacaMarket.TimeInForce.Cls:
                            leanOrder = new MarketOnCloseOrder(leanSymbol, quantity, brokerageOrder.SubmittedAtUtc.Value, properties: orderProperties);
                            break;
                        default:
                            leanOrder = new Orders.MarketOrder(leanSymbol, quantity, brokerageOrder.SubmittedAtUtc.Value, properties: orderProperties);
                            break;
                    }
                    break;
                case AlpacaMarket.OrderType.Limit:
                    leanOrder = new Orders.LimitOrder(leanSymbol, quantity, brokerageOrder.LimitPrice.Value, brokerageOrder.SubmittedAtUtc.Value, properties: orderProperties);
                    break;
                case AlpacaMarket.OrderType.Stop:
                    leanOrder = new StopMarketOrder(leanSymbol, quantity, brokerageOrder.StopPrice.Value, brokerageOrder.SubmittedAtUtc.Value, properties: orderProperties);
                    break;
                case AlpacaMarket.OrderType.StopLimit:
                    leanOrder = new Orders.StopLimitOrder(leanSymbol, quantity, brokerageOrder.StopPrice.Value, brokerageOrder.LimitPrice.Value, brokerageOrder.SubmittedAtUtc.Value, properties: orderProperties);
                    break;
                case AlpacaMarket.OrderType.TrailingStop:
                    var trailingAsPercent = brokerageOrder.TrailOffsetInPercent.HasValue ? true : false;
                    var trailingAmount = brokerageOrder.TrailOffsetInPercent.HasValue ? brokerageOrder.TrailOffsetInPercent.Value / 100m : brokerageOrder.TrailOffsetInDollars.Value;
                    leanOrder = new Orders.TrailingStopOrder(leanSymbol, quantity, brokerageOrder.StopPrice.Value, trailingAmount, trailingAsPercent, brokerageOrder.SubmittedAtUtc.Value, properties: orderProperties);
                    break;
                default:
                    throw new NotSupportedException($"{nameof(AlpacaBrokerage)}.{nameof(GetOpenOrders)}: Order type '{brokerageOrder.OrderType}' is not supported.");
            }

            leanOrder.Status = Orders.OrderStatus.Submitted;
            if (brokerageOrder.FilledQuantity > 0 && brokerageOrder.FilledQuantity != brokerageOrder.Quantity)
            {
                leanOrder.Status = Orders.OrderStatus.PartiallyFilled;
            }

            var brokerageOrderId = brokerageOrder.OrderId;
            _duplicationExecutionOrderIdByBrokerageOrderId[brokerageOrderId] = [];
            leanOrder.BrokerId.Add(brokerageOrderId.ToString());

            return true;
        }

        /// <summary>
        /// Gets all holdings for the account
        /// </summary>
        /// <returns>The current holdings from the account</returns>
        public override List<Holding> GetAccountHoldings()
        {
            var positions = _tradingClient.ListPositionsAsync().SynchronouslyAwaitTaskResult();

            var holdings = new List<Holding>();
            foreach (var position in positions)
            {
                holdings.Add(new Holding()
                {
                    AveragePrice = position.AverageEntryPrice,
                    CurrencySymbol = Currencies.USD,
                    MarketValue = position.MarketValue ?? 0m,
                    MarketPrice = position.AssetCurrentPrice ?? 0m,
                    Quantity = position.Quantity,
                    Symbol = _symbolMapper.GetLeanSymbol(position.AssetClass, position.Symbol),
                    UnrealizedPnL = position.UnrealizedProfitLoss ?? 0m,
                    UnrealizedPnLPercent = position.UnrealizedProfitLossPercent ?? 0m,
                });
            }
            return holdings;
        }

        /// <summary>
        /// Gets the current cash balance for each currency held in the brokerage account
        /// </summary>
        /// <returns>The current cash balance for each currency available for trading</returns>
        public override List<CashAmount> GetCashBalance()
        {
            var accounts = _tradingClient.GetAccountAsync().SynchronouslyAwaitTaskResult();
            return new List<CashAmount>() { new(accounts.TradableCash, accounts.Currency) };
        }

        /// <summary>
        /// Places a new order and assigns a new broker ID to the order.
        /// Detects bracket orders (orders with <see cref="AlpacaBracketOrderProperties"/>)
        /// and routes them through the bracket-specific submission path which uses
        /// Alpaca's native bracket API.
        /// </summary>
        /// <param name="order">The order to be placed</param>
        /// <returns>True if the request for a new order has been placed, false otherwise</returns>
        public override bool PlaceOrder(Order order)
        {
            if (!CanSubscribe(order.Symbol))
            {
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, -1, $"Symbol is not supported {order.Symbol}"));
                return false;
            }

            try
            {
                _messageHandler.WithLockedStream(() =>
                {
                    // --- Check for bracket order ---
                    if (order.Properties is AlpacaBracketOrderProperties bracketProps && bracketProps.IsBracketOrder)
                    {
                        // Auto-register with the BracketOrderManager on first bracket order.
                        // IAlgorithm doesn't expose BrokerageInstance, so we use the order
                        // properties as a bridge for manager ↔ brokerage discovery.
                        if (_bracketManager == null && bracketProps.OriginatingManager != null)
                        {
                            RegisterManager(bracketProps.OriginatingManager);
                        }

                        Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceOrder: Detected bracket entry order. " +
                            $"OrderId={order.Id}, GroupId={bracketProps.BracketGroupId}, " +
                            $"Symbol={order.Symbol}, Qty={order.Quantity}, " +
                            $"Stop={bracketProps.StopLossStopPrice}, Target={bracketProps.TakeProfitLimitPrice}, " +
                            $"StopLimit={bracketProps.StopLossLimitPrice}");
                        PlaceBracketOrder(order, bracketProps);
                        return;
                    }

                    // --- Check for OCO order ---
                    if (order.Properties is AlpacaOcoOrderProperties ocoProps && ocoProps.IsOcoOrder)
                    {
                        if (_bracketManager == null && ocoProps.OriginatingManager != null)
                        {
                            RegisterManager(ocoProps.OriginatingManager);
                        }

                        Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceOrder: Detected OCO order. " +
                            $"OrderId={order.Id}, GroupId={ocoProps.OcoGroupId}, " +
                            $"Symbol={order.Symbol}, Qty={order.Quantity}, " +
                            $"StopPrice={ocoProps.StopLossStopPrice}, StopLimit={ocoProps.StopLossLimitPrice}");
                        PlaceOcoOrder(order, ocoProps);
                        return;
                    }

                    // --- Standard (non-bracket) order flow ---
                    var holdingQuantity = _securityProvider.GetHoldingsQuantity(order.Symbol);
                    var isPlaceCrossOrder = TryCrossZeroPositionOrder(order, holdingQuantity);
                    if (isPlaceCrossOrder == null)
                    {
                        var orderRequest = order.CreateAlpacaOrder(order.AbsoluteQuantity, _symbolMapper, order.Type);
                        var response = _tradingClient.PostOrderAsync(orderRequest).SynchronouslyAwaitTaskResult();
                        if (response == null || response.OrderStatus == AlpacaMarket.OrderStatus.Rejected)
                        {
                            OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, $"{nameof(AlpacaBrokerage)} Place Order Failed") { Status = Orders.OrderStatus.Invalid });
                            return;
                        }
                        order.BrokerId.Add(response.OrderId.ToString());

                        OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, $"{nameof(AlpacaBrokerage)} Order Event") { Status = Orders.OrderStatus.Submitted });
                    }
                });
            }
            catch (Exception ex)
            {
                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, ex.Message) { Status = Orders.OrderStatus.Invalid });
            }
            return true;
        }

        /// <summary>
        /// Places a bracket order using Alpaca's native bracket API.
        /// Creates all three orders (entry + stop-loss + take-profit) atomically
        /// in a single API call. The response contains the parent order and its legs,
        /// each with their own Alpaca order IDs.
        ///
        /// After successful submission:
        /// 1. Maps the entry order's Alpaca ID to the LEAN order
        /// 2. Creates phantom LEAN orders for each leg via OnNewBrokerageOrderNotification
        /// 3. Registers leg tickets with the BracketOrderManager
        /// </summary>
        private void PlaceBracketOrder(Order order, AlpacaBracketOrderProperties bracketProps)
        {
            try
            {
                // Step 1: Create the base entry order using existing extension method.
                // CreateAlpacaOrder returns OrderBase, but .Bracket() is defined on
                // SimpleOrderBase. Bracket entries must be Market or Limit orders,
                // both of which extend SimpleOrderBase, so the cast is safe.
                Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceBracketOrder: Creating base entry order for group {bracketProps.BracketGroupId}");
                var baseOrder = order.CreateAlpacaOrder(order.AbsoluteQuantity, _symbolMapper, order.Type);

                if (baseOrder is not AlpacaMarket.SimpleOrderBase simpleBaseOrder)
                {
                    Log.Error($"{nameof(AlpacaBrokerage)}.PlaceBracketOrder: Entry order type {order.Type} " +
                        $"does not support .Bracket(). Only Market and Limit entries are supported.");
                    OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero,
                        $"Bracket entry type {order.Type} not supported. Use Market or Limit.")
                    { Status = Orders.OrderStatus.Invalid });
                    return;
                }

                // Step 2: Chain .Bracket() call to create the bracket order.
                // .Bracket() returns a BracketOrder which extends AdvancedOrderBase → OrderBase.
                // PostOrderAsync accepts OrderBase, so this works directly.
                Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceBracketOrder: Chaining .Bracket() call. " +
                    $"TakeProfit={bracketProps.TakeProfitLimitPrice}, StopLoss={bracketProps.StopLossStopPrice}, " +
                    $"StopLossLimit={bracketProps.StopLossLimitPrice}");

                AlpacaMarket.OrderBase bracketOrder;
                if (bracketProps.StopLossLimitPrice.HasValue)
                {
                    // Bracket with stop-limit leg
                    bracketOrder = simpleBaseOrder.Bracket(
                        takeProfitLimitPrice: bracketProps.TakeProfitLimitPrice.Value,
                        stopLossStopPrice: bracketProps.StopLossStopPrice.Value,
                        stopLossLimitPrice: bracketProps.StopLossLimitPrice.Value);
                }
                else
                {
                    // Bracket with stop-market leg
                    bracketOrder = simpleBaseOrder.Bracket(
                        takeProfitLimitPrice: bracketProps.TakeProfitLimitPrice.Value,
                        stopLossStopPrice: bracketProps.StopLossStopPrice.Value);
                }

                // Step 3: Submit to Alpaca
                Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceBracketOrder: Submitting bracket order to Alpaca for group {bracketProps.BracketGroupId}");
                var response = _tradingClient.PostOrderAsync(bracketOrder).SynchronouslyAwaitTaskResult();

                if (response == null || response.OrderStatus == AlpacaMarket.OrderStatus.Rejected)
                {
                    Log.Error($"{nameof(AlpacaBrokerage)}.PlaceBracketOrder: Bracket order REJECTED by Alpaca for group {bracketProps.BracketGroupId}. " +
                        $"Response: {response?.OrderStatus}");
                    OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero,
                        "Bracket Order Rejected by Alpaca") { Status = Orders.OrderStatus.Invalid });
                    return;
                }

                // Step 4: Map the entry order
                Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceBracketOrder: Bracket order ACCEPTED. " +
                    $"EntryAlpacaId={response.OrderId}, LegsCount={response.Legs.Count}, " +
                    $"GroupId={bracketProps.BracketGroupId}");

                order.BrokerId.Add(response.OrderId.ToString());
                _duplicationExecutionOrderIdByBrokerageOrderId.TryAdd(response.OrderId, []);

                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero,
                    $"Bracket Order Submitted (group: {bracketProps.BracketGroupId})")
                { Status = Orders.OrderStatus.Submitted });

                // Step 5: Create phantom LEAN orders for the legs
                // The legs are available immediately in the response even though
                // they won't activate until the entry fills.
                Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceBracketOrder: Processing {response.Legs.Count} legs " +
                    $"for group {bracketProps.BracketGroupId}");

                foreach (var leg in response.Legs)
                {
                    Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceBracketOrder: Processing leg. " +
                        $"AlpacaId={leg.OrderId}, Type={leg.OrderType}, Side={leg.OrderSide}, " +
                        $"LimitPrice={leg.LimitPrice}, StopPrice={leg.StopPrice}");
                    CreateAndRegisterLegOrder(leg, order.Symbol, bracketProps);
                }

                Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceBracketOrder: Bracket order fully processed for group {bracketProps.BracketGroupId}");
            }
            catch (Exception ex)
            {
                Log.Error($"{nameof(AlpacaBrokerage)}.PlaceBracketOrder: Exception placing bracket order for group {bracketProps.BracketGroupId}: {ex}");
                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero,
                    $"Bracket Order Failed: {ex.Message}") { Status = Orders.OrderStatus.Invalid });
            }
        }

        /// <summary>
        /// Creates a phantom LEAN order for an Alpaca bracket leg and registers it
        /// with the deduplication map, bracket leg mapping, and BracketOrderManager.
        /// </summary>
        private void CreateAndRegisterLegOrder(IOrder alpacaLeg, Symbol symbol,
            AlpacaBracketOrderProperties bracketProps)
        {
            // Determine the leg type based on the Alpaca order type
            var legType = alpacaLeg.OrderType switch
            {
                AlpacaMarket.OrderType.Limit => BracketLegType.TakeProfit,
                AlpacaMarket.OrderType.Stop => BracketLegType.StopLoss,
                AlpacaMarket.OrderType.StopLimit => BracketLegType.StopLoss,
                _ => throw new NotSupportedException(
                    $"Unexpected bracket leg order type: {alpacaLeg.OrderType}")
            };

            // Build bracket properties and pass via TryConvertToLeanOrder so they're
            // set through the constructor (Order.Properties has a private setter)
            var legProps = new AlpacaBracketOrderProperties
            {
                BracketGroupId = bracketProps.BracketGroupId,
                LegType = legType
            };

            // Convert the Alpaca leg order to a LEAN order with bracket properties
            if (!TryConvertToLeanOrder(alpacaLeg, out var leanOrder, legProps))
            {
                Log.Error($"{nameof(AlpacaBrokerage)}.CreateAndRegisterLegOrder: Failed to convert bracket leg " +
                    $"AlpacaId={alpacaLeg.OrderId} to LEAN order for group {bracketProps.BracketGroupId}");
                return;
            }

            Log.Debug($"{nameof(AlpacaBrokerage)}.CreateAndRegisterLegOrder: Leg type={legType}, " +
                $"AlpacaId={alpacaLeg.OrderId}, LeanOrderType={leanOrder.Type}, " +
                $"GroupId={bracketProps.BracketGroupId}");

            // Register in the bracket leg mapping for trade update routing
            _bracketLegMapping[alpacaLeg.OrderId] = new BracketLegInfo
            {
                BracketGroupId = bracketProps.BracketGroupId,
                AlpacaLegOrderId = alpacaLeg.OrderId,
                LegType = legType,
            };

            Log.Debug($"{nameof(AlpacaBrokerage)}.CreateAndRegisterLegOrder: Registered leg in _bracketLegMapping. " +
                $"AlpacaId={alpacaLeg.OrderId}, GroupId={bracketProps.BracketGroupId}, Type={legType}");

            // Store in provisional mapping BEFORE firing the notification.
            // This ensures HandleTradeUpdate can find the order even if WebSocket
            // trade updates arrive before the engine processes the async notification.
            var alpacaOrderIdStr = alpacaLeg.OrderId.ToString();
            _provisionalBracketLegOrders[alpacaOrderIdStr] = leanOrder;
            Log.Debug($"{nameof(AlpacaBrokerage)}.CreateAndRegisterLegOrder: Stored in provisional mapping. " +
                $"AlpacaId={alpacaOrderIdStr}, GroupId={bracketProps.BracketGroupId}, Type={legType}");

            // Notify LEAN's engine about this new order so it creates tracking structures
            OnNewBrokerageOrderNotification(new NewBrokerageOrderNotificationEventArgs(leanOrder));

            // The engine processes the notification asynchronously and assigns a LEAN order ID.
            // Register with BracketOrderManager after the ticket is available.
            // Note: The ticket may not be immediately available; HandleTradeUpdate will also
            // try to register if needed when events arrive.
            if (_bracketManager != null && leanOrder.Id != 0)
            {
                var ticket = _orderProvider.GetOrderTicket(leanOrder.Id);
                if (ticket != null)
                {
                    _bracketManager.RegisterLegTicket(bracketProps.BracketGroupId, legType, ticket);
                    Log.Debug($"{nameof(AlpacaBrokerage)}.CreateAndRegisterLegOrder: Registered leg ticket " +
                        $"LeanId={leanOrder.Id} with BracketOrderManager for group {bracketProps.BracketGroupId}");
                }
                else
                {
                    Log.Debug($"{nameof(AlpacaBrokerage)}.CreateAndRegisterLegOrder: Ticket not yet available " +
                        $"for LeanId={leanOrder.Id}. Will be registered when trade update arrives.");
                }
            }
        }

        /// <summary>
        /// Places an OCO (One-Cancels-Other) order using Alpaca's native OCO API.
        /// Creates a linked pair of sell orders: limit (take-profit) + stop (stop-loss).
        /// The primary order submitted through LEAN's pipeline is the take-profit (Limit) leg.
        /// The stop leg is created as a phantom LEAN order.
        /// </summary>
        private void PlaceOcoOrder(Order order, AlpacaOcoOrderProperties ocoProps)
        {
            try
            {
                // Step 1: Create the primary leg (limit sell = take-profit).
                // This is the order that was submitted through LEAN's pipeline.
                // OCO primary leg must be a Limit order.
                Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceOcoOrder: Creating base order for group {ocoProps.OcoGroupId}");
                var baseOrder = order.CreateAlpacaOrder(order.AbsoluteQuantity, _symbolMapper, order.Type);

                // .OneCancelsOther() is defined on LimitOrder (not SimpleOrderBase like .Bracket()).
                if (baseOrder is not AlpacaMarket.LimitOrder limitOrder)
                {
                    Log.Error($"{nameof(AlpacaBrokerage)}.PlaceOcoOrder: OCO primary leg must be Limit. Got {order.Type}.");
                    OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero,
                        $"OCO primary leg must be Limit. Got {order.Type}.")
                    { Status = Orders.OrderStatus.Invalid });
                    return;
                }

                // Step 2: Chain .OneCancelsOther() to create the OCO pair.
                Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceOcoOrder: Chaining .OneCancelsOther(). " +
                    $"StopLoss={ocoProps.StopLossStopPrice}, StopLimit={ocoProps.StopLossLimitPrice}");

                AlpacaMarket.OrderBase ocoOrder;
                if (ocoProps.StopLossLimitPrice.HasValue)
                {
                    ocoOrder = limitOrder.OneCancelsOther(
                        stopLossStopPrice: ocoProps.StopLossStopPrice.Value,
                        stopLossLimitPrice: ocoProps.StopLossLimitPrice.Value);
                }
                else
                {
                    ocoOrder = limitOrder.OneCancelsOther(
                        stopLossStopPrice: ocoProps.StopLossStopPrice.Value);
                }

                // Step 3: Submit to Alpaca
                Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceOcoOrder: Submitting OCO to Alpaca for group {ocoProps.OcoGroupId}");
                var response = _tradingClient.PostOrderAsync(ocoOrder).SynchronouslyAwaitTaskResult();

                if (response == null || response.OrderStatus == AlpacaMarket.OrderStatus.Rejected)
                {
                    Log.Error($"{nameof(AlpacaBrokerage)}.PlaceOcoOrder: OCO REJECTED by Alpaca for group {ocoProps.OcoGroupId}. " +
                        $"Response: {response?.OrderStatus}");
                    OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero,
                        "OCO Order Rejected by Alpaca") { Status = Orders.OrderStatus.Invalid });
                    return;
                }

                // Step 4: Map the primary (take-profit) leg
                Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceOcoOrder: OCO ACCEPTED. " +
                    $"PrimaryAlpacaId={response.OrderId}, LegsCount={response.Legs.Count}, " +
                    $"GroupId={ocoProps.OcoGroupId}");

                order.BrokerId.Add(response.OrderId.ToString());
                _duplicationExecutionOrderIdByBrokerageOrderId.TryAdd(response.OrderId, []);

                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero,
                    $"OCO Order Submitted (group: {ocoProps.OcoGroupId})")
                { Status = Orders.OrderStatus.Submitted });

                // Step 5: Create phantom LEAN order for the stop leg.
                // OCO orders have exactly 1 leg (the stop). The primary order IS the limit.
                Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceOcoOrder: Processing {response.Legs.Count} legs " +
                    $"for group {ocoProps.OcoGroupId}");

                foreach (var leg in response.Legs)
                {
                    Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceOcoOrder: Processing leg. " +
                        $"AlpacaId={leg.OrderId}, Type={leg.OrderType}, Side={leg.OrderSide}, " +
                        $"StopPrice={leg.StopPrice}");
                    CreateAndRegisterOcoLegOrder(leg, order.Symbol, ocoProps);
                }

                Log.Debug($"{nameof(AlpacaBrokerage)}.PlaceOcoOrder: OCO fully processed for group {ocoProps.OcoGroupId}");
            }
            catch (Exception ex)
            {
                Log.Error($"{nameof(AlpacaBrokerage)}.PlaceOcoOrder: Exception placing OCO for group {ocoProps.OcoGroupId}: {ex}");
                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero,
                    $"OCO Order Failed: {ex.Message}") { Status = Orders.OrderStatus.Invalid });
            }
        }

        /// <summary>
        /// Creates a phantom LEAN order for an Alpaca OCO stop leg and registers it
        /// with tracking dictionaries and the BracketOrderManager.
        /// Follows the same pattern as <see cref="CreateAndRegisterLegOrder"/> for bracket legs.
        /// </summary>
        private void CreateAndRegisterOcoLegOrder(IOrder alpacaLeg, Symbol symbol,
            AlpacaOcoOrderProperties ocoProps)
        {
            // OCO legs are always the stop-loss side (the primary is the limit/target)
            var legType = alpacaLeg.OrderType switch
            {
                AlpacaMarket.OrderType.Stop => BracketLegType.StopLoss,
                AlpacaMarket.OrderType.StopLimit => BracketLegType.StopLoss,
                AlpacaMarket.OrderType.Limit => BracketLegType.TakeProfit,
                _ => throw new NotSupportedException(
                    $"Unexpected OCO leg order type: {alpacaLeg.OrderType}")
            };

            // Use AlpacaBracketOrderProperties for leg orders (same as bracket legs)
            // so the existing HandleTradeUpdate routing works identically.
            var legProps = new AlpacaBracketOrderProperties
            {
                BracketGroupId = ocoProps.OcoGroupId,
                LegType = legType
            };

            if (!TryConvertToLeanOrder(alpacaLeg, out var leanOrder, legProps))
            {
                Log.Error($"{nameof(AlpacaBrokerage)}.CreateAndRegisterOcoLegOrder: Failed to convert OCO leg " +
                    $"AlpacaId={alpacaLeg.OrderId} to LEAN order for group {ocoProps.OcoGroupId}");
                return;
            }

            Log.Debug($"{nameof(AlpacaBrokerage)}.CreateAndRegisterOcoLegOrder: Leg type={legType}, " +
                $"AlpacaId={alpacaLeg.OrderId}, LeanOrderType={leanOrder.Type}, " +
                $"GroupId={ocoProps.OcoGroupId}");

            // Register in bracket leg mapping for trade update routing
            _bracketLegMapping[alpacaLeg.OrderId] = new BracketLegInfo
            {
                BracketGroupId = ocoProps.OcoGroupId,
                AlpacaLegOrderId = alpacaLeg.OrderId,
                LegType = legType,
            };

            // Store in provisional mapping
            var alpacaOrderIdStr = alpacaLeg.OrderId.ToString();
            _provisionalBracketLegOrders[alpacaOrderIdStr] = leanOrder;

            // Notify LEAN's engine
            OnNewBrokerageOrderNotification(new NewBrokerageOrderNotificationEventArgs(leanOrder));

            // Register with BracketOrderManager
            if (_bracketManager != null && leanOrder.Id != 0)
            {
                var ticket = _orderProvider.GetOrderTicket(leanOrder.Id);
                if (ticket != null)
                {
                    _bracketManager.RegisterLegTicket(ocoProps.OcoGroupId, legType, ticket);
                    Log.Debug($"{nameof(AlpacaBrokerage)}.CreateAndRegisterOcoLegOrder: Registered leg ticket " +
                        $"LeanId={leanOrder.Id} with BracketOrderManager for group {ocoProps.OcoGroupId}");
                }
                else
                {
                    Log.Debug($"{nameof(AlpacaBrokerage)}.CreateAndRegisterOcoLegOrder: Ticket not yet available " +
                        $"for LeanId={leanOrder.Id}. Will be registered when trade update arrives.");
                }
            }
        }

        internal void HandleTradeUpdate(ITradeUpdate obj)
        {
            try
            {
                // TODO: Revert to Log.Debug when issue #28 is resolved
                Log.Trace($"{nameof(AlpacaBrokerage)}.{nameof(HandleTradeUpdate)}: {obj}");

                // --- Bracket leg identification ---
                // Check if this trade update is for a known bracket leg by Alpaca order ID.
                // This helps with logging context and bracket-specific handling.
                var isBracketLeg = _bracketLegMapping.TryGetValue(obj.Order.OrderId, out var bracketLegInfo);
                if (isBracketLeg)
                {
                    Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(HandleTradeUpdate)}: Event for bracket leg. " +
                        $"AlpacaId={obj.Order.OrderId}, GroupId={bracketLegInfo.BracketGroupId}, " +
                        $"LegType={bracketLegInfo.LegType}, Event={obj.Event}");
                }

                var brokerageOrderId = obj.Order.OrderId.ToString();
                var newLeanOrderStatus = GetOrderStatus(obj.Event);
                if (!TryGetOrRemoveCrossZeroOrder(brokerageOrderId, newLeanOrderStatus, out var leanOrder))
                {
                    leanOrder = _orderProvider.GetOrdersByBrokerageId(brokerageOrderId)?.SingleOrDefault();
                    if (leanOrder == null && TryConvertToLeanOrder(obj.Order, out leanOrder))
                    {
                        OnNewBrokerageOrderNotification(new(leanOrder));

                        if (leanOrder.Id != 0)
                        {
                            OnOrderEvent(new OrderEvent(leanOrder, DateTime.UtcNow, OrderFee.Zero, $"Order was submitted outside Lean")
                            { Status = Orders.OrderStatus.Submitted });

                            if (newLeanOrderStatus == Orders.OrderStatus.Submitted)
                            {
                                return;
                            }
                        }
                        else
                        {
                            leanOrder = null;
                        }
                    }
                }
                // --- Bracket leg provisional lookup ---
                // If the order wasn't found via normal channels, check the provisional
                // bracket leg mapping. This handles the race condition where WebSocket
                // trade updates arrive before LEAN's engine processes the async
                // OnNewBrokerageOrderNotification for bracket legs.
                if (leanOrder == null && _provisionalBracketLegOrders.TryGetValue(brokerageOrderId, out leanOrder))
                {
                    Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(HandleTradeUpdate)}: Found bracket leg in provisional mapping. " +
                        $"AlpacaId={brokerageOrderId}, LeanOrderType={leanOrder.Type}");
                }

                // Clean up provisional mapping when order reaches terminal status
                if (leanOrder != null && newLeanOrderStatus.IsClosed())
                {
                    _provisionalBracketLegOrders.TryRemove(brokerageOrderId, out _);
                }

                if (leanOrder == null)
                {
                    Log.Error($"{nameof(AlpacaBrokerage)}.{nameof(HandleTradeUpdate)}: order id not found: {obj.Order.OrderId}" +
                        (isBracketLeg ? $" (bracket leg: {bracketLegInfo})" : ""));
                    return;
                }

                // --- Try to register bracket leg ticket if we haven't yet ---
                // The ticket might not have been available when the leg was first created
                // (async processing). Now that we have the LEAN order, we can register it.
                if (isBracketLeg && _bracketManager != null)
                {
                    var ticket = _orderProvider.GetOrderTicket(leanOrder.Id);
                    if (ticket != null)
                    {
                        // RegisterLegTicket is idempotent — it's fine to call multiple times
                        _bracketManager.RegisterLegTicket(bracketLegInfo.BracketGroupId, bracketLegInfo.LegType, ticket);
                    }
                }

                switch (obj.Event)
                {
                    case TradeEvent.New:
                        // we don't send anything for this event
                        _duplicationExecutionOrderIdByBrokerageOrderId.TryAdd(obj.Order.OrderId, []);
                        // Deferred PATCH retry: if UpdateOrder was rejected with "pending_new" for this
                        // order, the request was stored. Now that the order is in stable "new" state,
                        // retry the PATCH. Alpaca will respond with a "replaced" event which the normal
                        // HandleTradeUpdate flow handles (UpdateSubmitted, bracket leg remapping, etc.).
                        if (_pendingLegReplaces.TryRemove(obj.Order.OrderId, out var retryRequest))
                        {
                            Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(HandleTradeUpdate)}: " +
                                $"Retrying deferred PATCH for order {obj.Order.OrderId} " +
                                $"(order now stable after pending_new).");
                            try
                            {
                                _tradingClient.PatchOrderAsync(retryRequest).SynchronouslyAwaitTaskResult();
                                Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(HandleTradeUpdate)}: " +
                                    $"Deferred PATCH submitted successfully for order {obj.Order.OrderId}. " +
                                    $"Awaiting Alpaca 'replaced' event for final confirmation.");
                            }
                            catch (Exception retryEx)
                            {
                                Log.Error(retryEx, $"{nameof(AlpacaBrokerage)}.{nameof(HandleTradeUpdate)}: " +
                                    $"Deferred PATCH retry failed for order {obj.Order.OrderId}.");
                                if (leanOrder != null)
                                {
                                    OnOrderEvent(new OrderEvent(leanOrder, DateTime.UtcNow, OrderFee.Zero, retryEx.Message)
                                        { Status = Orders.OrderStatus.Invalid });
                                }
                            }
                        }
                        return;
                    case TradeEvent.PendingNew:
                        // we don't send anything for this event
                        _duplicationExecutionOrderIdByBrokerageOrderId.TryAdd(obj.Order.OrderId, []);
                        return;
                    case TradeEvent.Rejected:
                    case TradeEvent.Canceled:
                    case TradeEvent.Replaced:
                    case TradeEvent.Expired:
                    case TradeEvent.DoneForDay:
                        if (_duplicationExecutionOrderIdByBrokerageOrderId.TryRemove(obj.Order.OrderId, out _))
                        {
                            if (newLeanOrderStatus == Orders.OrderStatus.UpdateSubmitted)
                            {
                                var replacedBrokerageOrderId = obj.Order.ReplacedByOrderId.Value;
                                // If the order already exists in the BrokerId list, it means the update was initiated by Lean.
                                // Otherwise, the order was updated outside of Lean and we need to notify about the new brokerage ID.
                                if (!leanOrder.BrokerId.Contains(replacedBrokerageOrderId.ToString()))
                                {
                                    OnOrderIdChangedEvent(new() { OrderId = leanOrder.Id, BrokerId = [replacedBrokerageOrderId.ToString()] });
                                }
                                _duplicationExecutionOrderIdByBrokerageOrderId[replacedBrokerageOrderId] = [];

                                // --- Bracket leg ID remapping on Replaced ---
                                // When Alpaca replaces a bracket leg (e.g., due to partial fill on
                                // the sibling, or a PATCH update), the old Alpaca ID becomes invalid
                                // and a new one is assigned. Update our tracking.
                                if (isBracketLeg && _bracketLegMapping.TryRemove(obj.Order.OrderId, out var oldLegInfo))
                                {
                                    oldLegInfo.AlpacaLegOrderId = replacedBrokerageOrderId;
                                    _bracketLegMapping[replacedBrokerageOrderId] = oldLegInfo;
                                    Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(HandleTradeUpdate)}: Remapped bracket leg. " +
                                        $"OldAlpacaId={obj.Order.OrderId} → NewAlpacaId={replacedBrokerageOrderId}, " +
                                        $"GroupId={oldLegInfo.BracketGroupId}, LegType={oldLegInfo.LegType}");
                                }
                            }

                            if (!_tradeEventReason.TryGetValue(obj.Event, out var message))
                            {
                                message = $"{nameof(AlpacaBrokerage)} Order Event";
                            }

                            // Add bracket context to the message if applicable
                            if (isBracketLeg)
                            {
                                message += $" [bracket leg: {bracketLegInfo.LegType}, group: {bracketLegInfo.BracketGroupId}]";
                            }

                            var statusEvent = new OrderEvent(leanOrder, DateTime.UtcNow, OrderFee.Zero, message) { Status = newLeanOrderStatus };

                            // CRITICAL: ProcessOrderEvent BEFORE OnOrderEvent.
                            // This guarantees BracketGroup.State is updated when the strategy's
                            // on_order_event fires. (PRD R5.5)
                            var groupId = isBracketLeg ? bracketLegInfo?.BracketGroupId : null;
                            groupId ??= _bracketManager?.GetGroupIdForOrder(leanOrder.Id);
                            if (groupId != null)
                            {
                                _bracketManager?.ProcessOrderEvent(statusEvent);
                            }

                            OnOrderEvent(statusEvent);

                            // Safety-net cleanup: when a bracket group becomes terminal,
                            // sweep provisional entries for any remaining legs of this group.
                            if (groupId != null)
                            {
                                var group = _bracketManager?.GetGroup(groupId);
                                if (group != null && group.IsTerminal && _provisionalBracketLegOrders.Count > 0)
                                {
                                    foreach (var kvp in _provisionalBracketLegOrders)
                                    {
                                        if (kvp.Value.Properties is AlpacaBracketOrderProperties props &&
                                            props.BracketGroupId == groupId)
                                        {
                                            _provisionalBracketLegOrders.TryRemove(kvp.Key, out _);
                                            Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(HandleTradeUpdate)}: " +
                                                $"Cleaned up stale provisional entry for completed group {groupId}, " +
                                                $"AlpacaId={kvp.Key}");
                                        }
                                    }
                                }
                            }
                        }
                        return;
                    case TradeEvent.Fill:
                        if (_duplicationExecutionOrderIdByBrokerageOrderId.TryRemove(obj.Order.OrderId, out _))
                        {
                            if (isBracketLeg)
                            {
                                Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(HandleTradeUpdate)}: Bracket leg FILL. " +
                                    $"AlpacaId={obj.Order.OrderId}, GroupId={bracketLegInfo.BracketGroupId}, " +
                                    $"LegType={bracketLegInfo.LegType}, Price={obj.Price}. " +
                                    $"Alpaca will handle OCO cancellation of the sibling leg.");
                            }
                            break;
                        }
                        return;
                    case TradeEvent.PartialFill:
                        // Use TryGetValue to guard against KeyNotFoundException: if a Fill
                        // event already removed this order from the dedup map (e.g. a stale
                        // PartialFill replayed after WebSocket reconnection), skip it.
                        if (_duplicationExecutionOrderIdByBrokerageOrderId.TryGetValue(obj.Order.OrderId, out var execIds)
                            && execIds.Add(obj.ExecutionId.Value))
                        {
                            if (isBracketLeg)
                            {
                                Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(HandleTradeUpdate)}: Bracket leg PARTIAL FILL. " +
                                    $"AlpacaId={obj.Order.OrderId}, GroupId={bracketLegInfo.BracketGroupId}, " +
                                    $"LegType={bracketLegInfo.LegType}, Price={obj.Price}, " +
                                    $"FilledQty={obj.Order.FilledQuantity}/{obj.Order.Quantity}");
                            }
                            break;
                        }
                        return;
                    case TradeEvent.Accepted:
                    case TradeEvent.PendingReplace:
                    case TradeEvent.PendingCancel:
                        // Skip this event to avoid flooding logs
                        return;
                    default:
                        Log.Trace($"{nameof(AlpacaBrokerage)}.{nameof(HandleTradeUpdate)}.Event: {obj.Event}. TradeUpdate: {obj}");
                        return;
                }

                var leanSymbol = _symbolMapper.GetLeanSymbol(obj.Order.AssetClass, obj.Order.Symbol);

                // alpaca sends the accumulative filled quantity but we need the partial amount for our event
                _orderIdToFillQuantity.TryGetValue(leanOrder.Id, out var previouslyFilledAmount);
                var accumulativeFilledQuantity = _orderIdToFillQuantity[leanOrder.Id] =
                    obj.Order.OrderSide == OrderSide.Buy ? obj.Order.FilledQuantity : decimal.Negate(obj.Order.FilledQuantity);

                if (newLeanOrderStatus.IsClosed())
                {
                    // cleanup
                    _orderIdToFillQuantity.TryRemove(leanOrder.Id, out _);
                }

                var fee = new OrderFee(new CashAmount(0, Currencies.USD));
                if (newLeanOrderStatus == Orders.OrderStatus.Filled)
                {
                    var security = _securityProvider.GetSecurity(leanOrder.Symbol);
                    fee = security.FeeModel.GetOrderFee(new OrderFeeParameters(security, leanOrder));
                }

                var orderEvent = new OrderEvent(leanOrder, obj.TimestampUtc.HasValue ? obj.TimestampUtc.Value : DateTime.UtcNow, fee)
                {
                    Status = newLeanOrderStatus,
                    FillPrice = obj.Price ?? 0m,
                    FillQuantity = accumulativeFilledQuantity - previouslyFilledAmount,
                };

                // CRITICAL: ProcessOrderEvent BEFORE OnOrderEvent.
                // This guarantees BracketGroup.State is updated when the strategy's
                // on_order_event fires. (PRD R5.5)
                if (isBracketLeg || _bracketManager?.GetGroupIdForOrder(leanOrder.Id) != null)
                {
                    _bracketManager?.ProcessOrderEvent(orderEvent);
                }

                // if we filled the order and have another contingent order waiting, submit it
                if (!TryHandleRemainingCrossZeroOrder(leanOrder, orderEvent))
                {
                    OnOrderEvent(orderEvent);
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"TradeUpdate: {obj}");
                throw;
            }
        }

        /// <summary>
        /// Updates the order with the same id.
        /// For bracket legs, also updates the _bracketLegMapping with the new Alpaca order ID
        /// that results from the PATCH operation (Alpaca replaces the order).
        /// </summary>
        /// <param name="order">The new order information</param>
        /// <returns>True if the request was made for the order to be updated, false otherwise</returns>
        public override bool UpdateOrder(Order order)
        {
            if (!TryGetUpdateCrossZeroOrderQuantity(order, out var orderQuantity))
            {
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, -1, $"{nameof(AlpacaBrokerage)}.{nameof(UpdateOrder)}: Unable to modify order quantities."));
                return false;
            }

            var brokerageOrderId = order.BrokerId.Last();

            // Debug logging for bracket leg updates
            var oldAlpacaGuid = new Guid(brokerageOrderId);
            var isBracketLeg = _bracketLegMapping.TryGetValue(oldAlpacaGuid, out var legInfo);
            if (isBracketLeg)
            {
                Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(UpdateOrder)}: Updating bracket leg. " +
                    $"LeanOrderId={order.Id}, AlpacaId={brokerageOrderId}, " +
                    $"GroupId={legInfo.BracketGroupId}, LegType={legInfo.LegType}, " +
                    $"OrderType={order.Type}");
            }

            var pathOrderRequest = new ChangeOrderRequest(new Guid(brokerageOrderId)) { Quantity = Convert.ToInt64(Math.Abs(orderQuantity)) };

            switch (order)
            {
                case Orders.LimitOrder lo:
                    pathOrderRequest.LimitPrice = lo.LimitPrice;
                    break;
                case Orders.TrailingStopOrder sto:
                    pathOrderRequest.TrailOffset = AlpacaBrokerageExtensions.GetTrailOffsetValue(sto);
                    break;
                case StopMarketOrder smo:
                    pathOrderRequest.StopPrice = smo.StopPrice;
                    break;
                case Orders.StopLimitOrder slo:
                    pathOrderRequest.LimitPrice = slo.LimitPrice;
                    pathOrderRequest.StopPrice = slo.StopPrice;
                    break;
            }

            try
            {
                IOrder response = null;
                _messageHandler.WithLockedStream(() =>
                {
                    response = _tradingClient.PatchOrderAsync(pathOrderRequest).SynchronouslyAwaitTaskResult();
                    if (response == null || response.OrderStatus == AlpacaMarket.OrderStatus.Rejected)
                    {
                        OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, $"{nameof(AlpacaBrokerage)} Order Event") { Status = Orders.OrderStatus.Invalid });
                        return;
                    }

                    var brokerageOrderId = response.OrderId.ToString();
                    if (!order.BrokerId.Contains(brokerageOrderId))
                    {
                        order.BrokerId.Add(brokerageOrderId);
                    }

                    // Register the new brokerage ID in the deduplication map.
                    // When update + cancel happen quickly, Alpaca may skip the
                    // intermediate "replaced"/"new" events and only send "canceled"
                    // for the replacement ID. Without this, the cancel event is
                    // silently dropped because Remove() returns false.
                    _duplicationExecutionOrderIdByBrokerageOrderId[response.OrderId] = [];

                    // --- Bracket leg: remap Alpaca order ID after PATCH ---
                    // Alpaca replaces the order, giving it a new ID. Update our tracking.
                    if (isBracketLeg && _bracketLegMapping.TryRemove(oldAlpacaGuid, out var oldLegInfo))
                    {
                        oldLegInfo.AlpacaLegOrderId = response.OrderId;
                        _bracketLegMapping[response.OrderId] = oldLegInfo;
                        Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(UpdateOrder)}: Remapped bracket leg after PATCH. " +
                            $"OldAlpacaId={oldAlpacaGuid} → NewAlpacaId={response.OrderId}, " +
                            $"GroupId={oldLegInfo.BracketGroupId}, LegType={oldLegInfo.LegType}");
                    }
                });
                return response != null && response.OrderStatus != AlpacaMarket.OrderStatus.Rejected;
            }
            catch (Exception ex)
            {
                // Alpaca returns "order parameters are not changed" when the requested
                // price already matches the current order price. This is not an error —
                // the order is already at the target price, so treat it as success.
                // Firing Invalid here would kill the ticket and cause all subsequent
                // updates for this leg to be silently dropped by BracketOrderManager.
                if (ex.Message.Contains("order parameters are not changed"))
                {
                    Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(UpdateOrder)}: " +
                        $"Alpaca reported 'order parameters are not changed' for order {order.Id} — " +
                        $"treating as success (price already matches requested value).");
                    return true;
                }

                // Alpaca rejects PATCH while the order is in its transient "pending_new" state
                // (between "accepted" and "new"). This is the same deferred-retry pattern as
                // the LEAN-level "New" state deferral: store the request and replay it from
                // HandleTradeUpdate when TradeEvent.New arrives for this order.
                // Returning true keeps the ticket alive; the real UpdateSubmitted fires later
                // via the normal "replaced" event cascade once the retry PATCH succeeds.
                if (ex.Message.Contains("pending_new"))
                {
                    Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(UpdateOrder)}: " +
                        $"Alpaca 'pending_new' rejection for order {order.Id} (AlpacaId={oldAlpacaGuid}). " +
                        $"Storing PATCH request — will retry when order reaches 'new' state.");
                    _pendingLegReplaces[oldAlpacaGuid] = pathOrderRequest;
                    return true;
                }

                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, ex.Message) { Status = Orders.OrderStatus.Invalid });
                return false;
            }
        }

        /// <summary>
        /// Cancels the order with the specified ID
        /// </summary>
        /// <param name="order">The order to cancel</param>
        /// <returns>True if the request was made for the order to be canceled, false otherwise</returns>
        public override bool CancelOrder(Order order)
        {
            if (order.Status == Orders.OrderStatus.Filled)
            {
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, -1, "Order already filled"));
                return false;
            }

            if (order.Status is Orders.OrderStatus.Canceled)
            {
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, -1, "Order already canceled"));
                return false;
            }

            try
            {
                var response = false;
                _messageHandler.WithLockedStream(() =>
                {
                    var brokerageOrderId = new Guid(order.BrokerId.Last());
                    response = _tradingClient.CancelOrderAsync(brokerageOrderId).SynchronouslyAwaitTaskResult();
                });
                return response;
            }
            catch (Exception ex)
            {
                // Alpaca rejects cancel on sibling bracket legs while another leg is mid-replace.
                // Retry a few times with a brief delay to wait for the replace to complete.
                if (ex.Message.Contains("pending replacement"))
                {
                    const int maxRetries = 3;
                    const int retryDelayMs = 2000;
                    for (var retry = 1; retry <= maxRetries; retry++)
                    {
                        Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(CancelOrder)}: " +
                            $"Order {order.Id} pending replacement, retry {retry}/{maxRetries} in {retryDelayMs}ms");
                        System.Threading.Thread.Sleep(retryDelayMs);

                        if (order.Status is Orders.OrderStatus.Canceled or Orders.OrderStatus.Filled or Orders.OrderStatus.Invalid)
                        {
                            Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(CancelOrder)}: " +
                                $"Order {order.Id} already {order.Status} after waiting, no cancel needed.");
                            return true;
                        }

                        try
                        {
                            var retryResponse = false;
                            _messageHandler.WithLockedStream(() =>
                            {
                                var brokerageOrderId = new Guid(order.BrokerId.Last());
                                retryResponse = _tradingClient.CancelOrderAsync(brokerageOrderId).SynchronouslyAwaitTaskResult();
                            });
                            if (retryResponse)
                            {
                                Log.Debug($"{nameof(AlpacaBrokerage)}.{nameof(CancelOrder)}: " +
                                    $"Order {order.Id} cancel succeeded on retry {retry}.");
                                return true;
                            }
                        }
                        catch (Exception retryEx)
                        {
                            if (!retryEx.Message.Contains("pending replacement"))
                            {
                                // Different error — fall through to emit Invalid
                                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero,
                                    $"Cancel order {order.Id} failed: {retryEx.Message}") { Status = Orders.OrderStatus.Invalid });
                                return false;
                            }
                            // Same "pending replacement" error — continue retrying
                        }
                    }
                    Log.Error($"{nameof(AlpacaBrokerage)}.{nameof(CancelOrder)}: " +
                        $"Order {order.Id} cancel failed after {maxRetries} retries (pending replacement).");
                }

                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, $"Cancel order {order.Id} failed: {ex.Message}") { Status = Orders.OrderStatus.Invalid });
                return false;
            }
        }

        /// <summary>
        /// Places an order that crosses zero (transitions from a short position to a long position or vice versa) and returns the response.
        /// This method implements brokerage-specific logic for placing such orders using Tradier brokerage.
        /// </summary>
        /// <param name="crossZeroOrderRequest">The request object containing details of the cross zero order to be placed.</param>
        /// <param name="isPlaceOrderWithLeanEvent">
        /// A boolean indicating whether the order should be placed with triggering a Lean event.
        /// Default is <c>true</c>, meaning Lean events will be triggered.
        /// </param>
        /// <returns>
        /// A <see cref="CrossZeroOrderResponse"/> object indicating the result of the order placement.
        /// </returns>
        protected override CrossZeroOrderResponse PlaceCrossZeroOrder(CrossZeroFirstOrderRequest crossZeroOrderRequest, bool isPlaceOrderWithLeanEvent)
        {
            var orderRequest = crossZeroOrderRequest.LeanOrder.CreateAlpacaOrder(crossZeroOrderRequest.AbsoluteOrderQuantity, _symbolMapper, crossZeroOrderRequest.OrderType);
            var response = _tradingClient.PostOrderAsync(orderRequest).SynchronouslyAwaitTaskResult();
            if (response == null || response.OrderStatus == AlpacaMarket.OrderStatus.Rejected)
            {
                return new CrossZeroOrderResponse(string.Empty, false);
            }

            var newBrokerageOrderId = response.OrderId.ToString();
            if (!crossZeroOrderRequest.LeanOrder.BrokerId.Contains(newBrokerageOrderId))
            {
                crossZeroOrderRequest.LeanOrder.BrokerId.Add(newBrokerageOrderId);
            }

            if (isPlaceOrderWithLeanEvent)
            {
                OnOrderEvent(new OrderEvent(crossZeroOrderRequest.LeanOrder, DateTime.UtcNow, OrderFee.Zero, $"{nameof(AlpacaBrokerage)} Order Event") { Status = Orders.OrderStatus.Submitted });
            }
            return new CrossZeroOrderResponse(newBrokerageOrderId, true);
        }

        /// <summary>
        /// Connects the client to the broker's remote servers
        /// </summary>
        public override void Connect()
        {
            if (_connected)
            {
                return;
            }

            ConnectAndAuthenticate(_orderStreamingClient);
            _connected = true;

            // Start reconciliation timer for live trading (Layer 1 + Layer 2)
            StartReconciliationTimer();
        }

        /// <summary>
        /// Connects to the specified streaming client and authenticates synchronously.
        /// Throws <see cref="InvalidOperationException"/> if authentication fails.
        /// </summary>
        /// <param name="client">The streaming client to connect and authenticate.</param>
        private static void ConnectAndAuthenticate(IStreamingClient streamingClient)
        {
            if (streamingClient is AlpacaStreamingClientWrapper s && s.IsOpenAndAuthorized)
            {
                return;
            }

            var authorizedStatus = streamingClient.ConnectAndAuthenticateAsync().SynchronouslyAwaitTaskResult();
            if (authorizedStatus != AuthStatus.Authorized)
            {
                throw new InvalidOperationException($"Connect(): Failed to connect to {streamingClient.GetStreamingClientName()}");
            }
        }

        private void ReconnectionLogic()
        {
            Task.Factory.StartNew(() =>
            {
                Log.Trace($"{nameof(AlpacaBrokerage)}.{nameof(ReconnectionLogic)}: Starting reconnection loop.");
                while (!_cancellationTokenSource.IsCancellationRequested)
                {
                    _reconnectionResetEvent.WaitOne(_cancellationTokenSource.Token);

                    // The server enforces a 90-second timeout for "partially dead" connections.
                    // If another WebSocket connection is opened with the same API key/secret
                    // before the old one is fully closed, this may trigger the
                    // "Too many connections" error. Waiting here prevents premature reconnection
                    // attempts that would conflict with the server's timeout window.
                    if (_cancellationTokenSource.Token.WaitHandle.WaitOne(TimeSpan.FromSeconds(90)))
                    {
                        break;
                    }

                    _reconnectionResetEvent.Reset();

                    try
                    {
                        Connect();

                        if (!IsConnected)
                        {
                            _reconnectionResetEvent.Set();
                        }
                        else
                        {
                            // if we are used as a brokerage ignore data queue handler updates
                            if (_subscriptionManager != null)
                            {
                                // resubscribe legacy aggregator-based subscriptions
                                var symbols = _subscriptionManager.GetSubscribedSymbols();
                                Unsubscribe(symbols);
                                Subscribe(symbols);
                            }

                            // Reconnect direct-feed upstream channels (not tracked by _subscriptionManager)
                            ReconnectDirectFeeds();

                            // let consumers know we are reconnected, avoid lean killing us
                            OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Reconnect, "Reconnected", "Brokerage Reconnected"));
                        }
                    }
                    catch (Exception ex)
                    {
                        _reconnectionResetEvent.Set();
                        Log.Error(ex);
                    }
                }
                Log.Trace($"{nameof(AlpacaBrokerage)}.{nameof(ReconnectionLogic)}: Reconnection loop ended.");
                _reconnectionResetEvent?.DisposeSafely();
            }, _cancellationTokenSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        /// <summary>
        /// Disconnects the client from the broker's remote servers
        /// </summary>
        public override void Disconnect()
        {
            StopQuoteFlushTimer();

            _orderStreamingClient?.DisconnectAsync()?.SynchronouslyAwaitTask();
            _equityStreamingClient?.DisconnectAsync()?.SynchronouslyAwaitTask();
            _cryptoStreamingClient?.DisconnectAsync()?.SynchronouslyAwaitTask();
            _optionsStreamingClient?.DisconnectAsync()?.SynchronouslyAwaitTask();
        }

        public override void Dispose()
        {
            _reconciliationTimer?.Dispose();
            _reconciliationTimer = null;
            StopQuoteFlushTimer();

            // Dispose all direct-feed enumerators
            foreach (var kvp in _directFeeds)
            {
                kvp.Value.Enumerator.Dispose();
            }
            _directFeeds.Clear();

            _cancellationTokenSource?.Cancel();
            _cancellationTokenSource?.DisposeSafely();

            _tradingClient.DisposeSafely();

            _equityHistoricalDataClient.DisposeSafely();
            _cryptoHistoricalDataClient.DisposeSafely();
            _optionsHistoricalDataClient.DisposeSafely();

            // streaming
            _orderStreamingClient.DisposeSafely();
            _equityStreamingClient.DisposeSafely();
            _cryptoStreamingClient.DisposeSafely();
            _optionsStreamingClient.DisposeSafely();
        }

        /// <summary>
        /// Gets the latest market quote for the specified symbol.
        /// </summary>
        /// <param name="symbol">The symbol for which to get the latest quote.</param>
        /// <returns>The latest quote for the specified symbol.</returns>
        /// <exception cref="NotSupportedException">Thrown when the symbol's security type is not supported.</exception>
        /// <exception cref="Exception">Thrown when an error occurs while fetching the quote.</exception>
        protected IQuote GetLatestQuote(Symbol symbol)
        {
            var brokerageSymbol = _symbolMapper.GetBrokerageSymbol(symbol);
            switch (symbol.SecurityType)
            {
                case SecurityType.Equity:
                    return _equityHistoricalDataClient.GetLatestQuoteAsync(new LatestMarketDataRequest(brokerageSymbol)).SynchronouslyAwaitTaskResult();
                case SecurityType.Option:
                    return _optionsHistoricalDataClient.ListLatestQuotesAsync(new LatestOptionsDataRequest(new string[] { brokerageSymbol })).SynchronouslyAwaitTaskResult()[brokerageSymbol];
                case SecurityType.Crypto:
                    return _cryptoHistoricalDataClient.ListLatestQuotesAsync(new LatestDataListRequest(new string[] { brokerageSymbol })).SynchronouslyAwaitTaskResult()[brokerageSymbol];
                default:
                    throw new NotSupportedException($"{nameof(AlpacaBrokerage)}.{nameof(GetLatestQuote)}: Security type {symbol.SecurityType} is not supported.");
            }
        }

        private static Orders.OrderStatus GetOrderStatus(TradeEvent tradeEvent)
        {
            switch (tradeEvent)
            {
                case TradeEvent.PendingNew:
                    return Orders.OrderStatus.New;
                case TradeEvent.New:
                    return Orders.OrderStatus.Submitted;
                case TradeEvent.Rejected:
                    return Orders.OrderStatus.Invalid;
                case TradeEvent.Canceled:
                    return Orders.OrderStatus.Canceled;
                case TradeEvent.Replaced:
                    return Orders.OrderStatus.UpdateSubmitted;
                case TradeEvent.Fill:
                    return Orders.OrderStatus.Filled;
                case TradeEvent.PartialFill:
                    return Orders.OrderStatus.PartiallyFilled;
                case TradeEvent.Expired:
                    return Orders.OrderStatus.Canceled;
                case TradeEvent.DoneForDay:
                    return Orders.OrderStatus.Canceled;
                default:
                    return Orders.OrderStatus.New;
            }
        }

        #endregion

        private bool CanSubscribe(Symbol symbol)
        {
            if (symbol.Value.IndexOfInvariant("universe", true) != -1 || symbol.IsCanonical())
            {
                return false;
            }
            return _symbolMapper.SupportedSecurityType.Contains(symbol.SecurityType);
        }

        private class SubscriptionEntry
        {
            public Symbol Symbol { get; set; }
            public decimal PriceMagnifier { get; set; }
            public Tick LastTradeTick { get; set; }
            public Tick LastQuoteTick { get; set; }
            public Tick LastOpenInterestTick { get; set; }
        }

        private class ModulesReadLicenseRead : Api.RestResponse
        {
            [JsonProperty(PropertyName = "license")]
            public string License;
            [JsonProperty(PropertyName = "organizationId")]
            public string OrganizationId;
        }

        /// <summary>
        /// Validate the user of this project has permission to be using it via our web API.
        /// </summary>
        private static void ValidateSubscription()
        {
            try
            {
                var productId = 347;
                var userId = Globals.UserId;
                var token = Globals.UserToken;
                var organizationId = Globals.OrganizationID;
                // Verify we can authenticate with this user and token
                var api = new ApiConnection(userId, token);
                if (!api.Connected)
                {
                    throw new ArgumentException("Invalid api user id or token, cannot authenticate subscription.");
                }
                // Compile the information we want to send when validating
                var information = new Dictionary<string, object>()
                {
                    {"productId", productId},
                    {"machineName", Environment.MachineName},
                    {"userName", Environment.UserName},
                    {"domainName", Environment.UserDomainName},
                    {"os", Environment.OSVersion}
                };
                // IP and Mac Address Information
                try
                {
                    var interfaceDictionary = new List<Dictionary<string, object>>();
                    foreach (var nic in NetworkInterface.GetAllNetworkInterfaces().Where(nic => nic.OperationalStatus == OperationalStatus.Up))
                    {
                        var interfaceInformation = new Dictionary<string, object>();
                        // Get UnicastAddresses
                        var addresses = nic.GetIPProperties().UnicastAddresses
                            .Select(uniAddress => uniAddress.Address)
                            .Where(address => !IPAddress.IsLoopback(address)).Select(x => x.ToString());
                        // If this interface has non-loopback addresses, we will include it
                        if (!addresses.IsNullOrEmpty())
                        {
                            interfaceInformation.Add("unicastAddresses", addresses);
                            // Get MAC address
                            interfaceInformation.Add("MAC", nic.GetPhysicalAddress().ToString());
                            // Add Interface name
                            interfaceInformation.Add("name", nic.Name);
                            // Add these to our dictionary
                            interfaceDictionary.Add(interfaceInformation);
                        }
                    }
                    information.Add("networkInterfaces", interfaceDictionary);
                }
                catch (Exception)
                {
                    // NOP, not necessary to crash if fails to extract and add this information
                }
                // Include our OrganizationId is specified
                if (!string.IsNullOrEmpty(organizationId))
                {
                    information.Add("organizationId", organizationId);
                }
                // Create HTTP request
                using var request = ApiUtils.CreateJsonPostRequest("modules/license/read", information);
                api.TryRequest(request, out ModulesReadLicenseRead result);
                if (!result.Success)
                {
                    throw new InvalidOperationException($"Request for subscriptions from web failed, Response Errors : {string.Join(',', result.Errors)}");
                }

                var encryptedData = result.License;
                // Decrypt the data we received
                DateTime? expirationDate = null;
                long? stamp = null;
                bool? isValid = null;
                if (encryptedData != null)
                {
                    // Fetch the org id from the response if we are null, we need it to generate our validation key
                    if (string.IsNullOrEmpty(organizationId))
                    {
                        organizationId = result.OrganizationId;
                    }
                    // Create our combination key
                    var password = $"{token}-{organizationId}";
                    var key = SHA256.HashData(Encoding.UTF8.GetBytes(password));
                    // Split the data
                    var info = encryptedData.Split("::");
                    var buffer = Convert.FromBase64String(info[0]);
                    var iv = Convert.FromBase64String(info[1]);
                    // Decrypt our information
                    using var aes = new AesManaged();
                    var decryptor = aes.CreateDecryptor(key, iv);
                    using var memoryStream = new MemoryStream(buffer);
                    using var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read);
                    using var streamReader = new StreamReader(cryptoStream);
                    var decryptedData = streamReader.ReadToEnd();
                    if (!decryptedData.IsNullOrEmpty())
                    {
                        var jsonInfo = JsonConvert.DeserializeObject<JObject>(decryptedData);
                        expirationDate = jsonInfo["expiration"]?.Value<DateTime>();
                        isValid = jsonInfo["isValid"]?.Value<bool>();
                        stamp = jsonInfo["stamped"]?.Value<int>();
                    }
                }
                // Validate our conditions
                if (!expirationDate.HasValue || !isValid.HasValue || !stamp.HasValue)
                {
                    throw new InvalidOperationException("Failed to validate subscription.");
                }

                var nowUtc = DateTime.UtcNow;
                var timeSpan = nowUtc - Time.UnixTimeStampToDateTime(stamp.Value);
                if (timeSpan > TimeSpan.FromHours(12))
                {
                    throw new InvalidOperationException("Invalid API response.");
                }
                if (!isValid.Value)
                {
                    throw new ArgumentException($"Your subscription is not valid, please check your product subscriptions on our website.");
                }
                if (expirationDate < nowUtc)
                {
                    throw new ArgumentException($"Your subscription expired {expirationDate}, please renew in order to use this product.");
                }
            }
            catch (Exception e)
            {
                Log.Error($"ValidateSubscription(): Failed during validation, shutting down. Error : {e.Message}");
                Environment.Exit(1);
            }
        }

        #region Reconciliation — Layer 1 + Layer 2

        /// <summary>
        /// Starts the reconciliation timer. Called during brokerage initialization (live only).
        /// 30-second initial delay, 60-second period.
        /// </summary>
        internal void StartReconciliationTimer()
        {
            if (_reconciliationTimer != null) return;

            _reconciliationTimer = new Timer(_ =>
            {
                try
                {
                    ReconcileOrderStateAsync().GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    Log.Error($"{nameof(AlpacaBrokerage)}.ReconciliationTimer: {ex.Message}");
                }
            }, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(60));

            Log.Debug($"{nameof(AlpacaBrokerage)}.StartReconciliationTimer: Timer started (30s delay, 60s period).");
        }

        /// <summary>
        /// Pauses the reconciliation timer. Called during WebSocket reconnection.
        /// </summary>
        internal void PauseReconciliationTimer()
        {
            _reconciliationTimer?.Change(Timeout.Infinite, Timeout.Infinite);
            Log.Debug($"{nameof(AlpacaBrokerage)}.PauseReconciliationTimer: Timer paused.");
        }

        /// <summary>
        /// Resumes the reconciliation timer after WebSocket reconnection.
        /// Triggers an immediate catch-up run before resuming normal 60s cycle.
        /// </summary>
        internal void ResumeReconciliationTimer()
        {
            // Immediate run (0ms delay), then resume 60s cycle
            _reconciliationTimer?.Change(TimeSpan.Zero, TimeSpan.FromSeconds(60));
            Log.Debug($"{nameof(AlpacaBrokerage)}.ResumeReconciliationTimer: Timer resumed with immediate catch-up.");
        }

        /// <summary>
        /// Layer 1: Periodic order state reconciliation.
        /// Polls Alpaca REST for actual order states and emits synthetic OrderEvents
        /// for any transitions the WebSocket missed.
        /// </summary>
        private async Task ReconcileOrderStateAsync()
        {
            if (_bracketManager == null) return;

            // --- Phase 1 (no lock): REST I/O ---
            // Build Alpaca-side snapshot of order states

            // Pass 1: Fetch all open orders from Alpaca
            IReadOnlyList<IOrder> alpacaOpenOrders;
            try
            {
                var request = new ListOrdersRequest
                {
                    OrderStatusFilter = OrderStatusFilter.Open,
                    RollUpNestedOrders = false,
                };
                alpacaOpenOrders = await _tradingClient.ListOrdersAsync(request).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.Error($"{nameof(AlpacaBrokerage)}.ReconcileOrderState: Failed to list open orders: {ex.Message}");
                return;
            }

            var alpacaOpenOrderIds = new HashSet<Guid>(alpacaOpenOrders.Select(o => o.OrderId));

            // Pass 2: For each LEAN open order not found in the open set, query individually
            var leanOpenOrders = _orderProvider.GetOpenOrders();
            var alpacaSnapshot = new Dictionary<Guid, AlpacaOrderSnapshot>();

            // Build snapshot from open orders
            foreach (var alpacaOrder in alpacaOpenOrders)
            {
                alpacaSnapshot[alpacaOrder.OrderId] = new AlpacaOrderSnapshot
                {
                    OrderId = alpacaOrder.OrderId,
                    Status = alpacaOrder.OrderStatus,
                    FilledQuantity = alpacaOrder.FilledQuantity,
                    AverageFillPrice = alpacaOrder.AverageFillPrice,
                    FilledAt = alpacaOrder.FilledAtUtc,
                    ReplacedByOrderId = alpacaOrder.ReplacedByOrderId,
                };
            }

            // Individual lookups for LEAN open orders not in Alpaca's open set
            foreach (var leanOrder in leanOpenOrders)
            {
                if (leanOrder.BrokerId == null || leanOrder.BrokerId.Count == 0) continue;
                var brokerageId = leanOrder.BrokerId.Last(); // Use last (current) ID
                if (!Guid.TryParse(brokerageId, out var alpacaGuid)) continue;

                if (!alpacaOpenOrderIds.Contains(alpacaGuid) && !alpacaSnapshot.ContainsKey(alpacaGuid))
                {
                    try
                    {
                        var alpacaOrder = await _tradingClient.GetOrderAsync(alpacaGuid).ConfigureAwait(false);
                        if (alpacaOrder != null)
                        {
                            alpacaSnapshot[alpacaOrder.OrderId] = new AlpacaOrderSnapshot
                            {
                                OrderId = alpacaOrder.OrderId,
                                Status = alpacaOrder.OrderStatus,
                                FilledQuantity = alpacaOrder.FilledQuantity,
                                AverageFillPrice = alpacaOrder.AverageFillPrice,
                                FilledAt = alpacaOrder.FilledAtUtc,
                                ReplacedByOrderId = alpacaOrder.ReplacedByOrderId,
                            };
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Trace($"{nameof(AlpacaBrokerage)}.ReconcileOrderState: " +
                            $"Individual lookup failed for {alpacaGuid}: {ex.Message}");
                    }
                }
            }

            // --- Phase 2 (inside lock): detect + emit ---
            var discrepancyCount = 0;

            _messageHandler.WithLockedStream(() =>
            {
                foreach (var leanOrder in leanOpenOrders)
                {
                    // Re-check order status inside the lock — a WS event may have processed
                    // this order between Phase 1 (GetOpenOrders) and Phase 2 (this lock).
                    // If the order is no longer open, skip it to avoid duplicate events.
                    if (leanOrder.Status.IsClosed())
                    {
                        continue;
                    }

                    if (leanOrder.BrokerId == null || leanOrder.BrokerId.Count == 0) continue;
                    var brokerageId = leanOrder.BrokerId.Last();
                    if (!Guid.TryParse(brokerageId, out var alpacaGuid)) continue;

                    if (!alpacaSnapshot.TryGetValue(alpacaGuid, out var snapshot))
                    {
                        Log.Trace($"[PLUGIN:RECONCILED] LEAN order {leanOrder.Id} (alpaca={alpacaGuid}) " +
                            $"not found in Alpaca snapshot.");
                        continue;
                    }

                    try
                    {
                        // Case 1: Terminal at Alpaca, open in LEAN
                        if (IsTerminalAlpacaStatus(snapshot.Status) && leanOrder.Status.IsOpen())
                        {
                            // Case 4 check: Cancel-then-fill race
                            if (snapshot.FilledQuantity > 0 &&
                                (snapshot.Status == AlpacaMarket.OrderStatus.Canceled ||
                                 snapshot.Status == AlpacaMarket.OrderStatus.Expired))
                            {
                                // Has fills — emit synthetic fill first
                                EmitSyntheticFill(leanOrder, snapshot, "case4-cancel-then-fill");
                                discrepancyCount++;
                            }
                            else
                            {
                                // Pure terminal — emit synthetic cancel
                                var statusMsg = snapshot.Status == AlpacaMarket.OrderStatus.DoneForDay
                                    ? "DoneForDay" : snapshot.Status.ToString();
                                var syntheticEvent = new OrderEvent(leanOrder, DateTime.UtcNow, OrderFee.Zero,
                                    $"[Reconciled] Order {statusMsg} at Alpaca (missed WebSocket event)")
                                {
                                    Status = Orders.OrderStatus.Canceled
                                };

                                _bracketManager?.ProcessOrderEvent(syntheticEvent);
                                OnOrderEvent(syntheticEvent);
                                discrepancyCount++;

                                Log.Trace($"[PLUGIN:RECONCILED] Synthetic cancel for LEAN order {leanOrder.Id}: " +
                                    $"Alpaca status={snapshot.Status}");
                            }
                        }
                        // Case 2: Missed fills
                        else if (snapshot.FilledQuantity > 0)
                        {
                            _orderIdToFillQuantity.TryGetValue(leanOrder.Id, out var knownFillQty);
                            var alpacaSignedQty = leanOrder.Direction == OrderDirection.Buy
                                ? snapshot.FilledQuantity
                                : -snapshot.FilledQuantity;
                            var delta = alpacaSignedQty - knownFillQty;

                            if (Math.Abs(delta) > 0)
                            {
                                EmitSyntheticFill(leanOrder, snapshot, "case2-missed-fill");
                                discrepancyCount++;
                            }
                        }
                        // Case 3: Replaced status
                        else if (snapshot.Status == AlpacaMarket.OrderStatus.Replaced &&
                                 snapshot.ReplacedByOrderId.HasValue)
                        {
                            HandleReplacedOrder(leanOrder, snapshot);
                            discrepancyCount++;
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"[PLUGIN:RECONCILED] Error processing LEAN order {leanOrder.Id}: {ex.Message}");
                    }
                }
            });

            Log.Trace($"[PLUGIN:RECONCILED] Reconciliation cycle complete: " +
                $"{leanOpenOrders.Count} orders checked, {discrepancyCount} discrepancies found.");

            // Run Layer 2 protective invariant (timer-triggered)
            try
            {
                await ReconcileProtectiveInvariantAsync(isTimerTriggered: true).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.Error($"{nameof(AlpacaBrokerage)}.ReconcileOrderState: Layer 2 failed: {ex.Message}");
            }
        }

        /// <summary>
        /// Emits a synthetic fill event for a missed fill detected by reconciliation.
        /// </summary>
        private void EmitSyntheticFill(Order leanOrder, AlpacaOrderSnapshot snapshot, string reason)
        {
            _orderIdToFillQuantity.TryGetValue(leanOrder.Id, out var knownFillQty);
            var alpacaSignedQty = leanOrder.Direction == OrderDirection.Buy
                ? snapshot.FilledQuantity
                : -snapshot.FilledQuantity;
            var delta = alpacaSignedQty - knownFillQty;

            if (Math.Abs(delta) == 0) return;

            // Update fill tracking
            _orderIdToFillQuantity[leanOrder.Id] = alpacaSignedQty;

            var isFull = Math.Abs(alpacaSignedQty) >= Math.Abs(leanOrder.Quantity);
            var status = isFull ? Orders.OrderStatus.Filled : Orders.OrderStatus.PartiallyFilled;
            var fillPrice = snapshot.AverageFillPrice ?? 0m;
            var fillTime = snapshot.FilledAt ?? DateTime.UtcNow;

            var syntheticEvent = new OrderEvent(leanOrder, fillTime, OrderFee.Zero,
                $"[Reconciled:{reason}] Fill detected via REST reconciliation")
            {
                Status = status,
                FillPrice = fillPrice,
                FillQuantity = delta,
            };

            _bracketManager?.ProcessOrderEvent(syntheticEvent);
            OnOrderEvent(syntheticEvent);

            _bracketManager?.EmitPluginMessage("[PLUGIN:RECONCILED]", new Dictionary<string, object>
            {
                ["severity"] = "info",
                ["group_id"] = _bracketManager?.GetGroupIdForOrder(leanOrder.Id),
                ["symbol"] = leanOrder.Symbol.Value,
                ["msg"] = $"Synthetic fill: {delta} shares at {fillPrice}",
                ["event_type"] = "fill",
                ["fill_qty"] = delta,
                ["fill_price"] = fillPrice,
            });

            Log.Trace($"[PLUGIN:RECONCILED] Synthetic fill for LEAN order {leanOrder.Id}: " +
                $"delta={delta}, price={fillPrice}, reason={reason}");
        }

        /// <summary>
        /// Handles a Replaced status detected by reconciliation (Layer 1 Case 3).
        /// </summary>
        private void HandleReplacedOrder(Order leanOrder, AlpacaOrderSnapshot snapshot)
        {
            if (!snapshot.ReplacedByOrderId.HasValue) return;

            var newAlpacaId = snapshot.ReplacedByOrderId.Value;
            var oldAlpacaId = snapshot.OrderId;

            // Emit OnOrderIdChangedEvent so LEAN updates Order.BrokerId
            OnOrderIdChangedEvent(new BrokerageOrderIdChangedEvent
            {
                OrderId = leanOrder.Id,
                BrokerId = new List<string> { newAlpacaId.ToString() }
            });

            // Remap _bracketLegMapping
            if (_bracketLegMapping.TryRemove(oldAlpacaId, out var legInfo))
            {
                legInfo.AlpacaLegOrderId = newAlpacaId;
                _bracketLegMapping[newAlpacaId] = legInfo;
            }

            // Remap dedup map
            if (_duplicationExecutionOrderIdByBrokerageOrderId.TryRemove(oldAlpacaId, out var execIds))
            {
                _duplicationExecutionOrderIdByBrokerageOrderId[newAlpacaId] = execIds;
            }

            _bracketManager?.EmitPluginMessage("[PLUGIN:REPLACED_ID]", new Dictionary<string, object>
            {
                ["severity"] = "info",
                ["group_id"] = legInfo?.BracketGroupId,
                ["symbol"] = leanOrder.Symbol.Value,
                ["msg"] = $"Order ID remapped: {oldAlpacaId} → {newAlpacaId}",
                ["old_id"] = oldAlpacaId.ToString(),
                ["new_id"] = newAlpacaId.ToString(),
            });

            Log.Trace($"[PLUGIN:RECONCILED] Replaced order {oldAlpacaId} → {newAlpacaId} " +
                $"for LEAN order {leanOrder.Id}");
        }

        /// <summary>
        /// Returns true if the Alpaca order status is terminal.
        /// </summary>
        private static bool IsTerminalAlpacaStatus(AlpacaMarket.OrderStatus status)
        {
            return status == AlpacaMarket.OrderStatus.Filled ||
                   status == AlpacaMarket.OrderStatus.Canceled ||
                   status == AlpacaMarket.OrderStatus.Expired ||
                   status == AlpacaMarket.OrderStatus.Replaced ||
                   status == AlpacaMarket.OrderStatus.DoneForDay ||
                   status == AlpacaMarket.OrderStatus.Rejected;
        }

        /// <summary>
        /// Layer 2: Protective invariant check.
        /// Verifies every Protected bracket has active stop + target orders at Alpaca.
        /// </summary>
        private async Task ReconcileProtectiveInvariantAsync(bool isTimerTriggered)
        {
            if (_bracketManager == null) return;

            var activeBrackets = _bracketManager.ActiveBrackets;

            foreach (var group in activeBrackets)
            {
                if (group.State != BracketState.Protected) continue;

                // Timer-triggered: skip brackets that entered Protected < 30s ago
                if (isTimerTriggered && group.ProtectedEnteredAt.HasValue &&
                    (DateTime.UtcNow - group.ProtectedEnteredAt.Value).TotalSeconds < 30)
                {
                    continue;
                }

                // Event-triggered guard (R2.5b): skip if EITHER exit leg ticket is not yet
                // registered. After entry fill or rescue OCO, both legs are registered
                // asynchronously — there's a brief window where one is null. Checking both
                // avoids false positives during the registration window.
                if (group.StopTicket == null || group.TargetTicket == null)
                {
                    continue;
                }

                try
                {
                    // Query Alpaca directly for each leg by its brokerage ID.
                    // This is the authoritative check — it returns the order regardless of
                    // whether it's "open", "held", "new", etc. Alpaca bracket stop legs are
                    // often in "held" status and excluded from ListOrdersAsync(Open), so a
                    // bulk open-orders query produces false positives. Individual GetOrderAsync
                    // calls are targeted (one per suspicious leg, max 2 per bracket) and
                    // return the actual Alpaca-side state.
                    var stopBrokerId = group.StopTicket != null
                        ? _orderProvider.GetOrderById(group.StopTicket.OrderId)?.BrokerId?.LastOrDefault()
                        : null;
                    var targetBrokerId = group.TargetTicket != null
                        ? _orderProvider.GetOrderById(group.TargetTicket.OrderId)?.BrokerId?.LastOrDefault()
                        : null;

                    var hasStop = await CheckLegExistsAtAlpacaAsync(stopBrokerId, "stop", group);
                    var hasTarget = await CheckLegExistsAtAlpacaAsync(targetBrokerId, "target", group);

                    if (!hasStop || !hasTarget)
                    {
                        Log.Error($"[PLUGIN:PROTECTIVE_INVARIANT] Protected bracket {group.GroupId} " +
                            $"for {group.Symbol.Value} missing protective orders. " +
                            $"HasStop={hasStop} (brokerId={stopBrokerId}), " +
                            $"HasTarget={hasTarget} (brokerId={targetBrokerId}).");

                        _bracketManager?.EmitPluginMessage("[PLUGIN:PROTECTIVE_INVARIANT]", new Dictionary<string, object>
                        {
                            ["severity"] = "warning",
                            ["group_id"] = group.GroupId,
                            ["symbol"] = group.Symbol.Value,
                            ["msg"] = $"Missing protective orders. HasStop={hasStop}, HasTarget={hasTarget}",
                            ["leg"] = !hasStop ? "stop" : "target",
                            ["price"] = !hasStop ? (object)group.StopLossPrice : group.TakeProfitPrice,
                        });

                        // TODO: Implement protective order re-creation via Alpaca REST PostOrderAsync.
                    }
                }
                catch (Exception ex)
                {
                    Log.Error($"{nameof(AlpacaBrokerage)}.ReconcileProtectiveInvariant: " +
                        $"Error checking group {group.GroupId}: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Checks whether a bracket leg exists at Alpaca by querying the individual order.
        /// Returns true if the order exists and is in a non-terminal status (New, Accepted,
        /// Held, PartiallyFilled, PendingNew, PendingReplace, PendingCancel — any status
        /// that means the order is still live or held at Alpaca).
        /// </summary>
        private async Task<bool> CheckLegExistsAtAlpacaAsync(string brokerId, string legName, BracketGroup group)
        {
            if (string.IsNullOrEmpty(brokerId))
            {
                Log.Trace($"[PLUGIN:PROTECTIVE_INVARIANT] {group.Symbol.Value} {legName}: no brokerId in LEAN");
                return false;
            }

            if (!Guid.TryParse(brokerId, out var alpacaGuid))
            {
                Log.Trace($"[PLUGIN:PROTECTIVE_INVARIANT] {group.Symbol.Value} {legName}: invalid brokerId format: {brokerId}");
                return false;
            }

            try
            {
                var alpacaOrder = await _tradingClient.GetOrderAsync(alpacaGuid).ConfigureAwait(false);
                if (alpacaOrder == null)
                {
                    return false;
                }

                // Terminal statuses mean the order is gone
                var isTerminal = alpacaOrder.OrderStatus == AlpacaMarket.OrderStatus.Filled ||
                                 alpacaOrder.OrderStatus == AlpacaMarket.OrderStatus.Canceled ||
                                 alpacaOrder.OrderStatus == AlpacaMarket.OrderStatus.Expired ||
                                 alpacaOrder.OrderStatus == AlpacaMarket.OrderStatus.Replaced;

                if (isTerminal)
                {
                    Log.Trace($"[PLUGIN:PROTECTIVE_INVARIANT] {group.Symbol.Value} {legName}: " +
                        $"Alpaca order {alpacaGuid} is terminal ({alpacaOrder.OrderStatus})");
                    return false;
                }

                // Order exists and is non-terminal (open, held, pending, etc.)
                return true;
            }
            catch (Exception ex)
            {
                // If we can't reach Alpaca, assume the order is there to avoid false alarms
                Log.Error($"[PLUGIN:PROTECTIVE_INVARIANT] {group.Symbol.Value} {legName}: " +
                    $"Failed to query Alpaca for {alpacaGuid}: {ex.Message}. Assuming order exists.");
                return true;
            }
        }

        /// <summary>
        /// FALLBACK EOD: Calls Alpaca's DELETE /v2/positions?cancel_orders=true
        /// to cancel all orders and liquidate all positions.
        /// Called by BracketOrderManager.LiquidateAllForEod() in live mode.
        /// </summary>
        internal bool LiquidateAllPositionsForEod()
        {
            try
            {
                Log.Debug($"{nameof(AlpacaBrokerage)}.LiquidateAllPositionsForEod: " +
                    $"Calling Alpaca DELETE /v2/positions?cancel_orders=true");

                // Use the trading client to close all positions
                var result = _tradingClient.DeleteAllPositionsAsync(
                    new DeleteAllPositionsRequest { CancelOrders = true }
                ).GetAwaiter().GetResult();

                Log.Debug($"{nameof(AlpacaBrokerage)}.LiquidateAllPositionsForEod: " +
                    $"Alpaca responded with {result?.Count ?? 0} position closures.");
                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"{nameof(AlpacaBrokerage)}.LiquidateAllPositionsForEod: Failed: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Snapshot of an Alpaca order's state, captured during Phase 1 of reconciliation.
        /// </summary>
        private class AlpacaOrderSnapshot
        {
            public Guid OrderId { get; set; }
            public AlpacaMarket.OrderStatus Status { get; set; }
            public decimal FilledQuantity { get; set; }
            public decimal? AverageFillPrice { get; set; }
            public DateTime? FilledAt { get; set; }
            public Guid? ReplacedByOrderId { get; set; }
        }

        #endregion
    }

    /// <summary>
    /// Tracks a bracket leg's relationship to its bracket group.
    /// Used to route Alpaca trade update events for bracket legs
    /// to the correct LEAN orders and bracket groups.
    /// </summary>
    internal class BracketLegInfo
    {
        /// <summary>The bracket group this leg belongs to.</summary>
        public string BracketGroupId { get; set; }

        /// <summary>The Alpaca order ID of this leg.</summary>
        public Guid AlpacaLegOrderId { get; set; }

        /// <summary>Whether this is a stop-loss or take-profit leg.</summary>
        public BracketLegType LegType { get; set; }

        public override string ToString()
        {
            return $"BracketLegInfo[Group={BracketGroupId}, AlpacaId={AlpacaLegOrderId}, Type={LegType}]";
        }
    }
}