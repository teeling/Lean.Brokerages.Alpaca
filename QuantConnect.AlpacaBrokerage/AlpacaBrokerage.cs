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
        internal readonly Dictionary<Guid, HashSet<Guid>> _duplicationExecutionOrderIdByBrokerageOrderId = [];

        /// <summary>
        /// Tracks unsupported TimeInForce values detected during order conversion.
        /// This allows the system to issue a warning for each unsupported TIF only once,
        /// preventing duplicate messages when processing multiple orders.
        /// </summary>
        private readonly HashSet<AlpacaMarket.TimeInForce> _unsupportedTimeInForce = [];

        // --- Bracket order support ---

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
            Log.Debug($"{nameof(AlpacaBrokerage)}.RegisterManager: BracketOrderManager registered.");
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
                if (TryConvertToLeanOrder(brokerageOrder, out var leanOrder))
                {
                    leanOrders.Add(leanOrder);
                }
            }

            return leanOrders;
        }

        private bool TryConvertToLeanOrder(IOrder brokerageOrder, out Order leanOrder)
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

            var orderProperties = new AlpacaOrderProperties();
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
            // Convert the Alpaca leg order to a LEAN order
            if (!TryConvertToLeanOrder(alpacaLeg, out var leanOrder))
            {
                Log.Error($"{nameof(AlpacaBrokerage)}.CreateAndRegisterLegOrder: Failed to convert bracket leg " +
                    $"AlpacaId={alpacaLeg.OrderId} to LEAN order for group {bracketProps.BracketGroupId}");
                return;
            }

            // Determine the leg type based on the Alpaca order type
            var legType = alpacaLeg.OrderType switch
            {
                AlpacaMarket.OrderType.Limit => BracketLegType.TakeProfit,
                AlpacaMarket.OrderType.Stop => BracketLegType.StopLoss,
                AlpacaMarket.OrderType.StopLimit => BracketLegType.StopLoss,
                _ => throw new NotSupportedException(
                    $"Unexpected bracket leg order type: {alpacaLeg.OrderType}")
            };

            Log.Debug($"{nameof(AlpacaBrokerage)}.CreateAndRegisterLegOrder: Leg type={legType}, " +
                $"AlpacaId={alpacaLeg.OrderId}, LeanOrderType={leanOrder.Type}, " +
                $"GroupId={bracketProps.BracketGroupId}");

            // Tag the LEAN order with bracket properties
            var legProps = new AlpacaBracketOrderProperties
            {
                BracketGroupId = bracketProps.BracketGroupId,
                LegType = legType
            };
            leanOrder.Properties = legProps;

            // Register in the bracket leg mapping for trade update routing
            _bracketLegMapping[alpacaLeg.OrderId] = new BracketLegInfo
            {
                BracketGroupId = bracketProps.BracketGroupId,
                AlpacaLegOrderId = alpacaLeg.OrderId,
                LegType = legType,
            };

            Log.Debug($"{nameof(AlpacaBrokerage)}.CreateAndRegisterLegOrder: Registered leg in _bracketLegMapping. " +
                $"AlpacaId={alpacaLeg.OrderId}, GroupId={bracketProps.BracketGroupId}, Type={legType}");

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
                    case TradeEvent.PendingNew:
                        // we don't send anything for this event
                        _duplicationExecutionOrderIdByBrokerageOrderId.TryAdd(obj.Order.OrderId, []);
                        return;
                    case TradeEvent.Rejected:
                    case TradeEvent.Canceled:
                    case TradeEvent.Replaced:
                    case TradeEvent.Expired:
                        if (_duplicationExecutionOrderIdByBrokerageOrderId.Remove(obj.Order.OrderId))
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
                            OnOrderEvent(statusEvent);

                            // Forward cancel/reject/expire events to BracketOrderManager for state tracking
                            if (isBracketLeg || _bracketManager?.GetGroupIdForOrder(leanOrder.Id) != null)
                            {
                                _bracketManager?.ProcessOrderEvent(statusEvent);
                            }
                        }
                        return;
                    case TradeEvent.Fill:
                        if (_duplicationExecutionOrderIdByBrokerageOrderId.Remove(obj.Order.OrderId))
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
                        if (_duplicationExecutionOrderIdByBrokerageOrderId[obj.Order.OrderId].Add(obj.ExecutionId.Value))
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

                // if we filled the order and have another contingent order waiting, submit it
                if (!TryHandleRemainingCrossZeroOrder(leanOrder, orderEvent))
                {
                    OnOrderEvent(orderEvent);
                }

                // Forward fill/partial fill events to BracketOrderManager for state tracking.
                // This must happen after OnOrderEvent so the order status is updated in LEAN first.
                if (isBracketLeg || _bracketManager?.GetGroupIdForOrder(leanOrder.Id) != null)
                {
                    _bracketManager?.ProcessOrderEvent(orderEvent);
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
                                // resubscribe
                                var symbols = _subscriptionManager.GetSubscribedSymbols();
                                Unsubscribe(symbols);
                                Subscribe(symbols);
                            }
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
            _orderStreamingClient?.DisconnectAsync()?.SynchronouslyAwaitTask();
            _equityStreamingClient?.DisconnectAsync()?.SynchronouslyAwaitTask();
            _cryptoStreamingClient?.DisconnectAsync()?.SynchronouslyAwaitTask();
            _optionsStreamingClient?.DisconnectAsync()?.SynchronouslyAwaitTask();
        }

        public override void Dispose()
        {
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