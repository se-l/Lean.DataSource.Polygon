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

using System.Text;
using System.Collections.Concurrent;
using System.Linq;
using Newtonsoft.Json;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Subscription manager to handle the subscriptions for the Polygon data queue handler via RabbitMQ.
    /// </summary>
    public class RabbitMQSubscriptionManager : IPolygonSubscriptionManager
    {
        private readonly string _mqHost;
        private readonly string _mqVirtualHost;
        private readonly string _mqUser;
        private readonly string _mqPass;
        private readonly string _subscriptionQueue;
        private readonly string _publishExchange;

        private IConnection _connection;
        private IChannel _channel;
        private string _consumerTag;

        private readonly PolygonSymbolMapper _symbolMapper;
        private readonly Action<string> _onMessage;
        private readonly object _lock = new();
        private bool _disposed;
        private string _dataQueueName;
        private bool _optionTradeSubscriptionsEnabled;

        /// <summary>
        /// Holds the active subscription configurations, keyed by tick type and symbol.
        /// </summary>
        private readonly ConcurrentDictionary<(TickType TickType, Symbol Symbol), SubscriptionDataConfig> _activeSubscriptions = new();

        public bool IsConnected => _connection != null && _connection.IsOpen;

        /// <summary>
        /// Indicates whether data is being streamed using aggregates or ticks.
        /// RabbitMQ implementation currently only supports ticks for trades and quotes.
        /// </summary>
        public bool UsingAggregates => false;

        public RabbitMQSubscriptionManager(PolygonSymbolMapper symbolMapper, Action<string> onMessage)
        {
            _symbolMapper = symbolMapper;
            _onMessage = onMessage;

            _mqHost = Config.Get("MQHost", "localhost:5672");
            _mqVirtualHost = Config.Get("MQVirtualHostMassiveBroker", "/");
            _mqUser = Config.Get("MQUser", "guest");
            _mqPass = Config.Get("MQPass", "guest");
            _subscriptionQueue = Config.Get("broker-subscription-queue", "massive.subscriptions");
            _publishExchange = Config.Get("broker-publish-exchange", "massive.marketdata");
            _optionTradeSubscriptionsEnabled = bool.Parse(Config.Get("option-trade-subscriptions-enabled", "false"));

            Connect();
        }

        private void Connect()
        {
            lock (_lock)
            {
                if (IsConnected) return;

                var factory = new ConnectionFactory
                {
                    HostName = _mqHost.Split(':')[0],
                    Port = _mqHost.Contains(':') ? int.Parse(_mqHost.Split(':')[1]) : 5672,
                    VirtualHost = _mqVirtualHost,
                    UserName = _mqUser,
                    Password = _mqPass,
                    AutomaticRecoveryEnabled = true
                };

                try
                {
                    _connection = factory.CreateConnectionAsync("QC.DataSource.Polygon").GetAwaiter().GetResult();
                    _channel = _connection.CreateChannelAsync().GetAwaiter().GetResult();

                    // Declare the exchange just in case
                    _channel.ExchangeDeclareAsync(exchange: _publishExchange, type: ExchangeType.Topic, durable: true).GetAwaiter().GetResult();

                    // Create a temporary queue for this instance to consume data
                    var queueDeclareResult = _channel.QueueDeclareAsync().GetAwaiter().GetResult();
                    var queueName = queueDeclareResult.QueueName;

                    var consumer = new AsyncEventingBasicConsumer(_channel);
                    consumer.ReceivedAsync += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        _onMessage(message);
                        return Task.CompletedTask;
                    };

                    _consumerTag = _channel.BasicConsumeAsync(queue: queueName, autoAck: true, consumer: consumer).GetAwaiter().GetResult();
                    
                    // Store the queue name to bind it later during subscription
                    _dataQueueName = queueName;

                    Log.Trace($"RabbitMQSubscriptionManager.Connect(): Connected to {_mqHost}");
                }
                catch (Exception ex)
                {
                    Log.Error($"RabbitMQSubscriptionManager.Connect(): Failed to connect to {_mqHost}: {ex.Message}");
                    throw;
                }
            }
        }

        public void Subscribe(SubscriptionDataConfig config)
        {
            if (config.Symbol.SecurityType == SecurityType.Option && config.TickType == TickType.Trade && !_optionTradeSubscriptionsEnabled)
            {
                Log.Trace($"RabbitMQSubscriptionManager.Subscribe(): Skip subscription for Trade Subscription {config.Symbol.Value}");
                return;
            }

            _activeSubscriptions[(config.TickType, config.Symbol)] = config;
            SendSubscriptionRequest(config, "subscribe");
            BindToRoutingKey(config);
        }

        public void Unsubscribe(SubscriptionDataConfig config)
        {
            _activeSubscriptions.TryRemove((config.TickType, config.Symbol), out _);
            SendSubscriptionRequest(config, "unsubscribe");
            UnbindFromRoutingKey(config);
        }

        /// <summary>
        /// Retrieves the list of unique <see cref="Symbol"/> instances that are currently subscribed for a specific <see cref="TickType"/>.
        /// </summary>
        /// <param name="tickType">The type of tick data to filter subscriptions by.</param>
        /// <returns>A collection of unique <see cref="Symbol"/> objects that match the specified <paramref name="tickType"/>.</returns>
        public IEnumerable<Symbol> GetSubscribedSymbols(TickType tickType)
        {
            return _activeSubscriptions.Keys
                .Where(subscription => subscription.TickType == tickType)
                .Select(subscription => subscription.Symbol)
                .Distinct();
        }

        private void SendSubscriptionRequest(SubscriptionDataConfig config, string action)
        {
            var symbol = _symbolMapper.GetBrokerageSymbol(config.Symbol);
            var market = GetMarket(config.Symbol.SecurityType);
            var topic = GetTopic(config.TickType);

            var request = new
            {
                action = action,
                market = market,
                topic = topic,
                symbol = symbol
            };

            var message = JsonConvert.SerializeObject(request);
            var body = Encoding.UTF8.GetBytes(message);

            lock (_lock)
            {
                if (!IsConnected) Connect();
                _channel.BasicPublishAsync(exchange: string.Empty, routingKey: _subscriptionQueue, body: body).GetAwaiter().GetResult();
            }

            Log.Trace($"RabbitMQSubscriptionManager.SendSubscriptionRequest(): {action} {market}.{topic}.{symbol}");
        }

        private void BindToRoutingKey(SubscriptionDataConfig config)
        {
            var routingKey = GetRoutingKey(config);
            lock (_lock)
            {
                if (!IsConnected) Connect();
                _channel.QueueBindAsync(queue: _dataQueueName, exchange: _publishExchange, routingKey: routingKey).GetAwaiter().GetResult();
            }
            Log.Trace($"RabbitMQSubscriptionManager.BindToRoutingKey(): Bound to {routingKey}");
        }

        private void UnbindFromRoutingKey(SubscriptionDataConfig config)
        {
            var routingKey = GetRoutingKey(config);
            lock (_lock)
            {
                if (IsConnected)
                {
                    _channel.QueueUnbindAsync(queue: _dataQueueName, exchange: _publishExchange, routingKey: routingKey).GetAwaiter().GetResult();
                }
            }
            Log.Trace($"RabbitMQSubscriptionManager.UnbindFromRoutingKey(): Unbound from {routingKey}");
        }

        private string GetRoutingKey(SubscriptionDataConfig config)
        {
            var symbol = _symbolMapper.GetBrokerageSymbol(config.Symbol);
            var market = GetMarket(config.Symbol.SecurityType);
            var topic = GetTopic(config.TickType);
            
            // Format: {market}.{topic}.{symbol}
            return $"{market}.{topic}.{symbol}";
        }

        private string GetMarket(SecurityType securityType)
        {
            return securityType == SecurityType.Option || securityType == SecurityType.IndexOption ? "options" : "stocks";
        }

        private string GetTopic(TickType tickType)
        {
            return tickType switch
            {
                TickType.Trade => "trades",
                TickType.Quote => "quotes",
                _ => "aggs"
            };
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (_disposed) return;
                _disposed = true;

                if (_channel != null)
                {
                    if (!string.IsNullOrEmpty(_consumerTag))
                    {
                        _channel.BasicCancelAsync(_consumerTag).GetAwaiter().GetResult();
                    }
                    _channel.CloseAsync().GetAwaiter().GetResult();
                    _channel.Dispose();
                }

                if (_connection != null)
                {
                    _connection.CloseAsync().GetAwaiter().GetResult();
                    _connection.Dispose();
                }
            }
        }
    }
}
