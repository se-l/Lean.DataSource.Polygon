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

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Interface for subscription managers to handle the subscriptions for the Polygon data queue handler.
    /// </summary>
    public interface IPolygonSubscriptionManager : IDisposable
    {
        /// <summary>
        /// Indicates whether data is being streamed using aggregates or ticks
        /// </summary>
        bool UsingAggregates { get; }

        /// <summary>
        /// Whether or not there is at least one open connection
        /// </summary>
        bool IsConnected { get; }

        /// <summary>
        /// Subscribes the specified configuration to the data feed
        /// </summary>
        /// <param name="config">The subscription data configuration to subscribe</param>
        void Subscribe(SubscriptionDataConfig config);

        /// <summary>
        /// Unsubscribes the specified configuration from the data feed
        /// </summary>
        /// <param name="config">The subscription data configuration to unsubscribe</param>
        void Unsubscribe(SubscriptionDataConfig config);
    }
}
