/*
 * Alpaca Brokerage Message Handler
 *
 * Extends the default handler to accept bracket leg orders created by
 * the Alpaca brokerage via OnNewBrokerageOrderNotification.
 *
 * When a bracket order is placed, Alpaca returns the entry + leg orders
 * atomically. The brokerage creates phantom LEAN orders for the legs
 * and fires OnNewBrokerageOrderNotification so LEAN's engine tracks them.
 * The default handler rejects these as "outside Lean" orders — this
 * handler recognizes them by their AlpacaBracketOrderProperties and
 * accepts them.
 */

using QuantConnect.Api;
using QuantConnect.Interfaces;
using QuantConnect.Packets;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Custom brokerage message handler for Alpaca that accepts bracket leg orders
    /// </summary>
    public class AlpacaBrokerageMessageHandler : DefaultBrokerageMessageHandler
    {
        /// <summary>
        /// Creates a new instance
        /// </summary>
        public AlpacaBrokerageMessageHandler(IAlgorithm algorithm, AlgorithmNodePacket job, IApi api)
            : base(algorithm, job, api)
        {
        }

        /// <summary>
        /// Handles a new order notification from the brokerage.
        /// Returns true for bracket leg orders so LEAN's engine registers them.
        /// Delegates all other orders to the default handler (which rejects them).
        /// </summary>
        public override bool HandleOrder(NewBrokerageOrderNotificationEventArgs eventArgs)
        {
            if (eventArgs.Order.Properties is AlpacaBracketOrderProperties bracketProps
                && bracketProps.LegType.HasValue)
            {
                return true;
            }

            return base.HandleOrder(eventArgs);
        }
    }
}
