#nullable disable
using System.Diagnostics.CodeAnalysis;
using Microsoft.Azure.ServiceBus;

namespace EYMP.Services.Equity.API.Subscribers
{  
    [ExcludeFromCodeCoverage]
    public class StockUpdateSubscriptionClient : SubscriptionClient, IStockUpdateSubscriptionClient
    {
        public StockUpdateSubscriptionClient(string connectionString,
            string topicPath, 
            string subscriptionName, 
            ReceiveMode receiveMode = ReceiveMode.PeekLock, 
            RetryPolicy retryPolicy = null) : base(connectionString, topicPath, subscriptionName, receiveMode, retryPolicy)
        {
        }
    }
}
