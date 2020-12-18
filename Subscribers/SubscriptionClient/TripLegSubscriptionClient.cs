#nullable disable
using System.Diagnostics.CodeAnalysis;
using Microsoft.Azure.ServiceBus;
namespace EYMP.Services.Equity.API.Subscribers
{    
    public class TripLegSubscriptionClient : SubscriptionClient, ITripLegSubscriptionClient
    {       
        [ExcludeFromCodeCoverage]
        public TripLegSubscriptionClient(string connectionString,
            string topicPath,
            string subscriptionName,
            ReceiveMode receiveMode = ReceiveMode.PeekLock,
            RetryPolicy retryPolicy = null) : base(connectionString, topicPath, subscriptionName, receiveMode, retryPolicy)
        {
        }
    }
    public interface ITripLegSubscriptionClient : ISubscriptionClient { }
}
