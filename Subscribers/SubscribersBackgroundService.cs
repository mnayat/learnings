using System.Threading;
using EYMP.Services.Equity.API.Helpers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace EYMP.Services.Equity.API.Subscribers
{
    using System.Threading.Tasks;

    public class SubscribersBackgroundService : BackgroundService
    {
        private readonly IStockUpdateSubscriber stockUpdateSubscriber;
        private readonly IServiceScopeFactory serviceScopeFactory;

        public SubscribersBackgroundService(IStockUpdateSubscriber stockUpdateSubscriber, IServiceScopeFactory serviceScopeFactory)
        {
            this.stockUpdateSubscriber = Validator.ValidateIsNotNull(stockUpdateSubscriber, nameof(stockUpdateSubscriber));
            this.serviceScopeFactory = Validator.ValidateIsNotNull(serviceScopeFactory, nameof(serviceScopeFactory));
        }

        protected override async Task ExecuteAsync(CancellationToken _ )
        {
            IServiceScope scope = serviceScopeFactory.CreateScope();
            ITripLegSubscriber? tripLegSubscriber = scope.ServiceProvider.GetRequiredService<ITripLegSubscriber>();

            await tripLegSubscriber.Register().ConfigureAwait(false);
            await stockUpdateSubscriber.Register().ConfigureAwait(false);
        }
    }
}
