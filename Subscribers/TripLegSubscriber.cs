#nullable disable

using System;
using System.Text;
using System.Threading;
using EYMP.Services.Equity.API.Helpers;
using EYMP.Services.Equity.API.Models;
using EYMP.Services.Equity.API.Models.TriplegSubscriber;
using EYMP.Services.Equity.API.Services.Interfaces;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;

namespace EYMP.Services.Equity.API.Subscribers
{
    using System.Diagnostics.CodeAnalysis;
    using System.Text.Json;
    using System.Threading.Tasks;
    using EYMP.Services.Equity.Domain;
    using Microsoft.Data.SqlClient;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.DependencyInjection;

    /// <summary>
    /// The TripLegSubscriber process a Tripleg Message and will send a Stock Update Request for
    /// Newly Eligible Employee
    /// </summary>
    public class TripLegSubscriber : ITripLegSubscriber, IDisposable
    {
        private readonly IIntegrationService integrationService;
        private readonly IEligibilityService eligibilityService;
        private readonly ILogger<TripLegSubscriber> logger;
        private readonly ITripLegSubscriptionClient subscriptionClient;
        private readonly IServiceScopeFactory serviceScopeFactory;
        private bool disposedValue;

        /// <summary>
        /// TripLegSubscriber Constructor
        /// </summary>
        /// <param name="integrationService">The Integration Service will send Stock Update</param>
        /// <param name="eligibilityService">
        /// The Eligibility Service will add Eligible Employee and Extend Effectivity End Date
        /// </param>
        /// <param name="subscriptionClient">The Subscription Client</param>
        /// <param name="logger">the Logger</param>
        /// <param name="serviceScopeFactory"></param>
        public TripLegSubscriber(
            IIntegrationService integrationService,
            IEligibilityService eligibilityService,
            ITripLegSubscriptionClient subscriptionClient,
            ILogger<TripLegSubscriber> logger,
            IServiceScopeFactory serviceScopeFactory)
        {
            this.integrationService = Validator
                .ValidateIsNotNull(
                    integrationService,
                    nameof(integrationService));

            this.eligibilityService = Validator
                .ValidateIsNotNull(
                    eligibilityService,
                    nameof(eligibilityService));

            this.subscriptionClient = Validator
                .ValidateIsNotNull(
                    subscriptionClient,
                    nameof(subscriptionClient));

            this.logger = Validator
                .ValidateIsNotNull(
                    logger,
                    nameof(logger));

            this.serviceScopeFactory = Validator
                .ValidateIsNotNull(
                    serviceScopeFactory,
                    nameof(serviceScopeFactory));
        }

        public Task Register()
        {
            RegisterOnMessageHandlerAndReceiveMessages();
            return Task.CompletedTask;
        }

        private void RegisterOnMessageHandlerAndReceiveMessages()
        {
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of concurrent calls to the callback ProcessMessagesAsync(), set to
                // 1 for simplicity. Set it according to how many messages the application wants to
                // process in parallel.
                MaxConcurrentCalls = 1,

                // Indicates whether MessagePump should automatically complete the messages after
                // returning from User Callback. False below indicates the Complete will be handled
                // by the User Callback as in `ProcessMessagesAsync` below.
                AutoComplete = false
            };

            // Register the function that processes messages.
            subscriptionClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        public async Task ProcessMessagesAsync(Message message, CancellationToken _)
        {
            Validator.ValidateIsNotNull(message, nameof(message));

            logger.LogEASInfo(
                    ResourceHelper.DeserializeMessage,
                    nameof(TripLegSubscriber));

            TripLeg tripLeg =
                    await DeserializeMessageToTripLegAsync(message).ConfigureAwait(false);

            if (tripLeg == null)
                return;

            logger.LogEASInfo("",
                "Tripleg Message",
                tripLeg.ToString());

            Validator.ValidateIsNotNull(tripLeg.ClientId, nameof(tripLeg.ClientId));

            await ProcessTriplegData(tripLeg, message).ConfigureAwait(false);

            // Complete the message so that it is not received again. This can be done only if the
            // subscriptionClient is created in ReceiveMode.PeekLock mode (which is the default).

            //Complete the message after EAS processed it successful, if not then it will be automatically move to DeadLetter Queue(DLQ)
            await subscriptionClient
                .CompleteAsync(GetLockToken(message))
                .ConfigureAwait(false);
        }

        private async Task<TripLeg> DeserializeMessageToTripLegAsync(Message message)
        {
            string messageBody = null;
            try
            {
                messageBody = Encoding.UTF8.GetString(message.Body);
                TripLeg tripleg = JsonHelper
                    .Deserialize<TripLeg>(messageBody);
                return tripleg;
            }
            catch (FormatException exception)
            {
                //log error and stop processing of trip leg message
                await LogTriplegSubscriberError(
                        message,
                        messageBody,
                        exception).ConfigureAwait(false);
                return null;
            }
            catch (JsonException exception)
            {
                //log error and stop processing of trip leg message
                await LogTriplegSubscriberError(
                        message,
                        messageBody,
                        exception).ConfigureAwait(false);
                return null;
            }
        }

        private async Task LogTriplegSubscriberError(
            Message message,
            string messageBody,
            Exception exception)
        {
            logger.LogEASError(
                ResourceHelper.TripLegSubscriberDeserializationFail,
                exception,
                "MessageId: " + message.MessageId.ToString(),
                "Trip Leg Message: " + messageBody);

            await SendToDeadLetterAsync(message)
                .ConfigureAwait(false);
        }

        [SuppressMessage("Design", "CA1031:Do not catch general exception types",
            Justification = "used for logging")]
        private async Task ProcessTriplegData(TripLeg tripLeg, Message message)
        {
            using (IServiceScope scope = serviceScopeFactory.CreateScope())
            {
                // Creates an instance of eligibilityService for each Processing of tripleg
                using (IEligibilityService eligibilityServiceInstance = scope.ServiceProvider.GetRequiredService<IEligibilityService>())
                {
                    try
                    {
                        var eligibility = await GetEligibleEmployeeAsync(
                                tripLeg,
                                eligibilityServiceInstance).ConfigureAwait(false);

                        if (eligibility == null)
                        {
                            //add eligibility
                            await AddNewEligibleEmployeeAsync(
                                tripLeg,
                                eligibility,
                                eligibilityServiceInstance).ConfigureAwait(false);
                        }
                        else
                        {
                            // extend eligibility period
                            await ExtendEligibilityPeriodAsync(
                                    tripLeg,
                                    eligibility).ConfigureAwait(false);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogEASError(
                            ResourceHelper.IntegrationRequestStockUpdateErrorMessage,
                            ex,
                            "Integration Service",
                            "RequestStockUpdate",
                            "MessageId: " + message.MessageId.ToString());                          

                        eligibilityServiceInstance.Rollback();

                        await SendToDeadLetterAsync(message)
                            .ConfigureAwait(false);
                    }
                }
            }
        }

        private async Task SendToDeadLetterAsync(Message message)
            => await subscriptionClient
                .DeadLetterAsync(GetLockToken(message))
                .ConfigureAwait(false);

        private static async Task<Eligibility> GetEligibleEmployeeAsync(
            TripLeg tripLeg,
            IEligibilityService eligibilityService) => await eligibilityService
            .GetEligibleEmployeeAsync(
                tripLeg.ClientId.Value,
                tripLeg.EmployeeCode).ConfigureAwait(false);

        private async Task ExtendEligibilityPeriodAsync(TripLeg tripLeg, Eligibility eligibility)
        {
            using (IServiceScope scope = serviceScopeFactory.CreateScope())
            {
                // create a new instance of eligibilityService
                using (IEligibilityService eligibilityServiceUpdate = scope.ServiceProvider.GetRequiredService<IEligibilityService>())
                {
                    eligibility = eligibility ??
                        await GetEligibleEmployeeAsync(
                           tripLeg,
                           eligibilityServiceUpdate).ConfigureAwait(false);

                    //Extend eligibility period
                    eligibilityServiceUpdate.BeginTransaction();
                    await eligibilityServiceUpdate.ExtendEligibilityPeriodAsync(
                       eligibility,
                       tripLeg.ArrivalDt).ConfigureAwait(false);
                    eligibilityServiceUpdate.Commit();
                }
            }
        }

        private async Task RequestStockUpdateAsync(TripLeg tripLeg)
        {
            // N.B.: changed from tripLeg.PersonId (internal employee ID) to tripLeg.EmployeeCode
            // (external employee ID)
            var requestStockUpdatePayload =
                   new RequestStockUpdate()
                   {
                       PersonnelNumber = tripLeg.EmployeeCode,
                       FromDate = $"{DateTime.Now.Year}-01-01"
                   };
            //request stock update message
            await integrationService
                    .RequestStockUpdateAsync(
                        StringHelper.ToInvariantCulture(tripLeg.ClientId.Value),
                        requestStockUpdatePayload).ConfigureAwait(false);
        }

        private async Task AddNewEligibleEmployeeAsync(
            TripLeg tripLeg,
            Eligibility eligibility,
            IEligibilityService eligibilityService)
        {
            try
            {
                eligibilityService.BeginTransaction();
                _ = await eligibilityService
                            .AddEligibleEmployeeAsync(
                                tripLeg.ClientId.Value,
                                tripLeg.EmployeeCode,
                                tripLeg.ArrivalDt).ConfigureAwait(false);

                //request stock update for newly eligible employee
                await RequestStockUpdateAsync(tripLeg).ConfigureAwait(false);
                eligibilityService.Commit();
            }
            catch (DbUpdateException ex)
            {
                // Violation of primary key. Handle Exception
                if ((ex.InnerException as SqlException)?.Number == 2627)
                {
                    eligibilityService.Rollback();

                    logger.LogEASInfo(
                        ResourceHelper.TriplegSubscriberAddEligibilityRaceCondition,
                        tripLeg.ToString());

                    await ExtendEligibilityPeriodAsync(
                            tripLeg,
                            eligibility).ConfigureAwait(false);
                }
                else
                {
                    // Throw other exception
                    throw;
                }
            }
        }

        private static string GetLockToken(Message message)
        {
            // msg.SystemProperties.LockToken Get property throws exception if not set. Return null instead.
            var systemProperties = message?.SystemProperties;

            return systemProperties?.IsLockTokenSet ?? false
                ? systemProperties.LockToken
                : null;
        }

        private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            logger.LogEASError(
                ResourceHelper.TripLegMessageProcessingErrorMessage,
                exceptionReceivedEventArgs.Exception);

            eligibilityService.Rollback();

            return Task.CompletedTask;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    eligibilityService.Dispose();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
