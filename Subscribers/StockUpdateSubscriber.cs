#nullable disable

using System;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Threading;
using EYMP.Services.Equity.API.Helpers;
using EYMP.Services.Equity.API.Models.Equity;
using EYMP.Services.Equity.API.Models.IntegrationService;
using EYMP.Services.Equity.API.Services.Interfaces;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;

namespace EYMP.Services.Equity.API.Subscribers
{
    using System.Text.Json;
    using System.Threading.Tasks;
    using EYMP.Services.Equity.Domain.Exceptions;

    /// <summary>
    /// This will receive Equity Data message from the integration service which we process the
    /// submit calculation in the EY GOES
    /// </summary>
    public class StockUpdateSubscriber : IStockUpdateSubscriber
    {
        private readonly IEYGoesService eyGoesService;
        private readonly ILogger<StockUpdateSubscriber> logger;
        private readonly IStockUpdateSubscriptionClient subscriptionClient;

        public StockUpdateSubscriber(
            IEYGoesService eyGoesService,
            IStockUpdateSubscriptionClient subscriptionClient,
            ILogger<StockUpdateSubscriber> logger)
        {
            this.eyGoesService = Validator.ValidateIsNotNull(eyGoesService, nameof(eyGoesService));
            this.subscriptionClient = Validator.ValidateIsNotNull(subscriptionClient, nameof(subscriptionClient));
            this.logger = Validator.ValidateIsNotNull(logger, nameof(logger));
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

        [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "used for logging")]
        public async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            Validator.ValidateIsNotNull(message, nameof(message));

            string client = message.UserProperties["Client"] as string;
            try
            {
                logger.LogEASInfo(
                    ResourceHelper.DeserializeMessage,
                    nameof(StockUpdate));

                StockUpdate stockUpdate = null;
                string messageBody = null;

                try
                {
                    messageBody = Encoding.UTF8.GetString(message.Body);
                    stockUpdate =
                        JsonHelper
                        .Deserialize<StockUpdate>(messageBody);
                }
                catch (FormatException exception)
                {
                    await LogStockUpdateSubscriberError(
                        message,
                        messageBody,
                        exception)
                        .ConfigureAwait(false);
                    return;
                }
                catch (JsonException exception)
                {
                    await LogStockUpdateSubscriberError(
                        message,
                        messageBody,
                        exception)
                        .ConfigureAwait(false);
                    return;
                }

                logger.LogEASInfo(
                    ResourceHelper.StockUpdateSubscriberStartCalculateEquity,
                    stockUpdate.ToString());

                if (!int.TryParse(client, out int clientId))
                {
                    logger.LogEASError(
                        ResourceHelper.StockUpdateSubscriberInvalidClient,
                        Validator.CreateArgumentOutOfRangeException(client),
                        $"Client:{client}");
                    throw Validator.CreateArgumentOutOfRangeException(client);
                }

                try
                {
                     await eyGoesService
                         .CalculateEquityAsync(stockUpdate, clientId)
                         .ConfigureAwait(false);
                    // Complete the message so that it is not received again. This can be done only
                    // if the subscriptionClient is created in ReceiveMode.PeekLock mode (which is
                    // the default).

                    //Complete the message after EAS processed it successfully, if not then it will be automatically move to DeadLetter Queue(DLQ)
                    await subscriptionClient
                        .CompleteAsync(GetLockToken(message))
                        .ConfigureAwait(false);
                }
                catch (EYGoesCalculateEquityTransactionResponseErrorException ex)
                {
                    logger.LogEASWarning("",
                        ex.Message,
                        string.Concat(
                            "Message ID: ",
                            message.MessageId));

                    await SendToDeadLetterAsync(message)
                        .ConfigureAwait(false);
                }
            }
            catch (Exception exception)
            {
                logger.LogEASError(
                    ResourceHelper.StockUpdateMessageProcessingErrorMessage,
                    exception,
                    nameof(StockUpdateSubscriber),
                    nameof(ProcessMessagesAsync),
                    exception.Message,
                    exception.StackTrace);

                await SendToDeadLetterAsync(message)
                    .ConfigureAwait(false);
            }
        }

        private static string GetLockToken(Message msg) =>
            // msg.SystemProperties.LockToken Get property throws exception if not set. Return null instead.
            msg.SystemProperties == null
                ? null
                : msg.SystemProperties.IsLockTokenSet
                    ? msg.SystemProperties.LockToken
                    : null;

        private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            logger.LogEASError(
                ResourceHelper.StockUpdateMessageProcessingErrorMessage,
                exceptionReceivedEventArgs,
                nameof(StockUpdateSubscriber),
                nameof(ProcessMessagesAsync),
                exceptionReceivedEventArgs.Exception.Message);

            return Task.CompletedTask;
        }

        private async Task LogStockUpdateSubscriberError(
            Message message,
            string messageBody,
            Exception exception)
        {
            logger.LogEASError(
                ResourceHelper.StockUpdateSubscriberDeserializationFail,
                exception,
                nameof(StockUpdateSubscriber),
                nameof(ProcessMessagesAsync),
                $"Stock Update Message: {messageBody}",
                exception.Message,
                exception.StackTrace
                );

            await SendToDeadLetterAsync(message)
                    .ConfigureAwait(false);
        }

        private async Task SendToDeadLetterAsync(Message message)
            => await subscriptionClient
                .DeadLetterAsync(GetLockToken(message))
                .ConfigureAwait(false);
    }
}
