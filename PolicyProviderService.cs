#nullable disable

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using EYMP.Services.Equity.API.Helpers;
using EYMP.Services.Equity.API.Models.AppSetting;
using EYMP.Services.Equity.API.Services.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Fallback;
using Polly.Retry;
using Polly.Timeout;
using Polly.Wrap;

namespace EYMP.Services.Equity.API.Services
{
    using System.Threading.Tasks;

    public class PolicyProviderService : IPolicyProviderService
    {
        private readonly IOptions<PolicyConfig> policy;

        private readonly List<HttpStatusCode> httpStatusCodesWorthRetrying = new List<HttpStatusCode>()
                {
                   HttpStatusCode.RequestTimeout, // 408
                   HttpStatusCode.InternalServerError, // 500
                   HttpStatusCode.BadGateway, // 502
                   HttpStatusCode.ServiceUnavailable, // 503
                   HttpStatusCode.GatewayTimeout // 504
                };

        public PolicyProviderService(IOptions<PolicyConfig> policy)
        {
            this.policy = Validator.ValidateIsNotNull(policy, nameof(policy));
        }

        public async Task<T> ExecuteAsync<T, TException, TLogger>(
            Func<Context, Task<T>> action,
            ILogger<TLogger> logger,
            Context ctx) where TException : Exception
        {
            var retryWithFallBackAndTimeout = CreatePolicy<T, TException, TLogger>(logger);
            return await retryWithFallBackAndTimeout.ExecuteAsync(action, ctx).ConfigureAwait(false);
        }

        private AsyncPolicyWrap<T> CreatePolicy<T, TException, TLogger>(ILogger<TLogger> logger) where TException : Exception
        {
            var retryPolicy = GetRetryPolicyAsync<T, TException, TLogger>(logger);
            var timeoutPolicy = GetTimeOutPolicyAsync();
            var fallBackPolicy = GetFallBackPolicy<T, TException, TLogger>(logger);
            var retryWithTimeout = retryPolicy.WrapAsync(timeoutPolicy);

            var retryWithFallBackAndTimeout = fallBackPolicy.WrapAsync(retryWithTimeout);
            return retryWithFallBackAndTimeout;
        }

        private AsyncRetryPolicy<T> GetRetryPolicyAsync<T, TException, TLogger>(
            ILogger<TLogger> logger) where TException : Exception
        {
            var builder = CreatePolicyBuilder<T, TException>();

            return builder.WaitAndRetryAsync(
                policy.Value.MaxRetriesCount,
                times => TimeSpan.FromSeconds(times),
                onRetryAsync: async (result, delay, retryCount, context) =>
                {
                    var logMessage = BuildLogMessage<T, TLogger>(result, retryCount, context);

                    logger.LogEASInfo("", logMessage);

                    await Task.FromResult(result).ConfigureAwait(false);
                });
        }

        private string BuildLogMessage<T, TLogger>(DelegateResult<T> result, int retryCount, Context context)
        {
            (string methodName, string logMessage, string logDetails) log;

            log.methodName = (string)context["methodName"];

            if (typeof(T) != typeof(HttpResponseMessage))
            {
                log.logMessage = result.Exception.Message;
                log.logDetails = result.Exception.ToString();
            }
            else
            {
                var r = result.Result as HttpResponseMessage;
                log.logMessage = r is null ? result.Exception.Message : r.StatusCode.ToString();
                log.logDetails = r is null ? result.Exception.ToString() : r.ReasonPhrase;
            }

            return retryCount <= policy.Value.MaxRetriesCount
                ? ResourceHelper.GetResourceString(
                    ResourceHelper.EASPolicyLogRetryMessage,
                    typeof(TLogger),
                    log.methodName,
                    retryCount,
                    log.logMessage,
                    log.logDetails)
                : ResourceHelper.GetResourceString(
                    ResourceHelper.EASPolicyLogFallbackMessage,
                    typeof(TLogger),
                    log.methodName,
                    log.logMessage,
                    log.logDetails);
        }

        private PolicyBuilder<T> CreatePolicyBuilder<T, TException>() where TException : Exception
        {
            var exception = typeof(TException);
            PolicyBuilder<T> builder = Policy<T>
                .Handle<TException>();

            if (exception == typeof(HttpRequestException))
            {
                builder.OrResult(r =>
                {
                    var x = r as HttpResponseMessage;
                    return httpStatusCodesWorthRetrying.Contains(x.StatusCode) || x.Headers == null;
                });
            }

            return builder;
        }

        private AsyncTimeoutPolicy GetTimeOutPolicyAsync()
        {
            return
                Policy.TimeoutAsync(policy.Value.DefaultTimeout,
                TimeoutStrategy.Optimistic);
        }

        private AsyncFallbackPolicy<T> GetFallBackPolicy<T, TException, TLogger>(
            ILogger<TLogger> logger) where TException : Exception
        {
            var builder = CreatePolicyBuilder<T, TException>();

            return builder
                .FallbackAsync(
                    fallbackAction: async (result, context, token) =>
                    {
                        await Task.FromResult(result.Result).ConfigureAwait(false);

                        throw result.Result is null
                            ? result.Exception
                            : ResponseGetter.ThrowInvalidOperation(logger, context["methodName"].ToString(), result.Result.ToString());
                    },
                    onFallbackAsync: async (result, context) =>
                    {
                        //add 1 to maxretry to indicate that all retries were used
                        var retriesMaxOut = policy.Value.MaxRetriesCount + 1;
                        var logMessage = BuildLogMessage<T, TLogger>(result, retriesMaxOut, context);

                        logger.LogEASError("", logMessage);
                        await Task.CompletedTask.ConfigureAwait(false);
                    });
        }
    }
}
