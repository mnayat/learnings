#nullable disable
using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using EYMP.Services.Equity.API.Helpers;
using EYMP.Services.Equity.API.Services.Interfaces;
using Microsoft.Extensions.Logging;
using Polly;

namespace EYMP.Services.Equity.API.Services
{
    public class HttpRequestService : IHttpRequestService
    {
        private readonly IHttpClientFactory httpClientFactory;
        private readonly ILogger<HttpRequestService> logger;
        private readonly IPolicyProviderService policyProviderService;
        private readonly ITokenService tokenService;

        public HttpRequestService(IHttpClientFactory httpClientFactory,
           ILogger<HttpRequestService> logger,
           IPolicyProviderService policyProviderService,
           ITokenService tokenService)
        {
            this.logger = Validator.ValidateIsNotNull(logger, nameof(logger));
            this.httpClientFactory = Validator.ValidateIsNotNull(httpClientFactory, nameof(httpClientFactory)); ;
            this.policyProviderService = Validator.ValidateIsNotNull(policyProviderService, nameof(policyProviderService)); ;
            this.tokenService = Validator.ValidateIsNotNull(tokenService, nameof(tokenService));
        }

        public async Task<T> GetRequestWithPolly<T>(Uri requestUri, string methodName, Action<T> successful = null)
        {
            var context = new Context();
            var jwtToken = await tokenService.AcquireJwtToken().ConfigureAwait(false);
            var tokenRetryCount = 0;
            while (true)
            {
                var response = await policyProviderService
                    .ExecuteAsync<HttpResponseMessage, HttpRequestException, HttpRequestService>(
                        async _ =>
                        {
                            context["methodName"] = methodName;
                            return await ResponseGetter
                                .GetResponse(
                                    httpClientFactory.CreateClient(),
                                    HttpMethod.Get,
                                    requestUri,
                                    jwtToken)
                                .ConfigureAwait(false);
                        },
                        logger,
                        context)
                    .ConfigureAwait(false);

                if (response.StatusCode == HttpStatusCode.Unauthorized)
                {
                    // retry 3 times for now
                    if (tokenRetryCount == 3)
                    {
                        throw ResponseGetter.ThrowInvalidOperation(
                            logger,
                            methodName,
                            response.ToString());
                    }

                    jwtToken = await tokenService.RefreshJwtToken().ConfigureAwait(false);
                    tokenRetryCount++;
                }
                else
                {
                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        string jsonResult = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                        var result = JsonHelper.Deserialize<T>(jsonResult);
                      

                        successful?.Invoke(result);

                        return result;
                    }
                    else
                    {
                        throw ResponseGetter.ThrowInvalidOperation(
                            logger,
                            methodName,
                            response.ToString());
                    }
                }
            }
        }
    }
}
