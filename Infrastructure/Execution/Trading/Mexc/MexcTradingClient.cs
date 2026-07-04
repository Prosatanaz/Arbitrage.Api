using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Arbitrage.Api.Application.Instruments;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.Mexc;

// NOTE: field/endpoint names follow MEXC Futures' documented v1 contract API shape as of this
// writing (general API knowledge, not re-verified against live docs for this pass). Verify with a
// real smoke test before trusting fills - see feedback memory on unverified exchange details.
// MEXC's order "side" is a single numeric code that fuses direction AND open/close intent
// (1=open long, 2=close short, 3=open short, 4=close long) instead of separate side/reduceOnly
// flags like every other connector in this codebase - this is the single highest-risk piece of
// this client and must be sanity-checked against the exchange UI before relying on it. Orders are
// also sized in CONTRACTS via "contractSize" (base units per contract), same conversion risk as
// Gate.io/KuCoin.
public sealed class MexcTradingClient : IExchangeTradingClient
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient _httpClient;
    private readonly MexcTradingOptions _options;
    private readonly MexcAuthSigner _signer;
    private readonly ILogger<MexcTradingClient> _logger;

    public MexcTradingClient(
        HttpClient httpClient,
        IOptions<MexcTradingOptions> options,
        MexcAuthSigner signer,
        ILogger<MexcTradingClient> logger)
    {
        _httpClient = httpClient;
        _options = options.Value;
        _signer = signer;
        _logger = logger;

        _httpClient.BaseAddress = new Uri(_options.BaseUrl.TrimEnd('/') + "/");
        _httpClient.DefaultRequestHeaders.Accept.Clear();
        _httpClient.DefaultRequestHeaders.Accept.Add(
            new MediaTypeWithQualityHeaderValue("application/json"));
    }

    public string ConnectorName => "mexc_perpetual";

    public async Task<ExchangeConnectionCheckResult> CheckConnectionAsync(
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var checkedAt = DateTimeOffset.UtcNow;

        try
        {
            var balances = await GetBalancesAsync(
                credentials,
                ct);

            var hasUsdt = balances.Any(x =>
                x.Asset.Equals("USDT", StringComparison.OrdinalIgnoreCase));

            return new ExchangeConnectionCheckResult(
                ConnectorName: ConnectorName,
                IsConnected: true,
                Status: hasUsdt ? "Connected" : "ConnectedNoUsdtBalance",
                Error: hasUsdt ? null : "Connection succeeded, but USDT balance was not returned.",
                CheckedAt: checkedAt);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "MEXC connection check failed.");

            return new ExchangeConnectionCheckResult(
                ConnectorName: ConnectorName,
                IsConnected: false,
                Status: "ConnectionFailed",
                Error: ex.Message,
                CheckedAt: checkedAt);
        }
    }

    public async Task<IReadOnlyList<ExchangeBalanceSnapshot>> GetBalancesAsync(
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var response = await SendSignedGetAsync<MexcEnvelope<List<MexcAsset>>>(
            "/api/v1/private/account/assets",
            "",
            credentials,
            ct);

        EnsureMexcSuccess(response);

        var now = DateTimeOffset.UtcNow;

        return (response.Data ?? [])
            .Select(x => new ExchangeBalanceSnapshot(
                ConnectorName: ConnectorName,
                Asset: x.Currency ?? "",
                WalletBalance: ParseDecimal(x.CashBalance),
                AvailableBalance: ParseDecimal(x.AvailableBalance),
                Equity: ParseDecimal(x.Equity),
                UsdValue: ParseDecimal(x.Equity),
                ReceivedAt: now))
            .ToList<ExchangeBalanceSnapshot>();
    }

    public Task<IReadOnlyList<ExchangePositionSnapshot>> GetPositionsAsync(
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        IReadOnlyList<ExchangePositionSnapshot> result = [];

        return Task.FromResult(result);
    }

    public async Task<OrderFillResult> PlaceOrderAsync(
        ExchangeApiCredentialSecret credentials,
        PlaceOrderRequest request,
        CancellationToken ct)
    {
        var exchangeSymbol = ToMexcSymbol(request.TradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
        {
            throw new InvalidOperationException(
                $"MEXC symbol could not be derived for trading pair '{request.TradingPair}'.");
        }

        var contract = await GetContractAsync(
            exchangeSymbol,
            ct);

        if (contract is null || ParseDecimal(contract.ContractSize) <= 0)
        {
            throw new InvalidOperationException(
                $"MEXC contract metadata unavailable for '{exchangeSymbol}'.");
        }

        var contractSize = ParseDecimal(contract.ContractSize);
        var vol = (long)Math.Round(
            request.Quantity / contractSize,
            MidpointRounding.AwayFromZero);

        if (vol <= 0)
            vol = 1;

        var isBuy = request.Side.Equals("Buy", StringComparison.OrdinalIgnoreCase);

        // side: 1=open long, 2=close short, 3=open short, 4=close long
        var side = (isBuy, request.ReduceOnly) switch
        {
            (true, false) => 1,
            (false, false) => 3,
            (true, true) => 2,
            (false, true) => 4
        };

        var body = JsonSerializer.Serialize(new
        {
            symbol = exchangeSymbol,
            price = "0",
            vol,
            side,
            type = 5,
            openType = 2,
            leverage = 1,
            externalOid = request.ClientOrderId
        });

        var createResponse = await SendSignedPostAsync<MexcEnvelope<long?>>(
            "/api/v1/private/order/submit",
            body,
            credentials,
            ct);

        EnsureMexcSuccess(createResponse);

        var orderId = createResponse.Data?.ToString(CultureInfo.InvariantCulture) ?? "";

        var fill = await GetFillWithRetryAsync(
            orderId,
            contractSize,
            credentials,
            ct);

        return new OrderFillResult(
            ConnectorName: ConnectorName,
            ClientOrderId: request.ClientOrderId,
            ExchangeOrderId: orderId,
            IsFilled: fill.FilledQuantity > 0,
            FilledQuantity: fill.FilledQuantity,
            AverageFillPrice: fill.AveragePrice,
            FeePaidUsd: fill.FeePaid,
            Status: fill.FilledQuantity >= request.Quantity ? "Filled" : fill.FilledQuantity > 0 ? "PartiallyFilled" : "Unfilled",
            FilledAt: DateTimeOffset.UtcNow);
    }

    private async Task<(decimal FilledQuantity, decimal AveragePrice, decimal FeePaid)> GetFillWithRetryAsync(
        string orderId,
        decimal contractSize,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(orderId))
            return (0m, 0m, 0m);

        for (var attempt = 0; attempt < 5; attempt++)
        {
            var response = await SendSignedGetAsync<MexcEnvelope<MexcOrderDetail>>(
                $"/api/v1/private/order/get/{Uri.EscapeDataString(orderId)}",
                "",
                credentials,
                ct);

            EnsureMexcSuccess(response);

            var dealVol = ParseDecimal(response.Data?.DealVol);

            if (dealVol > 0)
            {
                var averagePrice = ParseDecimal(response.Data?.DealAvgPrice);
                var fee = Math.Abs(ParseDecimal(response.Data?.Fee) + ParseDecimal(response.Data?.TakerFee));

                return (dealVol * contractSize, averagePrice, fee);
            }

            await Task.Delay(200, ct);
        }

        return (0m, 0m, 0m);
    }

    public async Task<ExchangeSymbolRules?> GetSymbolRulesAsync(
        string tradingPair,
        CancellationToken ct)
    {
        var exchangeSymbol = ToMexcSymbol(tradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
            return null;

        var item = await GetContractAsync(
            exchangeSymbol,
            ct);

        if (item is null)
            return null;

        var contractSize = ParseDecimal(item.ContractSize);

        return new ExchangeSymbolRules(
            ConnectorName: ConnectorName,
            TradingPair: InstrumentFilterService.NormalizeTradingPair(tradingPair),
            ExchangeSymbol: item.Symbol ?? exchangeSymbol,
            Status: item.State ?? "",
            TickSize: ParseDecimal(item.PriceUnit),
            QuantityStep: contractSize,
            MinQuantity: contractSize * ParseDecimal(item.MinVol),
            MinNotional: 0m,
            MaxMarketQuantity: ParseNullableDecimal(item.MaxVol) is { } maxVol ? maxVol * contractSize : null,
            MaxLimitQuantity: null,
            ReceivedAt: DateTimeOffset.UtcNow);
    }

    private async Task<MexcContract?> GetContractAsync(
        string exchangeSymbol,
        CancellationToken ct)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"/api/v1/contract/detail?symbol={Uri.EscapeDataString(exchangeSymbol)}");

        var response = await SendAsync<MexcEnvelope<MexcContract>>(
            request,
            ct);

        EnsureMexcSuccess(response);

        return response.Data;
    }

    private async Task<T> SendSignedGetAsync<T>(
        string path,
        string queryString,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        return await SendSignedAsync<T>(
            HttpMethod.Get,
            string.IsNullOrEmpty(queryString) ? path : $"{path}?{queryString}",
            queryString,
            credentials,
            ct);
    }

    private async Task<T> SendSignedPostAsync<T>(
        string path,
        string body,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        return await SendSignedAsync<T>(
            HttpMethod.Post,
            path,
            body,
            credentials,
            ct);
    }

    private async Task<T> SendSignedAsync<T>(
        HttpMethod method,
        string requestUri,
        string paramString,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var timestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

        var signature = _signer.Sign(
            credentials.ApiKey,
            credentials.ApiSecret,
            timestampMs,
            paramString);

        using var request = new HttpRequestMessage(
            method,
            requestUri);

        request.Headers.TryAddWithoutValidation("ApiKey", credentials.ApiKey);
        request.Headers.TryAddWithoutValidation("Request-Time", timestampMs);
        request.Headers.TryAddWithoutValidation("Signature", signature);

        if (method == HttpMethod.Post)
        {
            request.Content = new StringContent(
                paramString,
                Encoding.UTF8,
                "application/json");
        }

        return await SendAsync<T>(
            request,
            ct);
    }

    private async Task<T> SendAsync<T>(
        HttpRequestMessage request,
        CancellationToken ct)
    {
        using var response = await _httpClient.SendAsync(
            request,
            ct);

        var body = await response.Content.ReadAsStringAsync(ct);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"MEXC HTTP request failed. StatusCode={(int)response.StatusCode}, Body={body}");
        }

        var parsed = JsonSerializer.Deserialize<T>(
            body,
            JsonOptions);

        if (parsed is null)
        {
            throw new InvalidOperationException(
                "MEXC response deserialization failed.");
        }

        return parsed;
    }

    private static void EnsureMexcSuccess<T>(MexcEnvelope<T> envelope)
    {
        if (!envelope.Success)
        {
            throw new InvalidOperationException(
                $"MEXC API returned error. Code={envelope.Code}, Message={envelope.Message}");
        }
    }

    private static string ToMexcSymbol(string tradingPair)
    {
        var normalized = InstrumentFilterService.NormalizeTradingPair(tradingPair);

        var parts = normalized.Split(
            '-',
            StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (parts.Length != 2)
            return "";

        return $"{parts[0]}_{parts[1]}";
    }

    private static decimal ParseDecimal(string? value)
    {
        if (decimal.TryParse(
                value,
                NumberStyles.Any,
                CultureInfo.InvariantCulture,
                out var result))
        {
            return result;
        }

        return 0m;
    }

    private static decimal? ParseNullableDecimal(string? value)
    {
        if (decimal.TryParse(
                value,
                NumberStyles.Any,
                CultureInfo.InvariantCulture,
                out var result))
        {
            return result;
        }

        return null;
    }

    private sealed class MexcEnvelope<T>
    {
        public bool Success { get; set; }

        public int Code { get; set; }

        public string? Message { get; set; }

        public T? Data { get; set; }
    }

    private sealed class MexcAsset
    {
        public string? Currency { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? AvailableBalance { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? CashBalance { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? Equity { get; set; }
    }

    private sealed class MexcOrderDetail
    {
        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? DealVol { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? DealAvgPrice { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? Fee { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? TakerFee { get; set; }

        public int? State { get; set; }
    }

    private sealed class MexcContract
    {
        public string? Symbol { get; set; }

        public string? State { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? ContractSize { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? PriceUnit { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? MinVol { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? MaxVol { get; set; }
    }
}
