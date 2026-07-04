using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Arbitrage.Api.Application.Instruments;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.KuCoin;

// NOTE: field/endpoint names follow KuCoin Futures' documented v1 API shape as of this writing
// (general API knowledge, not re-verified against live docs for this pass). Verify with a real
// smoke test before trusting fills - see feedback memory on unverified exchange details.
// Two KuCoin-specific risks worth flagging explicitly:
//  1. KuCoin Futures orders are sized in LOTS/contracts, not base-asset units - converted here via
//     the contract's "multiplier" (base units per lot), same class of risk as Gate.io's
//     quanto_multiplier conversion.
//  2. KuCoin Futures uses "XBT" instead of "BTC" in its Bitcoin contract symbol (e.g. XBTUSDTM) -
//     the symbol mapping below special-cases BTC only; verify no other asset needs a similar
//     rename before relying on this for a new trading pair.
public sealed class KuCoinTradingClient : IExchangeTradingClient
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient _httpClient;
    private readonly KuCoinTradingOptions _options;
    private readonly KuCoinAuthSigner _signer;
    private readonly ILogger<KuCoinTradingClient> _logger;

    public KuCoinTradingClient(
        HttpClient httpClient,
        IOptions<KuCoinTradingOptions> options,
        KuCoinAuthSigner signer,
        ILogger<KuCoinTradingClient> logger)
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

    public string ConnectorName => "kucoin_perpetual";

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
                "KuCoin connection check failed.");

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
        var response = await SendSignedGetAsync<KuCoinEnvelope<KuCoinAccountOverview>>(
            "/api/v1/account-overview",
            "currency=USDT",
            credentials,
            ct);

        EnsureKuCoinSuccess(response);

        var now = DateTimeOffset.UtcNow;
        var data = response.Data;

        return
        [
            new ExchangeBalanceSnapshot(
                ConnectorName: ConnectorName,
                Asset: "USDT",
                WalletBalance: ParseDecimal(data?.AccountEquity),
                AvailableBalance: ParseDecimal(data?.AvailableBalance),
                Equity: ParseDecimal(data?.AccountEquity),
                UsdValue: ParseDecimal(data?.AccountEquity),
                ReceivedAt: now)
        ];
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
        var exchangeSymbol = ToKuCoinSymbol(request.TradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
        {
            throw new InvalidOperationException(
                $"KuCoin symbol could not be derived for trading pair '{request.TradingPair}'.");
        }

        var contract = await GetContractAsync(
            exchangeSymbol,
            ct);

        if (contract is null || ParseDecimal(contract.Multiplier) <= 0)
        {
            throw new InvalidOperationException(
                $"KuCoin contract metadata unavailable for '{exchangeSymbol}'.");
        }

        var multiplier = ParseDecimal(contract.Multiplier);
        var lots = (long)Math.Round(
            request.Quantity / multiplier,
            MidpointRounding.AwayFromZero);

        if (lots <= 0)
            lots = 1;

        var body = JsonSerializer.Serialize(new
        {
            clientOid = request.ClientOrderId,
            side = request.Side.Equals("Buy", StringComparison.OrdinalIgnoreCase) ? "buy" : "sell",
            symbol = exchangeSymbol,
            type = "market",
            size = lots,
            reduceOnly = request.ReduceOnly,
            leverage = "1"
        });

        var createResponse = await SendSignedPostAsync<KuCoinEnvelope<KuCoinOrderCreateData>>(
            "/api/v1/orders",
            body,
            credentials,
            ct);

        EnsureKuCoinSuccess(createResponse);

        var orderId = createResponse.Data?.OrderId ?? "";

        var fill = await GetFillWithRetryAsync(
            orderId,
            multiplier,
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
        decimal multiplier,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(orderId))
            return (0m, 0m, 0m);

        for (var attempt = 0; attempt < 5; attempt++)
        {
            var response = await SendSignedGetAsync<KuCoinEnvelope<KuCoinOrderDetailData>>(
                $"/api/v1/orders/{Uri.EscapeDataString(orderId)}",
                "",
                credentials,
                ct);

            EnsureKuCoinSuccess(response);

            var dealSize = ParseDecimal(response.Data?.DealSize);

            if (dealSize > 0)
            {
                var dealValue = ParseDecimal(response.Data?.DealValue);
                var filledQuantity = dealSize * multiplier;
                var averagePrice = filledQuantity > 0 ? dealValue / filledQuantity : 0m;
                var fee = Math.Abs(ParseDecimal(response.Data?.Fee));

                return (filledQuantity, averagePrice, fee);
            }

            await Task.Delay(200, ct);
        }

        return (0m, 0m, 0m);
    }

    public async Task<ExchangeSymbolRules?> GetSymbolRulesAsync(
        string tradingPair,
        CancellationToken ct)
    {
        var exchangeSymbol = ToKuCoinSymbol(tradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
            return null;

        var item = await GetContractAsync(
            exchangeSymbol,
            ct);

        if (item is null)
            return null;

        var multiplier = ParseDecimal(item.Multiplier);

        return new ExchangeSymbolRules(
            ConnectorName: ConnectorName,
            TradingPair: InstrumentFilterService.NormalizeTradingPair(tradingPair),
            ExchangeSymbol: item.Symbol ?? exchangeSymbol,
            Status: item.Status ?? "",
            TickSize: ParseDecimal(item.TickSize),
            QuantityStep: multiplier,
            MinQuantity: multiplier,
            MinNotional: 0m,
            MaxMarketQuantity: ParseNullableDecimal(item.MaxOrderQty) is { } maxLots ? maxLots * multiplier : null,
            MaxLimitQuantity: null,
            ReceivedAt: DateTimeOffset.UtcNow);
    }

    private async Task<KuCoinContract?> GetContractAsync(
        string exchangeSymbol,
        CancellationToken ct)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"/api/v1/contracts/{Uri.EscapeDataString(exchangeSymbol)}");

        var response = await SendAsync<KuCoinEnvelope<KuCoinContract>>(
            request,
            ct);

        EnsureKuCoinSuccess(response);

        return response.Data;
    }

    private async Task<T> SendSignedGetAsync<T>(
        string endpoint,
        string queryString,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var endpointWithQuery = string.IsNullOrEmpty(queryString) ? endpoint : $"{endpoint}?{queryString}";

        return await SendSignedAsync<T>(
            HttpMethod.Get,
            endpointWithQuery,
            "",
            credentials,
            ct);
    }

    private async Task<T> SendSignedPostAsync<T>(
        string endpoint,
        string body,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        return await SendSignedAsync<T>(
            HttpMethod.Post,
            endpoint,
            body,
            credentials,
            ct);
    }

    private async Task<T> SendSignedAsync<T>(
        HttpMethod method,
        string endpointWithQuery,
        string body,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(credentials.Passphrase))
        {
            throw new InvalidOperationException(
                "KuCoin requires an API passphrase in addition to key/secret.");
        }

        var timestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

        var signature = _signer.Sign(
            credentials.ApiSecret,
            timestampMs,
            method.Method,
            endpointWithQuery,
            body);

        var signedPassphrase = _signer.SignPassphrase(
            credentials.ApiSecret,
            credentials.Passphrase);

        using var request = new HttpRequestMessage(
            method,
            endpointWithQuery);

        request.Headers.TryAddWithoutValidation("KC-API-KEY", credentials.ApiKey);
        request.Headers.TryAddWithoutValidation("KC-API-SIGN", signature);
        request.Headers.TryAddWithoutValidation("KC-API-TIMESTAMP", timestampMs);
        request.Headers.TryAddWithoutValidation("KC-API-PASSPHRASE", signedPassphrase);
        request.Headers.TryAddWithoutValidation("KC-API-KEY-VERSION", "2");

        if (!string.IsNullOrEmpty(body))
        {
            request.Content = new StringContent(
                body,
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
                $"KuCoin HTTP request failed. StatusCode={(int)response.StatusCode}, Body={body}");
        }

        var parsed = JsonSerializer.Deserialize<T>(
            body,
            JsonOptions);

        if (parsed is null)
        {
            throw new InvalidOperationException(
                "KuCoin response deserialization failed.");
        }

        return parsed;
    }

    private static void EnsureKuCoinSuccess<T>(KuCoinEnvelope<T> envelope)
    {
        if (envelope.Code != "200000")
        {
            throw new InvalidOperationException(
                $"KuCoin API returned error. Code={envelope.Code}, Msg={envelope.Msg}");
        }
    }

    private static string ToKuCoinSymbol(string tradingPair)
    {
        var normalized = InstrumentFilterService.NormalizeTradingPair(tradingPair);

        var parts = normalized.Split(
            '-',
            StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (parts.Length != 2)
            return "";

        var baseAsset = parts[0] == "BTC" ? "XBT" : parts[0];

        return $"{baseAsset}{parts[1]}M";
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

    private sealed class KuCoinEnvelope<T>
    {
        public string Code { get; set; } = "";

        public string? Msg { get; set; }

        public T? Data { get; set; }
    }

    private sealed class KuCoinAccountOverview
    {
        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? AccountEquity { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? AvailableBalance { get; set; }
    }

    private sealed class KuCoinOrderCreateData
    {
        public string? OrderId { get; set; }
    }

    private sealed class KuCoinOrderDetailData
    {
        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? DealSize { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? DealValue { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? Fee { get; set; }

        public string? Status { get; set; }
    }

    private sealed class KuCoinContract
    {
        public string? Symbol { get; set; }

        public string? Status { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? Multiplier { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? TickSize { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? MaxOrderQty { get; set; }
    }
}
