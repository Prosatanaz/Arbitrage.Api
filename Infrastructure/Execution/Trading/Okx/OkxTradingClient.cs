using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Arbitrage.Api.Application.Instruments;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.Okx;

// NOTE: field/endpoint names follow OKX's documented v5 unified account API shape as of this
// writing (general API knowledge, not re-verified against live docs for this pass). Verify with a
// real smoke test before trusting fills - see feedback memory on unverified exchange details.
// OKX requires a passphrase in addition to key/secret; credentials.Passphrase must be set.
public sealed class OkxTradingClient : IExchangeTradingClient
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient _httpClient;
    private readonly OkxTradingOptions _options;
    private readonly OkxAuthSigner _signer;
    private readonly ILogger<OkxTradingClient> _logger;

    public OkxTradingClient(
        HttpClient httpClient,
        IOptions<OkxTradingOptions> options,
        OkxAuthSigner signer,
        ILogger<OkxTradingClient> logger)
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

    public string ConnectorName => "okx_perpetual";

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
                "OKX connection check failed.");

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
        var response = await SendSignedGetAsync<OkxEnvelope<OkxBalanceData>>(
            path: "/api/v5/account/balance",
            queryString: "",
            credentials: credentials,
            ct: ct);

        EnsureOkxSuccess(response);

        var now = DateTimeOffset.UtcNow;
        var result = new List<ExchangeBalanceSnapshot>();

        foreach (var account in response.Data ?? [])
        {
            foreach (var detail in account.Details ?? [])
            {
                result.Add(new ExchangeBalanceSnapshot(
                    ConnectorName: ConnectorName,
                    Asset: detail.Ccy ?? "",
                    WalletBalance: ParseDecimal(detail.CashBal),
                    AvailableBalance: ParseDecimal(detail.AvailBal),
                    Equity: ParseDecimal(detail.Eq),
                    UsdValue: ParseDecimal(detail.EqUsd),
                    ReceivedAt: now));
            }
        }

        return result;
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
        var exchangeSymbol = ToOkxSymbol(request.TradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
        {
            throw new InvalidOperationException(
                $"OKX symbol could not be derived for trading pair '{request.TradingPair}'.");
        }

        var body = JsonSerializer.Serialize(new
        {
            instId = exchangeSymbol,
            tdMode = _options.TradeMode,
            side = request.Side.Equals("Buy", StringComparison.OrdinalIgnoreCase) ? "buy" : "sell",
            ordType = "market",
            sz = request.Quantity.ToString(CultureInfo.InvariantCulture),
            clOrdId = ToOkxClientOrderId(request.ClientOrderId),
            reduceOnly = request.ReduceOnly
        });

        var createResponse = await SendSignedPostAsync<OkxEnvelope<OkxOrderCreateData>>(
            path: "/api/v5/trade/order",
            body: body,
            credentials: credentials,
            ct: ct);

        EnsureOkxSuccess(createResponse);

        var created = createResponse.Data?.FirstOrDefault();

        if (created is not null && created.SCode != "0")
        {
            throw new InvalidOperationException(
                $"OKX order rejected. SCode={created.SCode}, SMsg={created.SMsg}");
        }

        var orderId = created?.OrdId ?? "";

        var fill = await GetFillWithRetryAsync(
            exchangeSymbol,
            orderId,
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
        string exchangeSymbol,
        string orderId,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        for (var attempt = 0; attempt < 5; attempt++)
        {
            var queryString = $"instId={Uri.EscapeDataString(exchangeSymbol)}&ordId={Uri.EscapeDataString(orderId)}";

            var response = await SendSignedGetAsync<OkxEnvelope<OkxOrderDetailData>>(
                path: "/api/v5/trade/order",
                queryString: queryString,
                credentials: credentials,
                ct: ct);

            EnsureOkxSuccess(response);

            var order = response.Data?.FirstOrDefault();

            var filledQuantity = ParseDecimal(order?.AccFillSz);

            if (filledQuantity > 0)
            {
                var averagePrice = ParseDecimal(order?.AvgPx);
                var fee = Math.Abs(ParseDecimal(order?.Fee));

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
        var exchangeSymbol = ToOkxSymbol(tradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
            return null;

        var queryString = $"instType=SWAP&instId={Uri.EscapeDataString(exchangeSymbol)}";

        var response = await SendPublicGetAsync<OkxEnvelope<OkxInstrumentData>>(
            path: "/api/v5/public/instruments",
            queryString: queryString,
            ct: ct);

        EnsureOkxSuccess(response);

        var item = response.Data?.FirstOrDefault();

        if (item is null)
            return null;

        return new ExchangeSymbolRules(
            ConnectorName: ConnectorName,
            TradingPair: InstrumentFilterService.NormalizeTradingPair(tradingPair),
            ExchangeSymbol: item.InstId ?? exchangeSymbol,
            Status: item.State ?? "",
            TickSize: ParseDecimal(item.TickSz),
            QuantityStep: ParseDecimal(item.LotSz),
            MinQuantity: ParseDecimal(item.MinSz),
            MinNotional: 0m,
            MaxMarketQuantity: ParseNullableDecimal(item.MaxMktSz),
            MaxLimitQuantity: ParseNullableDecimal(item.MaxLmtSz),
            ReceivedAt: DateTimeOffset.UtcNow);
    }

    private async Task<T> SendSignedGetAsync<T>(
        string path,
        string queryString,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var requestPath = string.IsNullOrEmpty(queryString) ? path : $"{path}?{queryString}";
        var timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture);

        RequirePassphrase(credentials);

        var signature = _signer.Sign(
            credentials.ApiSecret,
            timestamp,
            "GET",
            requestPath,
            "");

        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            requestPath);

        ApplyAuthHeaders(
            request,
            credentials,
            timestamp,
            signature);

        return await SendAsync<T>(
            request,
            ct);
    }

    private async Task<T> SendSignedPostAsync<T>(
        string path,
        string body,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture);

        RequirePassphrase(credentials);

        var signature = _signer.Sign(
            credentials.ApiSecret,
            timestamp,
            "POST",
            path,
            body);

        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            path);

        ApplyAuthHeaders(
            request,
            credentials,
            timestamp,
            signature);

        request.Content = new StringContent(
            body,
            Encoding.UTF8,
            "application/json");

        return await SendAsync<T>(
            request,
            ct);
    }

    private async Task<T> SendPublicGetAsync<T>(
        string path,
        string queryString,
        CancellationToken ct)
    {
        var requestPath = string.IsNullOrEmpty(queryString) ? path : $"{path}?{queryString}";

        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            requestPath);

        return await SendAsync<T>(
            request,
            ct);
    }

    private static void ApplyAuthHeaders(
        HttpRequestMessage request,
        ExchangeApiCredentialSecret credentials,
        string timestamp,
        string signature)
    {
        request.Headers.TryAddWithoutValidation("OK-ACCESS-KEY", credentials.ApiKey);
        request.Headers.TryAddWithoutValidation("OK-ACCESS-SIGN", signature);
        request.Headers.TryAddWithoutValidation("OK-ACCESS-TIMESTAMP", timestamp);
        request.Headers.TryAddWithoutValidation("OK-ACCESS-PASSPHRASE", credentials.Passphrase);
    }

    private static void RequirePassphrase(ExchangeApiCredentialSecret credentials)
    {
        if (string.IsNullOrWhiteSpace(credentials.Passphrase))
        {
            throw new InvalidOperationException(
                "OKX requires an API passphrase in addition to key/secret.");
        }
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
                $"OKX HTTP request failed. StatusCode={(int)response.StatusCode}, Body={body}");
        }

        var parsed = JsonSerializer.Deserialize<T>(
            body,
            JsonOptions);

        if (parsed is null)
        {
            throw new InvalidOperationException(
                "OKX response deserialization failed.");
        }

        return parsed;
    }

    private static void EnsureOkxSuccess<T>(OkxEnvelope<T> envelope)
    {
        if (envelope.Code != "0")
        {
            throw new InvalidOperationException(
                $"OKX API returned error. Code={envelope.Code}, Msg={envelope.Msg}");
        }
    }

    private static string ToOkxSymbol(string tradingPair)
    {
        var normalized = InstrumentFilterService.NormalizeTradingPair(tradingPair);

        var parts = normalized.Split(
            '-',
            StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (parts.Length != 2)
            return "";

        return $"{parts[0]}-{parts[1]}-SWAP";
    }

    private static string ToOkxClientOrderId(string clientOrderId)
    {
        var stripped = clientOrderId.Replace("-", "");

        return stripped.Length > 32 ? stripped[..32] : stripped;
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

    private sealed class OkxEnvelope<T>
    {
        public string Code { get; set; } = "";

        public string Msg { get; set; } = "";

        public List<T>? Data { get; set; }
    }

    private sealed class OkxBalanceData
    {
        public string? TotalEq { get; set; }

        public List<OkxBalanceDetail>? Details { get; set; }
    }

    private sealed class OkxBalanceDetail
    {
        public string? Ccy { get; set; }

        public string? CashBal { get; set; }

        public string? AvailBal { get; set; }

        public string? Eq { get; set; }

        public string? EqUsd { get; set; }
    }

    private sealed class OkxOrderCreateData
    {
        public string? OrdId { get; set; }

        public string? ClOrdId { get; set; }

        public string? SCode { get; set; }

        public string? SMsg { get; set; }
    }

    private sealed class OkxOrderDetailData
    {
        public string? AccFillSz { get; set; }

        public string? AvgPx { get; set; }

        public string? Fee { get; set; }

        public string? State { get; set; }
    }

    private sealed class OkxInstrumentData
    {
        public string? InstId { get; set; }

        public string? State { get; set; }

        public string? TickSz { get; set; }

        public string? LotSz { get; set; }

        public string? MinSz { get; set; }

        public string? MaxMktSz { get; set; }

        public string? MaxLmtSz { get; set; }
    }
}
