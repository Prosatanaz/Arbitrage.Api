using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Arbitrage.Api.Application.Instruments;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.BitMart;

// NOTE: field/endpoint names follow BitMart's documented contract (futures) v2 API shape as of
// this writing (general API knowledge, not re-verified against live docs for this pass). Verify
// with a real smoke test before trusting fills - see feedback memory on unverified exchange
// details. BitMart's order "side" is a single numeric code that fuses direction AND open/close
// intent (1=buy_open_long, 2=buy_close_short, 3=sell_close_long, 4=sell_open_short) - same class
// of risk flagged on the MEXC client, and the single highest-risk piece here. BitMart also
// requires a "memo" (stored in credentials.Passphrase) set when the API key was created, signed
// into every request alongside key/secret. Orders are sized in CONTRACTS via "contract_size",
// same conversion risk as Gate.io/KuCoin/MEXC.
public sealed class BitMartTradingClient : IExchangeTradingClient
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient _httpClient;
    private readonly BitMartTradingOptions _options;
    private readonly BitMartAuthSigner _signer;
    private readonly ILogger<BitMartTradingClient> _logger;

    public BitMartTradingClient(
        HttpClient httpClient,
        IOptions<BitMartTradingOptions> options,
        BitMartAuthSigner signer,
        ILogger<BitMartTradingClient> logger)
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

    public string ConnectorName => "bitmart_perpetual";

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
                "BitMart connection check failed.");

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
        var response = await SendSignedGetAsync<BitMartEnvelope<List<BitMartAsset>>>(
            "/contract/private/assets-detail",
            "",
            credentials,
            ct);

        EnsureBitMartSuccess(response);

        var now = DateTimeOffset.UtcNow;

        return (response.Data ?? [])
            .Select(x => new ExchangeBalanceSnapshot(
                ConnectorName: ConnectorName,
                Asset: x.Currency ?? "",
                WalletBalance: ParseDecimal(x.Equity),
                AvailableBalance: ParseDecimal(x.Available),
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
        var exchangeSymbol = ToBitMartSymbol(request.TradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
        {
            throw new InvalidOperationException(
                $"BitMart symbol could not be derived for trading pair '{request.TradingPair}'.");
        }

        var contract = await GetContractAsync(
            exchangeSymbol,
            ct);

        if (contract is null || ParseDecimal(contract.ContractSize) <= 0)
        {
            throw new InvalidOperationException(
                $"BitMart contract metadata unavailable for '{exchangeSymbol}'.");
        }

        var contractSize = ParseDecimal(contract.ContractSize);
        var size = (long)Math.Round(
            request.Quantity / contractSize,
            MidpointRounding.AwayFromZero);

        if (size <= 0)
            size = 1;

        var isBuy = request.Side.Equals("Buy", StringComparison.OrdinalIgnoreCase);

        // side: 1=buy_open_long, 2=buy_close_short, 3=sell_close_long, 4=sell_open_short
        var side = (isBuy, request.ReduceOnly) switch
        {
            (true, false) => 1,
            (false, false) => 4,
            (true, true) => 2,
            (false, true) => 3
        };

        var body = JsonSerializer.Serialize(new
        {
            symbol = exchangeSymbol,
            side,
            type = "market",
            size,
            leverage = "1",
            open_type = "cross",
            client_order_id = request.ClientOrderId
        });

        var createResponse = await SendSignedPostAsync<BitMartEnvelope<BitMartOrderCreateData>>(
            "/contract/private/submit-order",
            body,
            credentials,
            ct);

        EnsureBitMartSuccess(createResponse);

        var orderId = createResponse.Data?.OrderId ?? "";

        var fill = await GetFillWithRetryAsync(
            exchangeSymbol,
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
        string exchangeSymbol,
        string orderId,
        decimal contractSize,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(orderId))
            return (0m, 0m, 0m);

        for (var attempt = 0; attempt < 5; attempt++)
        {
            var queryString = $"symbol={Uri.EscapeDataString(exchangeSymbol)}&order_id={Uri.EscapeDataString(orderId)}";

            var response = await SendSignedGetAsync<BitMartEnvelope<BitMartOrderDetailData>>(
                "/contract/private/order",
                queryString,
                credentials,
                ct);

            EnsureBitMartSuccess(response);

            var dealSize = ParseDecimal(response.Data?.DealSize);

            if (dealSize > 0)
            {
                var averagePrice = ParseDecimal(response.Data?.DealAvgPrice);
                var fee = Math.Abs(ParseDecimal(response.Data?.PaidFees));

                return (dealSize * contractSize, averagePrice, fee);
            }

            await Task.Delay(200, ct);
        }

        return (0m, 0m, 0m);
    }

    public async Task<ExchangeSymbolRules?> GetSymbolRulesAsync(
        string tradingPair,
        CancellationToken ct)
    {
        var exchangeSymbol = ToBitMartSymbol(tradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
            return null;

        var item = await GetContractAsync(
            exchangeSymbol,
            ct);

        if (item is null)
            return null;

        var pricePrecision = ParseInt(item.PricePrecision);
        var contractSize = ParseDecimal(item.ContractSize);

        return new ExchangeSymbolRules(
            ConnectorName: ConnectorName,
            TradingPair: InstrumentFilterService.NormalizeTradingPair(tradingPair),
            ExchangeSymbol: item.Symbol ?? exchangeSymbol,
            Status: item.Status ?? "",
            TickSize: (decimal)Math.Pow(10, -pricePrecision),
            QuantityStep: contractSize,
            MinQuantity: contractSize * ParseDecimal(item.MinVolume),
            MinNotional: 0m,
            MaxMarketQuantity: ParseNullableDecimal(item.MaxVolume) is { } maxVol ? maxVol * contractSize : null,
            MaxLimitQuantity: null,
            ReceivedAt: DateTimeOffset.UtcNow);
    }

    private async Task<BitMartContract?> GetContractAsync(
        string exchangeSymbol,
        CancellationToken ct)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"/contract/public/details?symbol={Uri.EscapeDataString(exchangeSymbol)}");

        var response = await SendAsync<BitMartEnvelope<BitMartDetailsData>>(
            request,
            ct);

        EnsureBitMartSuccess(response);

        return response.Data?.Symbols?.FirstOrDefault();
    }

    private async Task<T> SendSignedGetAsync<T>(
        string path,
        string queryString,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var requestUri = string.IsNullOrEmpty(queryString) ? path : $"{path}?{queryString}";

        return await SendSignedAsync<T>(
            HttpMethod.Get,
            requestUri,
            "",
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
        string body,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(credentials.Passphrase))
        {
            throw new InvalidOperationException(
                "BitMart requires an API memo (stored as the connector's passphrase) in addition to key/secret.");
        }

        var timestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

        var signature = _signer.Sign(
            credentials.ApiSecret,
            timestampMs,
            credentials.Passphrase,
            body);

        using var request = new HttpRequestMessage(
            method,
            requestUri);

        request.Headers.TryAddWithoutValidation("X-BM-KEY", credentials.ApiKey);
        request.Headers.TryAddWithoutValidation("X-BM-SIGN", signature);
        request.Headers.TryAddWithoutValidation("X-BM-TIMESTAMP", timestampMs);

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
                $"BitMart HTTP request failed. StatusCode={(int)response.StatusCode}, Body={body}");
        }

        var parsed = JsonSerializer.Deserialize<T>(
            body,
            JsonOptions);

        if (parsed is null)
        {
            throw new InvalidOperationException(
                "BitMart response deserialization failed.");
        }

        return parsed;
    }

    private static void EnsureBitMartSuccess<T>(BitMartEnvelope<T> envelope)
    {
        if (envelope.Code != 1000)
        {
            throw new InvalidOperationException(
                $"BitMart API returned error. Code={envelope.Code}, Message={envelope.Message}");
        }
    }

    private static string ToBitMartSymbol(string tradingPair)
    {
        var normalized = InstrumentFilterService.NormalizeTradingPair(tradingPair);

        var parts = normalized.Split(
            '-',
            StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (parts.Length != 2)
            return "";

        return parts[0] + parts[1];
    }

    private static int ParseInt(string? value)
    {
        return int.TryParse(
            value,
            NumberStyles.Any,
            CultureInfo.InvariantCulture,
            out var result)
            ? result
            : 0;
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

    private sealed class BitMartEnvelope<T>
    {
        public int Code { get; set; }

        public string? Message { get; set; }

        public T? Data { get; set; }
    }

    private sealed class BitMartAsset
    {
        public string? Currency { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? Equity { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? Available { get; set; }
    }

    private sealed class BitMartOrderCreateData
    {
        public string? OrderId { get; set; }
    }

    private sealed class BitMartOrderDetailData
    {
        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? DealSize { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? DealAvgPrice { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? PaidFees { get; set; }
    }

    private sealed class BitMartDetailsData
    {
        public List<BitMartContract>? Symbols { get; set; }
    }

    private sealed class BitMartContract
    {
        public string? Symbol { get; set; }

        public string? Status { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? PricePrecision { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? ContractSize { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? MinVolume { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? MaxVolume { get; set; }
    }
}
