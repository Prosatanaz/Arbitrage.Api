using System.Globalization;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Arbitrage.Api.Application.Instruments;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.BingX;

// NOTE: field/endpoint names follow BingX's documented swap v2 API shape as of this writing
// (general API knowledge, not re-verified against live docs for this pass). Verify with a real
// smoke test before trusting fills - see feedback memory on unverified exchange details. BingX
// defaults new API keys to hedge mode, where "side" (BUY/SELL) and "positionSide" (LONG/SHORT) are
// both required and reduceOnly is implied by the side/positionSide combination rather than an
// independent flag - the mapping below is the highest-risk piece of this client. Unlike
// Gate.io/KuCoin/MEXC/BitMart, BingX swap quantity is expressed directly in base-asset units, so
// no contract-multiplier conversion is needed here.
public sealed class BingXTradingClient : IExchangeTradingClient
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient _httpClient;
    private readonly BingXTradingOptions _options;
    private readonly BingXAuthSigner _signer;
    private readonly ILogger<BingXTradingClient> _logger;

    public BingXTradingClient(
        HttpClient httpClient,
        IOptions<BingXTradingOptions> options,
        BingXAuthSigner signer,
        ILogger<BingXTradingClient> logger)
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

    public string ConnectorName => "bingx_perpetual";

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
                "BingX connection check failed.");

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
        var response = await SendSignedGetAsync<BingXEnvelope<BingXBalanceData>>(
            "/openApi/swap/v2/user/balance",
            null,
            credentials,
            ct);

        EnsureBingXSuccess(response);

        var now = DateTimeOffset.UtcNow;
        var balance = response.Data?.Balance;

        if (balance is null)
            return [];

        return
        [
            new ExchangeBalanceSnapshot(
                ConnectorName: ConnectorName,
                Asset: balance.Asset ?? "",
                WalletBalance: ParseDecimal(balance.Balance),
                AvailableBalance: ParseDecimal(balance.AvailableMargin),
                Equity: ParseDecimal(balance.Equity),
                UsdValue: ParseDecimal(balance.Equity),
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
        var exchangeSymbol = ToBingXSymbol(request.TradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
        {
            throw new InvalidOperationException(
                $"BingX symbol could not be derived for trading pair '{request.TradingPair}'.");
        }

        var isBuy = request.Side.Equals("Buy", StringComparison.OrdinalIgnoreCase);
        var side = isBuy ? "BUY" : "SELL";

        // Hedge-mode convention: opening buy -> LONG, opening sell -> SHORT.
        // Closing (reduceOnly) a position trades against it: closing a long is a SELL against
        // positionSide LONG; closing a short is a BUY against positionSide SHORT.
        var positionSide = request.ReduceOnly
            ? (isBuy ? "SHORT" : "LONG")
            : (isBuy ? "LONG" : "SHORT");

        var parameters = new Dictionary<string, string>
        {
            ["symbol"] = exchangeSymbol,
            ["side"] = side,
            ["positionSide"] = positionSide,
            ["type"] = "MARKET",
            ["quantity"] = request.Quantity.ToString(CultureInfo.InvariantCulture),
            ["clientOrderID"] = request.ClientOrderId
        };

        var createResponse = await SendSignedPostAsync<BingXEnvelope<BingXOrderWrapper>>(
            "/openApi/swap/v2/trade/order",
            parameters,
            credentials,
            ct);

        EnsureBingXSuccess(createResponse);

        var orderId = createResponse.Data?.Order?.OrderId?.ToString(CultureInfo.InvariantCulture) ?? "";

        var fill = await GetFillWithRetryAsync(
            exchangeSymbol,
            orderId,
            createResponse.Data?.Order,
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
        BingXOrder? createdOrder,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var order = createdOrder;

        for (var attempt = 0; attempt < 5; attempt++)
        {
            var executedQty = ParseDecimal(order?.ExecutedQty);

            if (executedQty > 0)
            {
                var averagePrice = ParseDecimal(order?.AvgPrice);
                var fee = Math.Abs(ParseDecimal(order?.Commission));

                return (executedQty, averagePrice, fee);
            }

            if (string.IsNullOrEmpty(orderId))
                break;

            await Task.Delay(200, ct);

            var response = await SendSignedGetAsync<BingXEnvelope<BingXOrderWrapper>>(
                "/openApi/swap/v2/trade/order",
                new Dictionary<string, string>
                {
                    ["symbol"] = exchangeSymbol,
                    ["orderId"] = orderId
                },
                credentials,
                ct);

            EnsureBingXSuccess(response);

            order = response.Data?.Order;
        }

        return (0m, 0m, 0m);
    }

    public async Task<ExchangeSymbolRules?> GetSymbolRulesAsync(
        string tradingPair,
        CancellationToken ct)
    {
        var exchangeSymbol = ToBingXSymbol(tradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
            return null;

        var response = await SendPublicGetAsync<BingXEnvelope<List<BingXSymbolInfo>>>(
            "/openApi/swap/v2/quote/contracts",
            ct);

        EnsureBingXSuccess(response);

        var item = response.Data?.FirstOrDefault(x =>
            string.Equals(x.Symbol, exchangeSymbol, StringComparison.OrdinalIgnoreCase));

        if (item is null)
            return null;

        return new ExchangeSymbolRules(
            ConnectorName: ConnectorName,
            TradingPair: InstrumentFilterService.NormalizeTradingPair(tradingPair),
            ExchangeSymbol: item.Symbol ?? exchangeSymbol,
            Status: item.Status?.ToString(CultureInfo.InvariantCulture) ?? "",
            TickSize: ParseDecimal(item.TickSize),
            QuantityStep: ParseDecimal(item.StepSize),
            MinQuantity: ParseDecimal(item.TradeMinQuantity),
            MinNotional: ParseDecimal(item.TradeMinUsdt),
            MaxMarketQuantity: null,
            MaxLimitQuantity: null,
            ReceivedAt: DateTimeOffset.UtcNow);
    }

    private async Task<T> SendSignedGetAsync<T>(
        string path,
        Dictionary<string, string>? extraParameters,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var query = BuildSignedQueryString(
            extraParameters,
            credentials.ApiSecret);

        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"{path}?{query}");

        request.Headers.TryAddWithoutValidation("X-BX-APIKEY", credentials.ApiKey);

        return await SendAsync<T>(
            request,
            ct);
    }

    private async Task<T> SendSignedPostAsync<T>(
        string path,
        Dictionary<string, string> parameters,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var query = BuildSignedQueryString(
            parameters,
            credentials.ApiSecret);

        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            $"{path}?{query}");

        request.Headers.TryAddWithoutValidation("X-BX-APIKEY", credentials.ApiKey);

        return await SendAsync<T>(
            request,
            ct);
    }

    private string BuildSignedQueryString(
        Dictionary<string, string>? extraParameters,
        string apiSecret)
    {
        var parameters = new SortedDictionary<string, string>(
            extraParameters ?? new Dictionary<string, string>(),
            StringComparer.Ordinal)
        {
            ["timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture)
        };

        var query = string.Join(
            "&",
            parameters.Select(x => $"{Uri.EscapeDataString(x.Key)}={Uri.EscapeDataString(x.Value)}"));

        var signature = _signer.Sign(
            apiSecret,
            query);

        return $"{query}&signature={signature}";
    }

    private async Task<T> SendPublicGetAsync<T>(
        string path,
        CancellationToken ct)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            path);

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
                $"BingX HTTP request failed. StatusCode={(int)response.StatusCode}, Body={body}");
        }

        var parsed = JsonSerializer.Deserialize<T>(
            body,
            JsonOptions);

        if (parsed is null)
        {
            throw new InvalidOperationException(
                "BingX response deserialization failed.");
        }

        return parsed;
    }

    private static void EnsureBingXSuccess<T>(BingXEnvelope<T> envelope)
    {
        if (envelope.Code != 0)
        {
            throw new InvalidOperationException(
                $"BingX API returned error. Code={envelope.Code}, Msg={envelope.Msg}");
        }
    }

    private static string ToBingXSymbol(string tradingPair)
    {
        return InstrumentFilterService.NormalizeTradingPair(tradingPair);
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

    private sealed class BingXEnvelope<T>
    {
        public int Code { get; set; }

        public string? Msg { get; set; }

        public T? Data { get; set; }
    }

    private sealed class BingXBalanceData
    {
        public BingXBalance? Balance { get; set; }
    }

    private sealed class BingXBalance
    {
        public string? Asset { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? Balance { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? Equity { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? AvailableMargin { get; set; }
    }

    private sealed class BingXOrderWrapper
    {
        public BingXOrder? Order { get; set; }
    }

    private sealed class BingXOrder
    {
        public long? OrderId { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? ExecutedQty { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? AvgPrice { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? Commission { get; set; }
    }

    private sealed class BingXSymbolInfo
    {
        public string? Symbol { get; set; }

        public int? Status { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? TickSize { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? StepSize { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? TradeMinQuantity { get; set; }

        [JsonConverter(typeof(FlexibleNumericStringConverter))]
        public string? TradeMinUsdt { get; set; }
    }
}
