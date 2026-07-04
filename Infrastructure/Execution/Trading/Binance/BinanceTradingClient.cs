using System.Globalization;
using System.Net.Http.Headers;
using System.Text.Json;
using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Arbitrage.Api.Application.Instruments;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.Binance;

// NOTE: field/endpoint names follow Binance's documented USDT-M futures (fapi) shape as of this
// writing (general API knowledge, not re-verified against live docs for this pass). Verify with a
// real smoke test before trusting fills - see feedback memory on unverified exchange details.
public sealed class BinanceTradingClient : IExchangeTradingClient
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient _httpClient;
    private readonly BinanceTradingOptions _options;
    private readonly BinanceAuthSigner _signer;
    private readonly ILogger<BinanceTradingClient> _logger;

    public BinanceTradingClient(
        HttpClient httpClient,
        IOptions<BinanceTradingOptions> options,
        BinanceAuthSigner signer,
        ILogger<BinanceTradingClient> logger)
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

    public string ConnectorName => "binance_perpetual";

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
                "Binance connection check failed.");

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
        var response = await SendSignedGetAsync<List<BinanceBalanceEntry>>(
            path: "fapi/v2/balance",
            extraParameters: null,
            credentials: credentials,
            ct: ct);

        var now = DateTimeOffset.UtcNow;

        return response
            .Select(x => new ExchangeBalanceSnapshot(
                ConnectorName: ConnectorName,
                Asset: x.Asset ?? "",
                WalletBalance: ParseDecimal(x.Balance),
                AvailableBalance: ParseDecimal(x.AvailableBalance),
                Equity: ParseDecimal(x.Balance),
                UsdValue: ParseDecimal(x.Balance),
                ReceivedAt: now))
            .ToList();
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
        var exchangeSymbol = ToBinanceSymbol(request.TradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
        {
            throw new InvalidOperationException(
                $"Binance symbol could not be derived for trading pair '{request.TradingPair}'.");
        }

        var side = request.Side.Equals("Buy", StringComparison.OrdinalIgnoreCase) ? "BUY" : "SELL";

        var parameters = new Dictionary<string, string>
        {
            ["symbol"] = exchangeSymbol,
            ["side"] = side,
            ["type"] = "MARKET",
            ["quantity"] = request.Quantity.ToString(CultureInfo.InvariantCulture),
            ["newClientOrderId"] = request.ClientOrderId
        };

        if (request.ReduceOnly)
            parameters["reduceOnly"] = "true";

        var createResponse = await SendSignedPostAsync<BinanceOrderResponse>(
            path: "fapi/v1/order",
            parameters: parameters,
            credentials: credentials,
            ct: ct);

        var orderId = createResponse.OrderId?.ToString(CultureInfo.InvariantCulture) ?? "";

        var fill = await GetFillWithRetryAsync(
            exchangeSymbol,
            orderId,
            createResponse,
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
        BinanceOrderResponse createResponse,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var executedQty = ParseDecimal(createResponse.ExecutedQty);

        if (executedQty > 0)
        {
            var avgPrice = ParseDecimal(createResponse.AvgPrice);
            var cumQuote = ParseDecimal(createResponse.CumQuote);

            if (avgPrice <= 0 && cumQuote > 0)
                avgPrice = cumQuote / executedQty;

            var fee = await SumTradeFeesAsync(
                exchangeSymbol,
                orderId,
                credentials,
                ct);

            return (executedQty, avgPrice, fee);
        }

        for (var attempt = 0; attempt < 5; attempt++)
        {
            await Task.Delay(200, ct);

            var order = await SendSignedGetAsync<BinanceOrderResponse>(
                path: "fapi/v1/order",
                extraParameters: new Dictionary<string, string>
                {
                    ["symbol"] = exchangeSymbol,
                    ["orderId"] = orderId
                },
                credentials: credentials,
                ct: ct);

            var qty = ParseDecimal(order.ExecutedQty);

            if (qty > 0)
            {
                var avgPrice = ParseDecimal(order.AvgPrice);
                var cumQuote = ParseDecimal(order.CumQuote);

                if (avgPrice <= 0 && cumQuote > 0)
                    avgPrice = cumQuote / qty;

                var fee = await SumTradeFeesAsync(
                    exchangeSymbol,
                    orderId,
                    credentials,
                    ct);

                return (qty, avgPrice, fee);
            }
        }

        return (0m, 0m, 0m);
    }

    private async Task<decimal> SumTradeFeesAsync(
        string exchangeSymbol,
        string orderId,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        try
        {
            var trades = await SendSignedGetAsync<List<BinanceUserTrade>>(
                path: "fapi/v1/userTrades",
                extraParameters: new Dictionary<string, string>
                {
                    ["symbol"] = exchangeSymbol,
                    ["orderId"] = orderId
                },
                credentials: credentials,
                ct: ct);

            return trades.Sum(x => ParseDecimal(x.Commission));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Binance user trades lookup failed, treating fee as zero.");

            return 0m;
        }
    }

    public async Task<ExchangeSymbolRules?> GetSymbolRulesAsync(
        string tradingPair,
        CancellationToken ct)
    {
        var exchangeSymbol = ToBinanceSymbol(tradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
            return null;

        var response = await SendPublicGetAsync<BinanceExchangeInfoResponse>(
            path: "fapi/v1/exchangeInfo",
            queryString: "",
            ct: ct);

        var item = response.Symbols?.FirstOrDefault(x =>
            string.Equals(x.Symbol, exchangeSymbol, StringComparison.OrdinalIgnoreCase));

        if (item is null)
            return null;

        var priceFilter = item.Filters?.FirstOrDefault(x => x.FilterType == "PRICE_FILTER");
        var lotSizeFilter = item.Filters?.FirstOrDefault(x => x.FilterType == "LOT_SIZE");
        var marketLotSizeFilter = item.Filters?.FirstOrDefault(x => x.FilterType == "MARKET_LOT_SIZE");
        var minNotionalFilter = item.Filters?.FirstOrDefault(x => x.FilterType == "MIN_NOTIONAL");

        return new ExchangeSymbolRules(
            ConnectorName: ConnectorName,
            TradingPair: InstrumentFilterService.NormalizeTradingPair(tradingPair),
            ExchangeSymbol: item.Symbol ?? exchangeSymbol,
            Status: item.Status ?? "",
            TickSize: ParseDecimal(priceFilter?.TickSize),
            QuantityStep: ParseDecimal(lotSizeFilter?.StepSize),
            MinQuantity: ParseDecimal(lotSizeFilter?.MinQty),
            MinNotional: ParseDecimal(minNotionalFilter?.Notional),
            MaxMarketQuantity: ParseNullableDecimal(marketLotSizeFilter?.MaxQty),
            MaxLimitQuantity: ParseNullableDecimal(lotSizeFilter?.MaxQty),
            ReceivedAt: DateTimeOffset.UtcNow);
    }

    private async Task<T> SendSignedGetAsync<T>(
        string path,
        IReadOnlyDictionary<string, string>? extraParameters,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var query = BuildSignedQueryString(
            extraParameters,
            credentials.ApiSecret);

        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"{path}?{query}");

        request.Headers.TryAddWithoutValidation("X-MBX-APIKEY", credentials.ApiKey);

        return await SendAsync<T>(
            request,
            ct);
    }

    private async Task<T> SendSignedPostAsync<T>(
        string path,
        IReadOnlyDictionary<string, string> parameters,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var query = BuildSignedQueryString(
            parameters,
            credentials.ApiSecret);

        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            $"{path}?{query}");

        request.Headers.TryAddWithoutValidation("X-MBX-APIKEY", credentials.ApiKey);

        return await SendAsync<T>(
            request,
            ct);
    }

    private string BuildSignedQueryString(
        IReadOnlyDictionary<string, string>? extraParameters,
        string apiSecret)
    {
        var parameters = new Dictionary<string, string>(extraParameters ?? new Dictionary<string, string>())
        {
            ["timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture),
            ["recvWindow"] = _options.RecvWindowMs.ToString(CultureInfo.InvariantCulture)
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
        string queryString,
        CancellationToken ct)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            string.IsNullOrEmpty(queryString) ? path : $"{path}?{queryString}");

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
                $"Binance HTTP request failed. StatusCode={(int)response.StatusCode}, Body={body}");
        }

        var parsed = JsonSerializer.Deserialize<T>(
            body,
            JsonOptions);

        if (parsed is null)
        {
            throw new InvalidOperationException(
                "Binance response deserialization failed.");
        }

        return parsed;
    }

    private static string ToBinanceSymbol(string tradingPair)
    {
        var normalized = InstrumentFilterService.NormalizeTradingPair(tradingPair);

        var parts = normalized.Split(
            '-',
            StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (parts.Length != 2)
            return "";

        return parts[0] + parts[1];
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

    private sealed class BinanceBalanceEntry
    {
        public string? Asset { get; set; }

        public string? Balance { get; set; }

        public string? AvailableBalance { get; set; }
    }

    private sealed class BinanceOrderResponse
    {
        public long? OrderId { get; set; }

        public string? Status { get; set; }

        public string? ExecutedQty { get; set; }

        public string? CumQuote { get; set; }

        public string? AvgPrice { get; set; }
    }

    private sealed class BinanceUserTrade
    {
        public string? Commission { get; set; }
    }

    private sealed class BinanceExchangeInfoResponse
    {
        public List<BinanceSymbolInfo>? Symbols { get; set; }
    }

    private sealed class BinanceSymbolInfo
    {
        public string? Symbol { get; set; }

        public string? Status { get; set; }

        public List<BinanceSymbolFilter>? Filters { get; set; }
    }

    private sealed class BinanceSymbolFilter
    {
        public string FilterType { get; set; } = "";

        public string? TickSize { get; set; }

        public string? StepSize { get; set; }

        public string? MinQty { get; set; }

        public string? MaxQty { get; set; }

        public string? Notional { get; set; }
    }
}
