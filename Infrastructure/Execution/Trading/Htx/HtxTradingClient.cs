using System.Globalization;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Arbitrage.Api.Application.Instruments;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.Htx;

public sealed class HtxTradingClient : IExchangeTradingClient
{
    private readonly HttpClient _httpClient;
    private readonly HtxTradingOptions _options;
    private readonly HtxAuthSigner _signer;
    private readonly ILogger<HtxTradingClient> _logger;

    public HtxTradingClient(
        HttpClient httpClient,
        IOptions<HtxTradingOptions> options,
        HtxAuthSigner signer,
        ILogger<HtxTradingClient> logger)
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

    public string ConnectorName => "htx_perpetual";

    public async Task<ExchangeConnectionCheckResult> CheckConnectionAsync(
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var checkedAt = DateTimeOffset.UtcNow;

        try
        {
            await GetBalancesAsync(
                credentials,
                ct);

            return new ExchangeConnectionCheckResult(
                ConnectorName: ConnectorName,
                IsConnected: true,
                Status: "Connected",
                Error: null,
                CheckedAt: checkedAt);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "HTX connection check failed.");

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
        using var document = await SendSignedGetJsonAsync(
            path: _options.AccountInfoPath,
            credentials: credentials,
            extraParameters: null,
            ct: ct);

        EnsureHtxSuccess(document.RootElement);

        var now = DateTimeOffset.UtcNow;

        var dataElement = TryGetPropertyIgnoreCase(
            document.RootElement,
            "data");

        // v3 unified_account_info returns data as an array with one entry per margin asset
        // (HUSD, BTC, ETH, USDT, ...). We must pick the USDT entry explicitly - a blind
        // recursive search returns the first asset's (usually zero) balance, not USDT's.
        var usdtAccount = FindMarginAsset(dataElement, "USDT");

        var walletBalance = GetDecimal(usdtAccount, "margin_balance");
        var availableBalance = GetDecimal(usdtAccount, "withdraw_available");

        if (availableBalance <= 0)
            availableBalance = GetDecimal(usdtAccount, "margin_available");

        return
        [
            new ExchangeBalanceSnapshot(
                ConnectorName: ConnectorName,
                Asset: "USDT",
                WalletBalance: walletBalance,
                AvailableBalance: availableBalance,
                Equity: walletBalance,
                UsdValue: walletBalance,
                ReceivedAt: now)
        ];
    }

    public async Task<IReadOnlyList<ExchangePositionSnapshot>> GetPositionsAsync(
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        using var document = await SendSignedPostJsonAsync(
            path: _options.PositionInfoPath,
            body: "{}",
            credentials: credentials,
            ct: ct);

        EnsureHtxSuccess(document.RootElement);

        var data = TryGetPropertyIgnoreCase(
            document.RootElement,
            "data");

        var now = DateTimeOffset.UtcNow;
        var result = new List<ExchangePositionSnapshot>();

        if (data.ValueKind != JsonValueKind.Array)
            return result;

        foreach (var item in data.EnumerateArray())
        {
            var volume = GetDecimal(item, "volume");

            if (volume <= 0)
                continue;

            var direction = GetString(item, "direction") ?? "";

            result.Add(new ExchangePositionSnapshot(
                ConnectorName: ConnectorName,
                TradingPair: GetString(item, "contract_code") ?? "",
                Size: volume,
                EntryPrice: GetDecimal(item, "cost_open"),
                MarkPrice: GetDecimal(item, "last_price"),
                UnrealizedPnl: GetDecimal(item, "profit_unreal"),
                Side: direction,
                ReceivedAt: now));
        }

        return result;
    }

    public async Task<IReadOnlyList<ExchangeOpenOrderSnapshot>> GetOpenOrdersAsync(
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        using var document = await SendSignedPostJsonAsync(
            path: _options.OpenOrdersPath,
            body: "{}",
            credentials: credentials,
            ct: ct);

        EnsureHtxSuccess(document.RootElement);

        var data = TryGetPropertyIgnoreCase(
            document.RootElement,
            "data");

        // HTX wraps the list under data.orders; fall back to data itself if it is already an array.
        var orders = TryGetPropertyIgnoreCase(data, "orders");

        if (orders.ValueKind != JsonValueKind.Array)
            orders = data;

        var result = new List<ExchangeOpenOrderSnapshot>();

        if (orders.ValueKind != JsonValueKind.Array)
            return result;

        foreach (var item in orders.EnumerateArray())
        {
            var reduceOnly = (GetString(item, "offset") ?? "")
                .Equals("close", StringComparison.OrdinalIgnoreCase);

            result.Add(new ExchangeOpenOrderSnapshot(
                ConnectorName: ConnectorName,
                ExchangeOrderId: GetString(item, "order_id_str") ?? GetString(item, "order_id") ?? "",
                TradingPair: GetString(item, "contract_code") ?? "",
                Side: GetString(item, "direction") ?? "",
                OrderType: GetString(item, "order_price_type") ?? "",
                Price: GetDecimal(item, "price"),
                Quantity: GetDecimal(item, "volume"),
                FilledQuantity: GetDecimal(item, "trade_volume"),
                ReduceOnly: reduceOnly,
                Status: GetString(item, "status") ?? "",
                CreatedAt: ParseUnixMs(item, "created_at")));
        }

        return result;
    }

    // NOTE: field names follow HTX's documented linear-swap order/order-info shape as of this
    // writing. Verify against the live API/sandbox before enabling real capital (see plan's
    // smoke-test step) - HTX's client_order_id must be a 64-bit integer, not a free-form string,
    // so the attempt Guid is deterministically hashed down to a positive long for idempotency.
    public async Task<OrderFillResult> PlaceOrderAsync(
        ExchangeApiCredentialSecret credentials,
        PlaceOrderRequest request,
        CancellationToken ct)
    {
        var contractCode = InstrumentFilterService.NormalizeTradingPair(request.TradingPair);
        var clientOrderId = ToHtxClientOrderId(request.ClientOrderId);

        var body = JsonSerializer.Serialize(new
        {
            contract_code = contractCode,
            client_order_id = clientOrderId,
            direction = request.Side.Equals("Buy", StringComparison.OrdinalIgnoreCase) ? "buy" : "sell",
            offset = request.ReduceOnly ? "close" : "open",
            lever_rate = 1,
            volume = (long)request.Quantity,
            order_price_type = "optimal_20_ioc",
            reduce_only = request.ReduceOnly ? 1 : 0
        });

        using var createDocument = await SendSignedPostJsonAsync(
            path: _options.OrderPath,
            body: body,
            credentials: credentials,
            ct: ct);

        EnsureHtxSuccess(createDocument.RootElement);

        var createData = TryGetPropertyIgnoreCase(
            createDocument.RootElement,
            "data");

        var orderId = GetString(createData, "order_id_str") ?? GetString(createData, "order_id") ?? "";

        var fill = await GetOrderFillWithRetryAsync(
            contractCode,
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

    private async Task<(decimal FilledQuantity, decimal AveragePrice, decimal FeePaid)> GetOrderFillWithRetryAsync(
        string contractCode,
        string orderId,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        for (var attempt = 0; attempt < 5; attempt++)
        {
            var extraParameters = new Dictionary<string, string>
            {
                ["contract_code"] = contractCode,
                ["order_id"] = orderId
            };

            using var document = await SendSignedGetJsonAsync(
                path: _options.OrderInfoPath,
                credentials: credentials,
                extraParameters: extraParameters,
                ct: ct);

            EnsureHtxSuccess(document.RootElement);

            var data = TryGetPropertyIgnoreCase(
                document.RootElement,
                "data");

            var order = data.ValueKind == JsonValueKind.Array && data.GetArrayLength() > 0
                ? data[0]
                : default;

            var filledQuantity = GetDecimal(order, "trade_volume");

            if (filledQuantity > 0)
            {
                var turnover = GetDecimal(order, "trade_turnover");
                var fee = Math.Abs(GetDecimal(order, "fee"));
                var averagePrice = GetDecimal(order, "trade_avg_price");

                if (averagePrice <= 0 && turnover > 0)
                    averagePrice = turnover / filledQuantity;

                return (filledQuantity, averagePrice, fee);
            }

            await Task.Delay(200, ct);
        }

        return (0m, 0m, 0m);
    }

    private static long ToHtxClientOrderId(string clientOrderId)
    {
        using var sha256 = SHA256.Create();

        var hash = sha256.ComputeHash(Encoding.UTF8.GetBytes(clientOrderId));

        var value = BitConverter.ToInt64(hash, 0);

        return value & 0x7FFFFFFFFFFFFFF;
    }

    private async Task<JsonDocument> SendSignedPostJsonAsync(
        string path,
        string body,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var signedQueryString = _signer.BuildSignedQueryString(
            method: "POST",
            host: _options.SignatureHost,
            path: path,
            accessKey: credentials.ApiKey,
            secretKey: credentials.ApiSecret,
            extraParameters: null);

        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            $"{path.TrimStart('/')}?{signedQueryString}");

        request.Content = new StringContent(
            body,
            Encoding.UTF8,
            "application/json");

        using var response = await _httpClient.SendAsync(
            request,
            ct);

        var responseBody = await response.Content.ReadAsStringAsync(ct);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"HTX HTTP request failed. StatusCode={(int)response.StatusCode}, Body={responseBody}");
        }

        return JsonDocument.Parse(responseBody);
    }

    public async Task<ExchangeSymbolRules?> GetSymbolRulesAsync(
        string tradingPair,
        CancellationToken ct)
    {
        var normalizedTradingPair = InstrumentFilterService.NormalizeTradingPair(tradingPair);

        if (string.IsNullOrWhiteSpace(normalizedTradingPair))
            return null;

        var query = $"contract_code={Uri.EscapeDataString(normalizedTradingPair)}";

        using var document = await SendPublicGetJsonAsync(
            path: _options.ContractInfoPath,
            queryString: query,
            ct: ct);

        EnsureHtxSuccess(document.RootElement);

        var data = TryGetPropertyIgnoreCase(
            document.RootElement,
            "data");

        if (data.ValueKind != JsonValueKind.Array ||
            data.GetArrayLength() == 0)
        {
            return null;
        }

        var item = data[0];

        var contractCode = GetString(
            item,
            "contract_code") ?? normalizedTradingPair;

        var status = GetString(
            item,
            "contract_status") ?? "";

        var tickSize = GetDecimal(
            item,
            "price_tick");

        var contractSize = GetDecimal(
            item,
            "contract_size");

        return new ExchangeSymbolRules(
            ConnectorName: ConnectorName,
            TradingPair: normalizedTradingPair,
            ExchangeSymbol: contractCode,
            Status: status,
            TickSize: tickSize,
            QuantityStep: 1m,
            MinQuantity: 1m,
            MinNotional: contractSize,
            MaxMarketQuantity: null,
            MaxLimitQuantity: null,
            ReceivedAt: DateTimeOffset.UtcNow);
    }

    private async Task<JsonDocument> SendSignedGetJsonAsync(
        string path,
        ExchangeApiCredentialSecret credentials,
        IReadOnlyDictionary<string, string>? extraParameters,
        CancellationToken ct)
    {
        var signedQueryString = _signer.BuildSignedQueryString(
            method: "GET",
            host: _options.SignatureHost,
            path: path,
            accessKey: credentials.ApiKey,
            secretKey: credentials.ApiSecret,
            extraParameters: extraParameters);

        return await SendGetJsonAsync(
            $"{path}?{signedQueryString}",
            ct);
    }

    private async Task<JsonDocument> SendPublicGetJsonAsync(
        string path,
        string queryString,
        CancellationToken ct)
    {
        return await SendGetJsonAsync(
            $"{path}?{queryString}",
            ct);
    }

    private async Task<JsonDocument> SendGetJsonAsync(
        string requestUri,
        CancellationToken ct)
    {
        using var response = await _httpClient.GetAsync(
            requestUri.TrimStart('/'),
            ct);

        var body = await response.Content.ReadAsStringAsync(ct);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"HTX HTTP request failed. StatusCode={(int)response.StatusCode}, Body={body}");
        }

        var document = JsonDocument.Parse(body);

        return document;
    }
    private static int? GetIntLike(
    JsonElement element,
    string propertyName)
    {
        var property = TryGetPropertyIgnoreCase(
            element,
            propertyName);

        if (property.ValueKind == JsonValueKind.Number &&
            property.TryGetInt32(out var number))
        {
            return number;
        }

        if (property.ValueKind == JsonValueKind.String &&
            int.TryParse(
                property.GetString(),
                NumberStyles.Any,
                CultureInfo.InvariantCulture,
                out var parsed))
        {
            return parsed;
        }

        return null;
    }
    private static void EnsureHtxSuccess(JsonElement root)
    {
        var status = GetString(root, "status");

        if (string.Equals(status, "ok", StringComparison.OrdinalIgnoreCase))
            return;

        var code =
            GetIntLike(root, "code") ??
            GetIntLike(root, "err_code") ??
            GetIntLike(root, "err-code");

        if (code == 200)
            return;

        var errCode = GetString(root, "err_code")
            ?? GetString(root, "err-code")
            ?? GetString(root, "code");

        var errMsg = GetString(root, "err_msg")
            ?? GetString(root, "err-msg")
            ?? GetString(root, "message")
            ?? GetString(root, "msg");

        throw new InvalidOperationException(
            $"HTX API returned error. Status={status}, ErrCode={errCode}, ErrMsg={errMsg}");
    }

    private static JsonElement TryGetPropertyIgnoreCase(
        JsonElement element,
        string propertyName)
    {
        if (element.ValueKind != JsonValueKind.Object)
            return default;

        foreach (var property in element.EnumerateObject())
        {
            if (property.Name.Equals(
                    propertyName,
                    StringComparison.OrdinalIgnoreCase))
            {
                return property.Value;
            }
        }

        return default;
    }

    private static string? GetString(
        JsonElement element,
        string propertyName)
    {
        var property = TryGetPropertyIgnoreCase(
            element,
            propertyName);

        if (property.ValueKind == JsonValueKind.String)
            return property.GetString();

        if (property.ValueKind == JsonValueKind.Number)
            return property.GetRawText();

        return null;
    }

    private static decimal GetDecimal(
        JsonElement element,
        string propertyName)
    {
        var property = TryGetPropertyIgnoreCase(
            element,
            propertyName);

        return ParseDecimal(property);
    }

    private static DateTimeOffset ParseUnixMs(
        JsonElement element,
        string propertyName)
    {
        var property = TryGetPropertyIgnoreCase(
            element,
            propertyName);

        long ms = 0;

        if (property.ValueKind == JsonValueKind.Number)
            property.TryGetInt64(out ms);
        else if (property.ValueKind == JsonValueKind.String)
            long.TryParse(property.GetString(), NumberStyles.Any, CultureInfo.InvariantCulture, out ms);

        return ms > 0
            ? DateTimeOffset.FromUnixTimeMilliseconds(ms)
            : DateTimeOffset.UtcNow;
    }

    /// <summary>
    /// Selects the account entry for a given margin asset (e.g. "USDT") out of HTX's
    /// per-asset <c>data</c> array. Returns <c>default</c> if the array or asset is absent.
    /// </summary>
    private static JsonElement FindMarginAsset(
        JsonElement dataElement,
        string marginAsset)
    {
        if (dataElement.ValueKind != JsonValueKind.Array)
            return default;

        foreach (var item in dataElement.EnumerateArray())
        {
            var asset = GetString(item, "margin_asset");

            if (string.Equals(asset, marginAsset, StringComparison.OrdinalIgnoreCase))
                return item;
        }

        return default;
    }

    private static decimal ParseDecimal(JsonElement element)
    {
        return ParseNullableDecimal(element) ?? 0m;
    }

    private static decimal? ParseNullableDecimal(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Number &&
            element.TryGetDecimal(out var number))
        {
            return number;
        }

        if (element.ValueKind == JsonValueKind.String &&
            decimal.TryParse(
                element.GetString(),
                NumberStyles.Any,
                CultureInfo.InvariantCulture,
                out var parsed))
        {
            return parsed;
        }

        return null;
    }
}