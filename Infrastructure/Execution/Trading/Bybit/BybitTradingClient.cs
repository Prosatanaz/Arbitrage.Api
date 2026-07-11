using System.Collections.Concurrent;
using System.Globalization;
using System.Net.Http.Headers;
using System.Text.Json;
using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Arbitrage.Api.Application.Instruments;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.Bybit;

public sealed class BybitTradingClient : IExchangeTradingClient
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient _httpClient;
    private readonly BybitTradingOptions _options;
    private readonly BybitAuthSigner _signer;
    private readonly ILogger<BybitTradingClient> _logger;

    public BybitTradingClient(
        HttpClient httpClient,
        IOptions<BybitTradingOptions> options,
        BybitAuthSigner signer,
        ILogger<BybitTradingClient> logger)
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

    public string ConnectorName => "bybit_perpetual";

    // Real signed position query (v5/position/list) - trusted to confirm the account is flat.
    public bool SupportsPositionReads => true;

    // Bybit "not modified" ret-codes, treated as success: the desired margin mode / leverage was
    // already in place (110026 = margin mode unchanged, 110043 = leverage unchanged).
    private const int RetCodeMarginModeNotModified = 110026;
    private const int RetCodeLeverageNotModified = 110043;

    // Symbols whose margin mode + 1x leverage have already been enforced this process, so the
    // config calls are made once per symbol rather than before every order.
    private readonly ConcurrentDictionary<string, bool> _marginConfigured = new(StringComparer.OrdinalIgnoreCase);

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
                "Bybit connection check failed.");

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
        var queryString = $"accountType={Uri.EscapeDataString(_options.AccountType)}&coin=USDT";

        var response = await SendSignedGetAsync<BybitWalletBalanceResponse>(
            path: "v5/account/wallet-balance",
            queryString: queryString,
            credentials: credentials,
            ct: ct);

        var now = DateTimeOffset.UtcNow;
        var result = new List<ExchangeBalanceSnapshot>();

        foreach (var account in response.Result?.List ?? [])
        {
            foreach (var coin in account.Coin ?? [])
            {
                result.Add(new ExchangeBalanceSnapshot(
                    ConnectorName: ConnectorName,
                    Asset: coin.Coin ?? "",
                    WalletBalance: ParseDecimal(coin.WalletBalance),
                    AvailableBalance: ParseDecimal(account.TotalAvailableBalance),
                    Equity: ParseDecimal(coin.Equity),
                    UsdValue: ParseDecimal(coin.UsdValue),
                    ReceivedAt: now));
            }
        }

        return result;
    }

    public async Task<IReadOnlyList<ExchangePositionSnapshot>> GetPositionsAsync(
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var queryString =
            $"category={Uri.EscapeDataString(_options.Category)}&settleCoin=USDT";

        var response = await SendSignedGetAsync<BybitPositionListResponse>(
            path: "v5/position/list",
            queryString: queryString,
            credentials: credentials,
            ct: ct);

        var now = DateTimeOffset.UtcNow;
        var result = new List<ExchangePositionSnapshot>();

        foreach (var item in response.Result?.List ?? [])
        {
            var size = ParseDecimal(item.Size);

            // Bybit returns a row per symbol even with zero size; only surface live exposure.
            if (size <= 0)
                continue;

            result.Add(new ExchangePositionSnapshot(
                ConnectorName: ConnectorName,
                TradingPair: item.Symbol ?? "",
                Size: size,
                EntryPrice: ParseDecimal(item.AvgPrice),
                MarkPrice: ParseDecimal(item.MarkPrice),
                UnrealizedPnl: ParseDecimal(item.UnrealisedPnl),
                Side: item.Side ?? "",
                ReceivedAt: now));
        }

        return result;
    }

    public async Task<IReadOnlyList<ExchangeOpenOrderSnapshot>> GetOpenOrdersAsync(
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var queryString =
            $"category={Uri.EscapeDataString(_options.Category)}&settleCoin=USDT&openOnly=0&limit=50";

        var response = await SendSignedGetAsync<BybitOpenOrderResponse>(
            path: "v5/order/realtime",
            queryString: queryString,
            credentials: credentials,
            ct: ct);

        var result = new List<ExchangeOpenOrderSnapshot>();

        foreach (var item in response.Result?.List ?? [])
        {
            result.Add(new ExchangeOpenOrderSnapshot(
                ConnectorName: ConnectorName,
                ExchangeOrderId: item.OrderId ?? "",
                TradingPair: item.Symbol ?? "",
                Side: item.Side ?? "",
                OrderType: item.OrderType ?? "",
                Price: ParseDecimal(item.Price),
                Quantity: ParseDecimal(item.Qty),
                FilledQuantity: ParseDecimal(item.CumExecQty),
                ReduceOnly: item.ReduceOnly ?? false,
                Status: item.OrderStatus ?? "",
                CreatedAt: ParseUnixMs(item.CreatedTime)));
        }

        return result;
    }

    /// <summary>
    /// Forces the symbol to isolated margin at 1x leverage before an opening order, so real
    /// positions are never opened on the account's default (leveraged) settings. Isolated is
    /// best-effort (it is account-level and not per-symbol on Unified accounts, so a failure there
    /// is logged, not fatal); the 1x leverage IS enforced - if it cannot be set, the order is not
    /// placed. Both calls tolerate Bybit's "not modified" codes (already at the desired value) and
    /// the result is cached per symbol so it runs once, not before every order.
    /// </summary>
    private async Task EnsureIsolatedOneXLeverageAsync(
        string exchangeSymbol,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        if (_marginConfigured.ContainsKey(exchangeSymbol))
            return;

        // Best-effort: switch to isolated margin at 1x. On Unified accounts margin mode is
        // account-level, so this can legitimately fail - log and continue to the leverage call.
        try
        {
            var isolatedBody = JsonSerializer.Serialize(new
            {
                category = _options.Category,
                symbol = exchangeSymbol,
                tradeMode = 1, // 0 = cross, 1 = isolated
                buyLeverage = "1",
                sellLeverage = "1"
            });

            await SendSignedPostAsync<BybitSimpleResponse>(
                path: "v5/position/switch-isolated",
                body: isolatedBody,
                credentials: credentials,
                ct: ct,
                toleratedRetCodes: new[] { RetCodeMarginModeNotModified, RetCodeLeverageNotModified });
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Bybit switch-isolated failed for {Symbol}; proceeding to enforce 1x leverage only.",
                exchangeSymbol);
        }

        // Mandatory: enforce 1x leverage. "Not modified" means it is already 1x (success). Any other
        // failure means we cannot guarantee no leverage, so the exception propagates and the opening
        // order is not placed - fail closed.
        var leverageBody = JsonSerializer.Serialize(new
        {
            category = _options.Category,
            symbol = exchangeSymbol,
            buyLeverage = "1",
            sellLeverage = "1"
        });

        await SendSignedPostAsync<BybitSimpleResponse>(
            path: "v5/position/set-leverage",
            body: leverageBody,
            credentials: credentials,
            ct: ct,
            toleratedRetCodes: new[] { RetCodeLeverageNotModified });

        _marginConfigured[exchangeSymbol] = true;
    }

    public async Task<OrderFillResult> PlaceOrderAsync(
        ExchangeApiCredentialSecret credentials,
        PlaceOrderRequest request,
        CancellationToken ct)
    {
        var exchangeSymbol = ToBybitSymbol(request.TradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
        {
            throw new InvalidOperationException(
                $"Bybit symbol could not be derived for trading pair '{request.TradingPair}'.");
        }

        // The carry-trade design is explicitly no-leverage. Bybit otherwise opens at the account's
        // default leverage (commonly 10x), which - with no stop-loss and a thesis that a WIDER basis
        // is a stronger signal - risks one leg being liquidated during divergence and turning the
        // hedge into a naked position. Enforce isolated 1x before any opening order. Skipped for
        // reduce-only closes (nothing to configure when flattening).
        if (!request.ReduceOnly)
            await EnsureIsolatedOneXLeverageAsync(exchangeSymbol, credentials, ct);

        var body = JsonSerializer.Serialize(new
        {
            category = _options.Category,
            symbol = exchangeSymbol,
            side = request.Side,
            orderType = "Market",
            qty = request.Quantity.ToString(CultureInfo.InvariantCulture),
            reduceOnly = request.ReduceOnly,
            timeInForce = "IOC",
            orderLinkId = request.ClientOrderId
        });

        var response = await SendSignedPostAsync<BybitOrderCreateResponse>(
            path: "v5/order/create",
            body: body,
            credentials: credentials,
            ct: ct);

        var orderId = response.Result?.OrderId ?? "";

        var executions = await GetExecutionsWithRetryAsync(
            exchangeSymbol,
            orderId,
            credentials,
            ct);

        var filledQuantity = executions.Sum(x => ParseDecimal(x.ExecQty));
        var filledNotional = executions.Sum(x => ParseDecimal(x.ExecQty) * ParseDecimal(x.ExecPrice));
        var feePaid = executions.Sum(x => ParseDecimal(x.ExecFee));
        var averagePrice = filledQuantity > 0 ? filledNotional / filledQuantity : 0m;

        return new OrderFillResult(
            ConnectorName: ConnectorName,
            ClientOrderId: request.ClientOrderId,
            ExchangeOrderId: orderId,
            IsFilled: filledQuantity > 0,
            FilledQuantity: filledQuantity,
            AverageFillPrice: averagePrice,
            FeePaidUsd: feePaid,
            Status: filledQuantity >= request.Quantity ? "Filled" : filledQuantity > 0 ? "PartiallyFilled" : "Unfilled",
            FilledAt: DateTimeOffset.UtcNow);
    }

    private async Task<List<BybitExecution>> GetExecutionsWithRetryAsync(
        string exchangeSymbol,
        string orderId,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        for (var attempt = 0; attempt < 5; attempt++)
        {
            var queryString =
                $"category={Uri.EscapeDataString(_options.Category)}&symbol={Uri.EscapeDataString(exchangeSymbol)}&orderId={Uri.EscapeDataString(orderId)}";

            var response = await SendSignedGetAsync<BybitExecutionListResponse>(
                path: "v5/execution/list",
                queryString: queryString,
                credentials: credentials,
                ct: ct);

            var executions = response.Result?.List ?? [];

            if (executions.Count > 0)
                return executions;

            await Task.Delay(200, ct);
        }

        return [];
    }

    private async Task<T> SendSignedPostAsync<T>(
        string path,
        string body,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct,
        int[]? toleratedRetCodes = null)
    {
        var timestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);
        var recvWindow = _options.RecvWindowMs.ToString(CultureInfo.InvariantCulture);

        var signature = _signer.SignGet(
            timestampMs,
            credentials.ApiKey,
            credentials.ApiSecret,
            recvWindow,
            body);

        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            path);

        request.Headers.TryAddWithoutValidation("X-BAPI-API-KEY", credentials.ApiKey);
        request.Headers.TryAddWithoutValidation("X-BAPI-TIMESTAMP", timestampMs);
        request.Headers.TryAddWithoutValidation("X-BAPI-RECV-WINDOW", recvWindow);
        request.Headers.TryAddWithoutValidation("X-BAPI-SIGN", signature);
        request.Content = new StringContent(
            body,
            System.Text.Encoding.UTF8,
            "application/json");

        using var response = await _httpClient.SendAsync(
            request,
            ct);

        var responseBody = await response.Content.ReadAsStringAsync(ct);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"Bybit HTTP request failed. StatusCode={(int)response.StatusCode}, Body={responseBody}");
        }

        var parsed = JsonSerializer.Deserialize<T>(
            responseBody,
            JsonOptions);

        if (parsed is null)
        {
            throw new InvalidOperationException(
                "Bybit response deserialization failed.");
        }

        ValidateBybitEnvelope(parsed, responseBody, toleratedRetCodes);

        return parsed;
    }

    public async Task<ExchangeSymbolRules?> GetSymbolRulesAsync(
        string tradingPair,
        CancellationToken ct)
    {
        var exchangeSymbol = ToBybitSymbol(tradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
            return null;

        var queryString =
            $"category={Uri.EscapeDataString(_options.Category)}&symbol={Uri.EscapeDataString(exchangeSymbol)}";

        var response = await SendPublicGetAsync<BybitInstrumentsInfoResponse>(
            path: "v5/market/instruments-info",
            queryString: queryString,
            ct: ct);

        var item = response.Result?.List?.FirstOrDefault();

        if (item is null)
            return null;

        return new ExchangeSymbolRules(
            ConnectorName: ConnectorName,
            TradingPair: InstrumentFilterService.NormalizeTradingPair(tradingPair),
            ExchangeSymbol: item.Symbol ?? exchangeSymbol,
            Status: item.Status ?? "",
            TickSize: ParseDecimal(item.PriceFilter?.TickSize),
            QuantityStep: ParseDecimal(item.LotSizeFilter?.QtyStep),
            MinQuantity: ParseDecimal(item.LotSizeFilter?.MinOrderQty),
            MinNotional: ParseDecimal(item.LotSizeFilter?.MinNotionalValue),
            MaxMarketQuantity: ParseNullableDecimal(item.LotSizeFilter?.MaxMktOrderQty),
            MaxLimitQuantity: ParseNullableDecimal(item.LotSizeFilter?.MaxOrderQty),
            ReceivedAt: DateTimeOffset.UtcNow);
    }

    private async Task<T> SendSignedGetAsync<T>(
        string path,
        string queryString,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var timestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);
        var recvWindow = _options.RecvWindowMs.ToString(CultureInfo.InvariantCulture);

        var signature = _signer.SignGet(
            timestampMs,
            credentials.ApiKey,
            credentials.ApiSecret,
            recvWindow,
            queryString);

        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"{path}?{queryString}");

        request.Headers.TryAddWithoutValidation("X-BAPI-API-KEY", credentials.ApiKey);
        request.Headers.TryAddWithoutValidation("X-BAPI-TIMESTAMP", timestampMs);
        request.Headers.TryAddWithoutValidation("X-BAPI-RECV-WINDOW", recvWindow);
        request.Headers.TryAddWithoutValidation("X-BAPI-SIGN", signature);

        using var response = await _httpClient.SendAsync(
            request,
            ct);

        var body = await response.Content.ReadAsStringAsync(ct);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"Bybit HTTP request failed. StatusCode={(int)response.StatusCode}, Body={body}");
        }

        var parsed = JsonSerializer.Deserialize<T>(
            body,
            JsonOptions);

        if (parsed is null)
        {
            throw new InvalidOperationException(
                "Bybit response deserialization failed.");
        }

        ValidateBybitEnvelope(parsed, body);

        return parsed;
    }

    private async Task<T> SendPublicGetAsync<T>(
        string path,
        string queryString,
        CancellationToken ct)
    {
        using var response = await _httpClient.GetAsync(
            $"{path}?{queryString}",
            ct);

        var body = await response.Content.ReadAsStringAsync(ct);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"Bybit public HTTP request failed. StatusCode={(int)response.StatusCode}, Body={body}");
        }

        var parsed = JsonSerializer.Deserialize<T>(
            body,
            JsonOptions);

        if (parsed is null)
        {
            throw new InvalidOperationException(
                "Bybit public response deserialization failed.");
        }

        ValidateBybitEnvelope(parsed, body);

        return parsed;
    }

    private static void ValidateBybitEnvelope<T>(
        T parsed,
        string rawBody,
        int[]? toleratedRetCodes = null)
    {
        if (parsed is not IBybitResponseEnvelope envelope)
            return;

        if (envelope.RetCode != 0 &&
            (toleratedRetCodes is null || !toleratedRetCodes.Contains(envelope.RetCode)))
        {
            throw new InvalidOperationException(
                $"Bybit API returned error. RetCode={envelope.RetCode}, RetMsg={envelope.RetMsg}, Body={rawBody}");
        }
    }

    private static string ToBybitSymbol(string tradingPair)
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

    private static DateTimeOffset ParseUnixMs(string? value)
    {
        if (long.TryParse(
                value,
                NumberStyles.Any,
                CultureInfo.InvariantCulture,
                out var ms) && ms > 0)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(ms);
        }

        return DateTimeOffset.UtcNow;
    }

    private interface IBybitResponseEnvelope
    {
        int RetCode { get; }

        string RetMsg { get; }
    }

    private sealed class BybitWalletBalanceResponse : IBybitResponseEnvelope
    {
        public int RetCode { get; set; }

        public string RetMsg { get; set; } = "";

        public BybitWalletBalanceResult? Result { get; set; }
    }

    private sealed class BybitWalletBalanceResult
    {
        public List<BybitWalletAccount>? List { get; set; }
    }

    private sealed class BybitWalletAccount
    {
        public string? AccountType { get; set; }

        public string? TotalEquity { get; set; }

        public string? TotalWalletBalance { get; set; }

        public string? TotalAvailableBalance { get; set; }

        public List<BybitWalletCoin>? Coin { get; set; }
    }

    private sealed class BybitWalletCoin
    {
        public string? Coin { get; set; }

        public string? Equity { get; set; }

        public string? WalletBalance { get; set; }

        public string? UsdValue { get; set; }

        public string? UnrealisedPnl { get; set; }
    }

    private sealed class BybitOrderCreateResponse : IBybitResponseEnvelope
    {
        public int RetCode { get; set; }

        public string RetMsg { get; set; } = "";

        public BybitOrderCreateResult? Result { get; set; }
    }

    // Envelope for config endpoints (set-leverage, switch-isolated) where only the ret-code matters.
    private sealed class BybitSimpleResponse : IBybitResponseEnvelope
    {
        public int RetCode { get; set; }

        public string RetMsg { get; set; } = "";
    }

    private sealed class BybitOrderCreateResult
    {
        public string? OrderId { get; set; }

        public string? OrderLinkId { get; set; }
    }

    private sealed class BybitExecutionListResponse : IBybitResponseEnvelope
    {
        public int RetCode { get; set; }

        public string RetMsg { get; set; } = "";

        public BybitExecutionListResult? Result { get; set; }
    }

    private sealed class BybitExecutionListResult
    {
        public List<BybitExecution>? List { get; set; }
    }

    private sealed class BybitExecution
    {
        public string? ExecQty { get; set; }

        public string? ExecPrice { get; set; }

        public string? ExecFee { get; set; }
    }

    private sealed class BybitPositionListResponse : IBybitResponseEnvelope
    {
        public int RetCode { get; set; }

        public string RetMsg { get; set; } = "";

        public BybitPositionListResult? Result { get; set; }
    }

    private sealed class BybitPositionListResult
    {
        public List<BybitPosition>? List { get; set; }
    }

    private sealed class BybitPosition
    {
        public string? Symbol { get; set; }

        public string? Side { get; set; }

        public string? Size { get; set; }

        public string? AvgPrice { get; set; }

        public string? MarkPrice { get; set; }

        public string? UnrealisedPnl { get; set; }
    }

    private sealed class BybitOpenOrderResponse : IBybitResponseEnvelope
    {
        public int RetCode { get; set; }

        public string RetMsg { get; set; } = "";

        public BybitOpenOrderResult? Result { get; set; }
    }

    private sealed class BybitOpenOrderResult
    {
        public List<BybitOpenOrder>? List { get; set; }
    }

    private sealed class BybitOpenOrder
    {
        public string? OrderId { get; set; }

        public string? Symbol { get; set; }

        public string? Side { get; set; }

        public string? OrderType { get; set; }

        public string? Price { get; set; }

        public string? Qty { get; set; }

        public string? CumExecQty { get; set; }

        public bool? ReduceOnly { get; set; }

        public string? OrderStatus { get; set; }

        public string? CreatedTime { get; set; }
    }

    private sealed class BybitInstrumentsInfoResponse : IBybitResponseEnvelope
    {
        public int RetCode { get; set; }

        public string RetMsg { get; set; } = "";

        public BybitInstrumentsInfoResult? Result { get; set; }
    }

    private sealed class BybitInstrumentsInfoResult
    {
        public string? Category { get; set; }

        public List<BybitInstrumentInfo>? List { get; set; }
    }

    private sealed class BybitInstrumentInfo
    {
        public string? Symbol { get; set; }

        public string? Status { get; set; }

        public string? BaseCoin { get; set; }

        public string? QuoteCoin { get; set; }

        public BybitPriceFilter? PriceFilter { get; set; }

        public BybitLotSizeFilter? LotSizeFilter { get; set; }
    }

    private sealed class BybitPriceFilter
    {
        public string? TickSize { get; set; }
    }

    private sealed class BybitLotSizeFilter
    {
        public string? MinNotionalValue { get; set; }

        public string? MaxOrderQty { get; set; }

        public string? MaxMktOrderQty { get; set; }

        public string? MinOrderQty { get; set; }

        public string? QtyStep { get; set; }
    }
}