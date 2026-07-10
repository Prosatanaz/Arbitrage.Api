using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Arbitrage.Api.Application.Instruments;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.Bitget;

// NOTE: field/endpoint names follow Bitget's documented v2 mix (USDT-FUTURES) API shape as of this
// writing (general API knowledge, not re-verified against live docs for this pass). Verify with a
// real smoke test before trusting fills - see feedback memory on unverified exchange details.
// Bitget requires a passphrase (set when creating the API key) in addition to key/secret.
public sealed class BitgetTradingClient : IExchangeTradingClient
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient _httpClient;
    private readonly BitgetTradingOptions _options;
    private readonly BitgetAuthSigner _signer;
    private readonly ILogger<BitgetTradingClient> _logger;

    public BitgetTradingClient(
        HttpClient httpClient,
        IOptions<BitgetTradingOptions> options,
        BitgetAuthSigner signer,
        ILogger<BitgetTradingClient> logger)
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

    public string ConnectorName => "bitget_perpetual";

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
                "Bitget connection check failed.");

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
        var queryString = $"productType={Uri.EscapeDataString(_options.ProductType)}";

        var response = await SendSignedGetAsync<BitgetEnvelope<List<BitgetAccount>>>(
            path: "/api/v2/mix/account/accounts",
            queryString: queryString,
            credentials: credentials,
            ct: ct);

        EnsureBitgetSuccess(response);

        var now = DateTimeOffset.UtcNow;

        return (response.Data ?? [])
            .Select(x => new ExchangeBalanceSnapshot(
                ConnectorName: ConnectorName,
                Asset: x.MarginCoin ?? "",
                WalletBalance: ParseDecimal(x.Available),
                AvailableBalance: ParseDecimal(x.Available),
                Equity: ParseDecimal(x.AccountEquity),
                UsdValue: ParseDecimal(x.UsdtEquity),
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
        var exchangeSymbol = ToBitgetSymbol(request.TradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
        {
            throw new InvalidOperationException(
                $"Bitget symbol could not be derived for trading pair '{request.TradingPair}'.");
        }

        var isBuyRequest = request.Side.Equals("Buy", StringComparison.OrdinalIgnoreCase);
        var isHedgeMode = _options.PositionMode.Equals("hedge", StringComparison.OrdinalIgnoreCase);

        var orderBody = new Dictionary<string, object>
        {
            ["symbol"] = exchangeSymbol,
            ["productType"] = _options.ProductType,
            ["marginMode"] = _options.MarginMode,
            ["marginCoin"] = _options.MarginCoin,
            ["size"] = request.Quantity.ToString(CultureInfo.InvariantCulture),
            ["orderType"] = "market",
            ["clientOid"] = request.ClientOrderId
        };

        if (isHedgeMode)
        {
            // Two-way mode: side is the POSITION direction and tradeSide is open/close. A close
            // uses the SAME side as the open (close long = side buy + close), so when the executor
            // asks to close via the opposite side + reduceOnly we flip it back to the position side.
            orderBody["tradeSide"] = request.ReduceOnly ? "close" : "open";
            orderBody["side"] = request.ReduceOnly
                ? (isBuyRequest ? "sell" : "buy")
                : (isBuyRequest ? "buy" : "sell");
        }
        else
        {
            // One-way (unilateral) mode: plain side + reduceOnly, no tradeSide.
            orderBody["side"] = isBuyRequest ? "buy" : "sell";
            orderBody["reduceOnly"] = request.ReduceOnly ? "YES" : "NO";
        }

        var body = JsonSerializer.Serialize(orderBody);

        var createResponse = await SendSignedPostAsync<BitgetEnvelope<BitgetOrderCreateData>>(
            path: "/api/v2/mix/order/place-order",
            body: body,
            credentials: credentials,
            ct: ct);

        EnsureBitgetSuccess(createResponse);

        var orderId = createResponse.Data?.OrderId ?? "";

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
            var queryString =
                $"symbol={Uri.EscapeDataString(exchangeSymbol)}&productType={Uri.EscapeDataString(_options.ProductType)}&orderId={Uri.EscapeDataString(orderId)}";

            var response = await SendSignedGetAsync<BitgetEnvelope<BitgetOrderDetailData>>(
                path: "/api/v2/mix/order/detail",
                queryString: queryString,
                credentials: credentials,
                ct: ct);

            EnsureBitgetSuccess(response);

            var filledQuantity = ParseDecimal(response.Data?.BaseVolume);

            if (filledQuantity > 0)
            {
                var averagePrice = ParseDecimal(response.Data?.PriceAvg);
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
        var exchangeSymbol = ToBitgetSymbol(tradingPair);

        if (string.IsNullOrWhiteSpace(exchangeSymbol))
            return null;

        var queryString =
            $"productType={Uri.EscapeDataString(_options.ProductType)}&symbol={Uri.EscapeDataString(exchangeSymbol)}";

        var response = await SendPublicGetAsync<BitgetEnvelope<List<BitgetContract>>>(
            path: "/api/v2/mix/market/contracts",
            queryString: queryString,
            ct: ct);

        EnsureBitgetSuccess(response);

        var item = response.Data?.FirstOrDefault();

        if (item is null)
            return null;

        var pricePlace = ParseInt(item.PricePlace);
        var volumePlace = ParseInt(item.VolumePlace);

        var tickSize = (decimal)Math.Pow(10, -pricePlace);
        var quantityStep = ParseDecimal(item.SizeMultiplier);

        if (quantityStep <= 0)
            quantityStep = (decimal)Math.Pow(10, -volumePlace);

        return new ExchangeSymbolRules(
            ConnectorName: ConnectorName,
            TradingPair: InstrumentFilterService.NormalizeTradingPair(tradingPair),
            ExchangeSymbol: item.Symbol ?? exchangeSymbol,
            Status: item.SymbolStatus ?? "",
            TickSize: tickSize,
            QuantityStep: quantityStep,
            MinQuantity: ParseDecimal(item.MinTradeNum),
            MinNotional: 0m,
            MaxMarketQuantity: null,
            MaxLimitQuantity: null,
            ReceivedAt: DateTimeOffset.UtcNow);
    }

    private async Task<T> SendSignedGetAsync<T>(
        string path,
        string queryString,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var requestPath = string.IsNullOrEmpty(queryString) ? path : $"{path}?{queryString}";

        return await SendSignedAsync<T>(
            HttpMethod.Get,
            requestPath,
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
        string requestPath,
        string body,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        RequirePassphrase(credentials);

        var timestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

        var signature = _signer.Sign(
            credentials.ApiSecret,
            timestampMs,
            method.Method,
            requestPath,
            body);

        using var request = new HttpRequestMessage(
            method,
            requestPath);

        request.Headers.TryAddWithoutValidation("ACCESS-KEY", credentials.ApiKey);
        request.Headers.TryAddWithoutValidation("ACCESS-SIGN", signature);
        request.Headers.TryAddWithoutValidation("ACCESS-TIMESTAMP", timestampMs);
        request.Headers.TryAddWithoutValidation("ACCESS-PASSPHRASE", credentials.Passphrase);

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

    private static void RequirePassphrase(ExchangeApiCredentialSecret credentials)
    {
        if (string.IsNullOrWhiteSpace(credentials.Passphrase))
        {
            throw new InvalidOperationException(
                "Bitget requires an API passphrase in addition to key/secret.");
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
                $"Bitget HTTP request failed. StatusCode={(int)response.StatusCode}, Body={body}");
        }

        var parsed = JsonSerializer.Deserialize<T>(
            body,
            JsonOptions);

        if (parsed is null)
        {
            throw new InvalidOperationException(
                "Bitget response deserialization failed.");
        }

        return parsed;
    }

    private static void EnsureBitgetSuccess<T>(BitgetEnvelope<T> envelope)
    {
        if (envelope.Code != "00000")
        {
            throw new InvalidOperationException(
                $"Bitget API returned error. Code={envelope.Code}, Msg={envelope.Msg}");
        }
    }

    private static string ToBitgetSymbol(string tradingPair)
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

    private sealed class BitgetEnvelope<T>
    {
        public string Code { get; set; } = "";

        public string Msg { get; set; } = "";

        public T? Data { get; set; }
    }

    private sealed class BitgetAccount
    {
        public string? MarginCoin { get; set; }

        public string? Available { get; set; }

        public string? AccountEquity { get; set; }

        public string? UsdtEquity { get; set; }
    }

    private sealed class BitgetOrderCreateData
    {
        public string? OrderId { get; set; }

        public string? ClientOid { get; set; }
    }

    private sealed class BitgetOrderDetailData
    {
        public string? BaseVolume { get; set; }

        public string? PriceAvg { get; set; }

        public string? Fee { get; set; }

        public string? State { get; set; }
    }

    private sealed class BitgetContract
    {
        public string? Symbol { get; set; }

        public string? SymbolStatus { get; set; }

        public string? PricePlace { get; set; }

        public string? VolumePlace { get; set; }

        public string? SizeMultiplier { get; set; }

        public string? MinTradeNum { get; set; }
    }
}
