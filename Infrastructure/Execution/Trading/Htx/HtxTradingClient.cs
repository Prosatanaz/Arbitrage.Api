using System.Globalization;
using System.Net.Http.Headers;
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

        var walletBalance = FindFirstDecimal(
            dataElement,
            "margin_balance",
            "margin_static",
            "total_margin_balance",
            "total_asset",
            "balance",
            "account_balance");

        var availableBalance = FindFirstDecimal(
            dataElement,
            "withdraw_available",
            "available_balance",
            "margin_available",
            "available");

        var equity = FindFirstDecimal(
            dataElement,
            "margin_balance",
            "total_margin_balance",
            "equity",
            "total_asset");

        return
        [
            new ExchangeBalanceSnapshot(
                ConnectorName: ConnectorName,
                Asset: "USDT",
                WalletBalance: walletBalance,
                AvailableBalance: availableBalance,
                Equity: equity,
                UsdValue: equity,
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

    // PlaceOrderAsync is intentionally not implemented yet. When added, round price/qty
    // via OrderSizeRounding using these rules and attach client_order_id for retry idempotency.
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

    private static decimal FindFirstDecimal(
        JsonElement element,
        params string[] propertyNames)
    {
        foreach (var propertyName in propertyNames)
        {
            var value = FindDecimalRecursive(
                element,
                propertyName);

            if (value is not null)
                return value.Value;
        }

        return 0m;
    }

    private static decimal? FindDecimalRecursive(
        JsonElement element,
        string propertyName)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    if (property.Name.Equals(
                            propertyName,
                            StringComparison.OrdinalIgnoreCase))
                    {
                        var parsed = ParseNullableDecimal(property.Value);

                        if (parsed is not null)
                            return parsed;
                    }

                    var nested = FindDecimalRecursive(
                        property.Value,
                        propertyName);

                    if (nested is not null)
                        return nested;
                }

                return null;

            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    var nested = FindDecimalRecursive(
                        item,
                        propertyName);

                    if (nested is not null)
                        return nested;
                }

                return null;

            default:
                return null;
        }
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