using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Arbitrage.Api.Application.Instruments;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.GateIo;

// NOTE: field/endpoint names follow Gate.io's documented v4 USDT-settled futures API shape as of
// this writing (general API knowledge, not re-verified against live docs for this pass). Verify
// with a real smoke test before trusting fills - see feedback memory on unverified exchange
// details. Gate.io futures orders are sized in CONTRACTS, not base-asset units - this client
// converts using the contract's quanto_multiplier (base units per contract); a wrong multiplier
// reading would place a wildly wrong-sized order, so this conversion is the single highest-risk
// piece of this implementation and should be sanity-checked against the exchange UI before relying
// on it with real capital. Per-trade fee is not exposed on the order response and is reported as 0
// (understates true cost) until a follow-up wires in the fills/trades endpoint.
public sealed class GateIoTradingClient : IExchangeTradingClient
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient _httpClient;
    private readonly GateIoTradingOptions _options;
    private readonly GateIoAuthSigner _signer;
    private readonly ILogger<GateIoTradingClient> _logger;

    public GateIoTradingClient(
        HttpClient httpClient,
        IOptions<GateIoTradingOptions> options,
        GateIoAuthSigner signer,
        ILogger<GateIoTradingClient> logger)
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

    public string ConnectorName => "gate_io_perpetual";

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
                "Gate.io connection check failed.");

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
        var account = await SendSignedAsync<GateIoAccount>(
            HttpMethod.Get,
            "/api/v4/futures/usdt/accounts",
            "",
            credentials,
            ct);

        var now = DateTimeOffset.UtcNow;

        return
        [
            new ExchangeBalanceSnapshot(
                ConnectorName: ConnectorName,
                Asset: account.Currency ?? "USDT",
                WalletBalance: ParseDecimal(account.Total),
                AvailableBalance: ParseDecimal(account.Available),
                Equity: ParseDecimal(account.Total),
                UsdValue: ParseDecimal(account.Total),
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
        var contract = ToGateIoContract(request.TradingPair);

        if (string.IsNullOrWhiteSpace(contract))
        {
            throw new InvalidOperationException(
                $"Gate.io contract could not be derived for trading pair '{request.TradingPair}'.");
        }

        var contractInfo = await GetContractAsync(
            contract,
            ct);

        if (contractInfo is null || ParseDecimal(contractInfo.QuantoMultiplier) <= 0)
        {
            throw new InvalidOperationException(
                $"Gate.io contract metadata unavailable for '{contract}'.");
        }

        var quantoMultiplier = ParseDecimal(contractInfo.QuantoMultiplier);
        var contractsSigned = (long)Math.Round(
            request.Quantity / quantoMultiplier,
            MidpointRounding.AwayFromZero);

        if (contractsSigned <= 0)
            contractsSigned = 1;

        var isBuy = request.Side.Equals("Buy", StringComparison.OrdinalIgnoreCase);

        if (!isBuy)
            contractsSigned = -contractsSigned;

        var body = JsonSerializer.Serialize(new
        {
            contract,
            size = contractsSigned,
            price = "0",
            tif = "ioc",
            reduce_only = request.ReduceOnly,
            text = "t-" + request.ClientOrderId.Replace("-", "")[..Math.Min(24, request.ClientOrderId.Replace("-", "").Length)]
        });

        var createResponse = await SendSignedAsync<GateIoOrder>(
            HttpMethod.Post,
            "/api/v4/futures/usdt/orders",
            body,
            credentials,
            ct);

        var orderId = createResponse.Id?.ToString(CultureInfo.InvariantCulture) ?? "";

        var fill = await GetFillWithRetryAsync(
            orderId,
            createResponse,
            quantoMultiplier,
            credentials,
            ct);

        return new OrderFillResult(
            ConnectorName: ConnectorName,
            ClientOrderId: request.ClientOrderId,
            ExchangeOrderId: orderId,
            IsFilled: fill.FilledQuantity > 0,
            FilledQuantity: fill.FilledQuantity,
            AverageFillPrice: fill.AveragePrice,
            FeePaidUsd: 0m,
            Status: fill.FilledQuantity >= request.Quantity ? "Filled" : fill.FilledQuantity > 0 ? "PartiallyFilled" : "Unfilled",
            FilledAt: DateTimeOffset.UtcNow);
    }

    private async Task<(decimal FilledQuantity, decimal AveragePrice)> GetFillWithRetryAsync(
        string orderId,
        GateIoOrder createResponse,
        decimal quantoMultiplier,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var order = createResponse;

        for (var attempt = 0; attempt < 5; attempt++)
        {
            var sizeAbs = Math.Abs(order.Size ?? 0);
            var leftAbs = Math.Abs(order.Left ?? sizeAbs);
            var filledContracts = sizeAbs - leftAbs;

            if (filledContracts > 0)
            {
                var averagePrice = ParseDecimal(order.FillPrice);

                if (averagePrice <= 0)
                    averagePrice = ParseDecimal(order.Price);

                return (filledContracts * quantoMultiplier, averagePrice);
            }

            if (string.IsNullOrEmpty(orderId))
                break;

            await Task.Delay(200, ct);

            order = await SendSignedAsync<GateIoOrder>(
                HttpMethod.Get,
                $"/api/v4/futures/usdt/orders/{Uri.EscapeDataString(orderId)}",
                "",
                credentials,
                ct);
        }

        return (0m, 0m);
    }

    public async Task<ExchangeSymbolRules?> GetSymbolRulesAsync(
        string tradingPair,
        CancellationToken ct)
    {
        var contract = ToGateIoContract(tradingPair);

        if (string.IsNullOrWhiteSpace(contract))
            return null;

        var item = await GetContractAsync(
            contract,
            ct);

        if (item is null)
            return null;

        var quantoMultiplier = ParseDecimal(item.QuantoMultiplier);

        return new ExchangeSymbolRules(
            ConnectorName: ConnectorName,
            TradingPair: InstrumentFilterService.NormalizeTradingPair(tradingPair),
            ExchangeSymbol: item.Name ?? contract,
            Status: item.InDelisting == true ? "Delisting" : "Trading",
            TickSize: ParseDecimal(item.OrderPriceRound),
            QuantityStep: quantoMultiplier,
            MinQuantity: quantoMultiplier * ParseDecimal(item.OrderSizeMin),
            MinNotional: 0m,
            MaxMarketQuantity: quantoMultiplier > 0 && item.OrderSizeMax is not null
                ? quantoMultiplier * ParseDecimal(item.OrderSizeMax)
                : null,
            MaxLimitQuantity: null,
            ReceivedAt: DateTimeOffset.UtcNow);
    }

    private async Task<GateIoContract?> GetContractAsync(
        string contract,
        CancellationToken ct)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"/api/v4/futures/usdt/contracts/{Uri.EscapeDataString(contract)}");

        try
        {
            return await SendAsync<GateIoContract>(
                request,
                ct);
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }

    private async Task<T> SendSignedAsync<T>(
        HttpMethod method,
        string urlPath,
        string body,
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct)
    {
        var timestampSeconds = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);
        var hashedBody = _signer.HashBody(body);

        var signature = _signer.Sign(
            credentials.ApiSecret,
            method.Method,
            urlPath,
            "",
            hashedBody,
            timestampSeconds);

        using var request = new HttpRequestMessage(
            method,
            urlPath);

        request.Headers.TryAddWithoutValidation("KEY", credentials.ApiKey);
        request.Headers.TryAddWithoutValidation("SIGN", signature);
        request.Headers.TryAddWithoutValidation("Timestamp", timestampSeconds);

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
                $"Gate.io HTTP request failed. StatusCode={(int)response.StatusCode}, Body={body}");
        }

        var parsed = JsonSerializer.Deserialize<T>(
            body,
            JsonOptions);

        if (parsed is null)
        {
            throw new InvalidOperationException(
                "Gate.io response deserialization failed.");
        }

        return parsed;
    }

    private static string ToGateIoContract(string tradingPair)
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

    private sealed class GateIoAccount
    {
        public string? Currency { get; set; }

        public string? Total { get; set; }

        public string? Available { get; set; }
    }

    private sealed class GateIoOrder
    {
        public long? Id { get; set; }

        public string? Contract { get; set; }

        public long? Size { get; set; }

        public long? Left { get; set; }

        public string? Price { get; set; }

        public string? FillPrice { get; set; }

        public string? Status { get; set; }
    }

    private sealed class GateIoContract
    {
        public string? Name { get; set; }

        public string? QuantoMultiplier { get; set; }

        public string? OrderPriceRound { get; set; }

        public string? OrderSizeMin { get; set; }

        public string? OrderSizeMax { get; set; }

        public bool? InDelisting { get; set; }
    }
}
