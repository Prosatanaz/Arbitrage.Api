using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Mexc;

public sealed class MexcOrderBookDepthStream : DepthStreamBase
{
    private const string Connector = "mexc_perpetual";
    private const string Channel = "push.depth.step";
    private const string DepthStep = "10";

    private readonly OrderBookDepthCache _depthCache;
    private readonly MexcContractMetadataStore _metadataStore;
    private readonly ILogger<MexcOrderBookDepthStream> _logger;

    private int _successfulUpdatesLogged;

    public MexcOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        MexcContractMetadataStore metadataStore,
        ILogger<MexcOrderBookDepthStream> logger)
        : base(targetStore, depthCache, logger)
    {
        _depthCache = depthCache;
        _metadataStore = metadataStore;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://contract.mexc.com/edge";

    protected override int ReceiveBufferSize => 1024 * 512;

    protected override void OnConnectionReset()
    {
        _successfulUpdatesLogged = 0;
    }

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        MexcSymbolMapper.ToMexcSymbol(tradingPair);

    protected override string MapExchangeSymbolToTradingPair(string exchangeSymbol) =>
        MexcSymbolMapper.FromMexcSymbol(exchangeSymbol);

    protected override async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbolsToAdd,
        CancellationToken ct)
    {
        foreach (var symbol in symbolsToAdd)
        {
            await SendSubscriptionAsync(socket, "sub.depth.step", symbol, ct);

            _logger.LogInformation(
                "MEXC depth subscribe requested. Symbol={Symbol}, Total={Total}",
                symbol,
                SubscribedSymbolCount);

            await Task.Delay(100, ct);
        }
    }

    protected override async Task UnsubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbolsToRemove,
        CancellationToken ct)
    {
        foreach (var symbol in symbolsToRemove)
        {
            await SendSubscriptionAsync(socket, "unsub.depth.step", symbol, ct);

            _logger.LogInformation(
                "MEXC depth unsubscribe requested. Symbol={Symbol}, Total={Total}",
                symbol,
                SubscribedSymbolCount);

            await Task.Delay(100, ct);
        }
    }

    private async Task SendSubscriptionAsync(
        ClientWebSocket socket,
        string method,
        string symbol,
        CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(new
        {
            method,
            param = new
            {
                symbol,
                step = DepthStep
            },
            gzip = false
        });

        await SendTextAsync(socket, payload, ct);
    }

    protected override Task? RunPingLoopAsync(ClientWebSocket socket, CancellationToken ct) =>
        PingLoopAsync(socket, ct);

    private async Task PingLoopAsync(ClientWebSocket socket, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            await Task.Delay(TimeSpan.FromSeconds(15), ct);

            if (socket.State != WebSocketState.Open)
                break;

            var payload = JsonSerializer.Serialize(new
            {
                method = "ping"
            });

            await SendTextAsync(socket, payload, ct);
        }
    }

    protected override async Task ProcessMessageAsync(
        ClientWebSocket socket,
        string json,
        CancellationToken ct)
    {
        try
        {
            ProcessMessage(json);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to parse MEXC depth message. Raw={Raw}",
                json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to process MEXC depth message. Raw={Raw}",
                json);
        }

        await Task.CompletedTask;
    }

    private void ProcessMessage(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (!TryGetStringProperty(root, "channel", out var channel))
            return;

        if (string.Equals(channel, "pong", StringComparison.OrdinalIgnoreCase))
            return;

        if (!string.Equals(channel, Channel, StringComparison.OrdinalIgnoreCase))
            return;

        if (!TryGetStringProperty(root, "symbol", out var symbol))
            return;

        if (!IsSubscribed(symbol))
            return;

        if (!TryGetPropertyIgnoreCase(root, "data", out var dataElement))
            return;

        ProcessDepth(symbol, dataElement);
    }

    private void ProcessDepth(
        string symbol,
        JsonElement data)
    {
        if (!TryGetPropertyIgnoreCase(data, "bids", out var bidsElement))
            return;

        if (!TryGetPropertyIgnoreCase(data, "asks", out var asksElement))
            return;

        var contractSize = GetContractSize(symbol);

        var bids = ReadLevels(bidsElement, contractSize)
            .OrderByDescending(x => x.Price)
            .ToList();

        var asks = ReadLevels(asksElement, contractSize)
            .OrderBy(x => x.Price)
            .ToList();

        if (bids.Count == 0 || asks.Count == 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = TryGetLongProperty(data, "ct", out var ct) && ct > 0
            ? ConvertTimestamp(ct)
            : receivedAt;

        var snapshot = new OrderBookDepthSnapshot(
            ConnectorName: Connector,
            TradingPair: MexcSymbolMapper.FromMexcSymbol(symbol),
            Bids: bids,
            Asks: asks,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _depthCache.Set(snapshot);

        if (Interlocked.CompareExchange(ref _successfulUpdatesLogged, 1, 0) == 0)
        {
            _logger.LogInformation(
                "MEXC depth first update received. Symbol={Symbol}, Bids={Bids}, Asks={Asks}, ContractSize={ContractSize}",
                symbol,
                bids.Count,
                asks.Count,
                contractSize);
        }
    }

    private decimal GetContractSize(string symbol)
    {
        if (_metadataStore.TryGetBySymbol(symbol, out var metadata) &&
            metadata.ContractSize > 0)
        {
            return metadata.ContractSize;
        }

        _logger.LogWarning(
            "MEXC contract metadata was not found. Symbol={Symbol}. Falling back to contract quantity as base amount.",
            symbol);

        return 1m;
    }

    private static IReadOnlyList<OrderBookDepthLevel> ReadLevels(
        JsonElement levelsElement,
        decimal contractSize)
    {
        if (levelsElement.ValueKind != JsonValueKind.Array)
            return [];

        var result = new List<OrderBookDepthLevel>();

        foreach (var level in levelsElement.EnumerateArray())
        {
            if (level.ValueKind != JsonValueKind.Array)
                continue;

            var values = level.EnumerateArray().ToList();

            if (values.Count < 2)
                continue;

            if (!TryReadDecimal(values[0], out var price))
                continue;

            // MEXC depth step level:
            // [price, orderCount, contractQuantity]
            // If third value exists, use it as contracts amount.
            // If not, fallback to second value.
            var quantityElement = values.Count >= 3
                ? values[2]
                : values[1];

            if (!TryReadDecimal(quantityElement, out var contractsAmount))
                continue;

            var baseAmount = contractsAmount * contractSize;

            if (price <= 0 || baseAmount <= 0)
                continue;

            result.Add(new OrderBookDepthLevel(
                Price: price,
                Amount: baseAmount));
        }

        return result;
    }

    private static DateTimeOffset ConvertTimestamp(long timestamp)
    {
        if (timestamp > 10_000_000_000)
            return DateTimeOffset.FromUnixTimeMilliseconds(timestamp);

        return DateTimeOffset.FromUnixTimeSeconds(timestamp);
    }

    private static bool TryReadDecimal(
        JsonElement element,
        out decimal value)
    {
        value = 0;

        if (element.ValueKind == JsonValueKind.Number)
            return element.TryGetDecimal(out value);

        if (element.ValueKind == JsonValueKind.String)
        {
            return decimal.TryParse(
                element.GetString(),
                NumberStyles.Number,
                CultureInfo.InvariantCulture,
                out value);
        }

        return false;
    }

    private static bool TryGetPropertyIgnoreCase(
        JsonElement element,
        string propertyName,
        out JsonElement value)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            value = default;
            return false;
        }

        if (element.TryGetProperty(propertyName, out value))
            return true;

        foreach (var property in element.EnumerateObject())
        {
            if (property.Name.Equals(
                    propertyName,
                    StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }

    private static bool TryGetStringProperty(
        JsonElement element,
        string propertyName,
        out string value)
    {
        value = "";

        if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
            return false;

        if (property.ValueKind != JsonValueKind.String)
            return false;

        value = property.GetString() ?? "";
        return !string.IsNullOrWhiteSpace(value);
    }

    private static bool TryGetLongProperty(
        JsonElement element,
        string propertyName,
        out long value)
    {
        value = 0;

        if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
            return false;

        if (property.ValueKind == JsonValueKind.Number)
            return property.TryGetInt64(out value);

        if (property.ValueKind == JsonValueKind.String)
        {
            return long.TryParse(
                property.GetString(),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out value);
        }

        return false;
    }
}
