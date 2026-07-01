using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.BitMart;

public sealed class BitMartOrderBookDepthStream : DepthStreamBase
{
    private const string Connector = "bitmart_perpetual";
    private const string Channel = "futures/depthAll20";
    private const string Speed = "200ms";

    private readonly OrderBookDepthCache _depthCache;
    private readonly BitMartContractMetadataStore _metadataStore;
    private readonly ILogger<BitMartOrderBookDepthStream> _logger;

    private int _successfulUpdatesLogged;

    public BitMartOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        BitMartContractMetadataStore metadataStore,
        ILogger<BitMartOrderBookDepthStream> logger)
        : base(targetStore, depthCache, logger)
    {
        _depthCache = depthCache;
        _metadataStore = metadataStore;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://openapi-ws-v2.bitmart.com/api?protocol=1.1";

    protected override int ReceiveBufferSize => 1024 * 256;

    protected override void OnConnectionReset()
    {
        _successfulUpdatesLogged = 0;
    }

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        BitMartSymbolMapper.ToBitMartSymbol(tradingPair);

    protected override string MapExchangeSymbolToTradingPair(string exchangeSymbol) =>
        BitMartSymbolMapper.FromBitMartSymbol(exchangeSymbol);

    protected override Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbolsToAdd,
        CancellationToken ct) =>
        SendSubscriptionAsync(socket, "subscribe", symbolsToAdd, ct, isSubscribe: true);

    protected override Task UnsubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbolsToRemove,
        CancellationToken ct) =>
        SendSubscriptionAsync(socket, "unsubscribe", symbolsToRemove, ct, isSubscribe: false);

    private async Task SendSubscriptionAsync(
        ClientWebSocket socket,
        string action,
        IReadOnlyList<string> symbols,
        CancellationToken ct,
        bool isSubscribe)
    {
        const int batchSize = 20;

        foreach (var batch in symbols.Chunk(batchSize))
        {
            var payload = JsonSerializer.Serialize(new
            {
                action,
                args = batch
                    .Select(symbol => $"{Channel}:{symbol}@{Speed}")
                    .ToList()
            });

            await SendTextAsync(socket, payload, ct);

            await Task.Delay(1000, ct);
        }

        if (isSubscribe)
        {
            _logger.LogInformation(
                "BitMart depth subscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                symbols.Count,
                SubscribedSymbolCount,
                string.Join(", ", symbols.Take(5)));
        }
        else
        {
            _logger.LogInformation(
                "BitMart depth unsubscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                symbols.Count,
                SubscribedSymbolCount,
                string.Join(", ", symbols.Take(5)));
        }
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

            await SendTextAsync(socket, "{\"action\":\"ping\"}", ct);
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
                "Failed to parse BitMart depth message. Raw={Raw}",
                json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to process BitMart depth message. Raw={Raw}",
                json);
        }

        await Task.CompletedTask;
    }

    private void ProcessMessage(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (TryGetStringProperty(root, "action", out var action))
        {
            if (action.Equals("subscribe", StringComparison.OrdinalIgnoreCase) ||
                action.Equals("unsubscribe", StringComparison.OrdinalIgnoreCase) ||
                action.Equals("pong", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            if (action.Equals("error", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning("BitMart depth WS error. Raw={Raw}", json);
                return;
            }
        }

        if (!TryGetStringProperty(root, "group", out var group))
            return;

        if (!group.StartsWith($"{Channel}:", StringComparison.OrdinalIgnoreCase))
            return;

        if (!TryGetPropertyIgnoreCase(root, "data", out var data))
            return;

        ProcessDepth(data);
    }

    private void ProcessDepth(JsonElement data)
    {
        if (!TryGetStringProperty(data, "symbol", out var symbol))
            return;

        if (!IsSubscribed(symbol))
            return;

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

        var exchangeTimestamp = TryGetLongProperty(data, "ms_t", out var timestampMs) &&
                                timestampMs > 0
            ? DateTimeOffset.FromUnixTimeMilliseconds(timestampMs)
            : receivedAt;

        var snapshot = new OrderBookDepthSnapshot(
            ConnectorName: Connector,
            TradingPair: BitMartSymbolMapper.FromBitMartSymbol(symbol),
            Bids: bids,
            Asks: asks,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _depthCache.Set(snapshot);

        if (Interlocked.CompareExchange(ref _successfulUpdatesLogged, 1, 0) == 0)
        {
            _logger.LogInformation(
                "BitMart depth first update received. Symbol={Symbol}, Bids={Bids}, Asks={Asks}, ContractSize={ContractSize}",
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
            "BitMart contract metadata was not found. Symbol={Symbol}. Falling back to volume as base amount.",
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
            if (!TryGetDecimalProperty(level, "price", out var price))
                continue;

            if (!TryGetDecimalProperty(level, "vol", out var volume))
                continue;

            var baseAmount = volume * contractSize;

            if (price <= 0 || baseAmount <= 0)
                continue;

            result.Add(new OrderBookDepthLevel(price, baseAmount));
        }

        return result;
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
            if (property.Name.Equals(propertyName, StringComparison.OrdinalIgnoreCase))
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

    private static bool TryGetDecimalProperty(
        JsonElement element,
        string propertyName,
        out decimal value)
    {
        value = 0;

        if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
            return false;

        if (property.ValueKind == JsonValueKind.Number)
            return property.TryGetDecimal(out value);

        if (property.ValueKind == JsonValueKind.String)
        {
            return decimal.TryParse(
                property.GetString(),
                NumberStyles.Number,
                CultureInfo.InvariantCulture,
                out value);
        }

        return false;
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
