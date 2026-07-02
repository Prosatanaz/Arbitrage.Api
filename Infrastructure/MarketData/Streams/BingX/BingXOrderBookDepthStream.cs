using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.BingX;

public sealed class BingXOrderBookDepthStream : DepthStreamBase
{
    private const string Connector = "bingx_perpetual";

    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger<BingXOrderBookDepthStream> _logger;

    private int _successfulUpdatesLogged;

    public BingXOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger<BingXOrderBookDepthStream> logger)
        : base(targetStore, depthCache, logger)
    {
        _depthCache = depthCache;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://open-api-swap.bingx.com/swap-market";

    protected override int ReceiveBufferSize => 1024 * 512;

    protected override void OnConnectionReset()
    {
        _successfulUpdatesLogged = 0;
    }

    protected override void OnConnecting()
    {
        _logger.LogInformation("Connecting to BingX depth stream: {Url}", WsUrl);
    }

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        BingXSymbolMapper.ToBingXSymbol(tradingPair);

    protected override string MapExchangeSymbolToTradingPair(string exchangeSymbol) =>
        BingXSymbolMapper.FromBingXSymbol(exchangeSymbol);

    protected override async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbolsToAdd,
        CancellationToken ct)
    {
        foreach (var symbol in symbolsToAdd.OrderBy(x => x))
        {
            await SendSubscriptionAsync(socket, "sub", symbol, ct);

            _logger.LogInformation(
                "BingX depth subscribe requested. Symbol={Symbol}, Total={Total}",
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
        foreach (var symbol in symbolsToRemove.OrderBy(x => x))
        {
            await SendSubscriptionAsync(socket, "unsub", symbol, ct);

            _logger.LogInformation(
                "BingX depth unsubscribe requested. Symbol={Symbol}, Total={Total}",
                symbol,
                SubscribedSymbolCount);

            await Task.Delay(100, ct);
        }
    }

    private async Task SendSubscriptionAsync(
        ClientWebSocket socket,
        string reqType,
        string symbol,
        CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(new
        {
            id = $"depth-{symbol}",
            reqType,
            dataType = $"{symbol}@depth5@500ms"
        });

        await SendTextAsync(socket, payload, ct);
    }

    protected override bool TryDecompress(
        byte[] buffer,
        int count,
        WebSocketMessageType messageType,
        out string text)
    {
        if (GzipTextDecoder.TryDecompress(buffer, count, out var decompressed))
        {
            text = decompressed;
            return true;
        }

        text = System.Text.Encoding.UTF8.GetString(buffer, 0, count);
        return true;
    }

    protected override async Task ProcessMessageAsync(
        ClientWebSocket socket,
        string json,
        CancellationToken ct)
    {
        try
        {
            await HandleMessageAsync(socket, json, ct);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to parse BingX depth message. Raw={Raw}",
                json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to process BingX depth message. Raw={Raw}",
                json);
        }
    }

    private async Task HandleMessageAsync(
        ClientWebSocket socket,
        string json,
        CancellationToken ct)
    {
        if (string.Equals(json, "Ping", StringComparison.OrdinalIgnoreCase))
        {
            await SendTextAsync(socket, "Pong", ct);
            return;
        }

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (TryGetStringProperty(root, "ping", out var pingString))
        {
            await SendTextAsync(
                socket,
                JsonSerializer.Serialize(new { pong = pingString }),
                ct);

            return;
        }

        if (TryGetLongProperty(root, "ping", out var pingLong))
        {
            await SendTextAsync(
                socket,
                JsonSerializer.Serialize(new { pong = pingLong }),
                ct);

            return;
        }

        if (TryGetStringProperty(root, "code", out var codeString) &&
            !string.Equals(codeString, "0", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning("BingX depth WS non-success message. Raw={Raw}", json);
            return;
        }

        if (TryGetLongProperty(root, "code", out var codeLong) &&
            codeLong != 0)
        {
            _logger.LogWarning("BingX depth WS non-success message. Raw={Raw}", json);
            return;
        }

        if (!TryExtractDepth(root, out var item))
        {
            _logger.LogDebug(
                "BingX depth message ignored. Raw={Raw}",
                json);

            return;
        }

        if (!IsSubscribed(item.Symbol))
            return;

        var bids = item.Bids
            .Where(x => x.Price > 0 && x.Amount > 0)
            .OrderByDescending(x => x.Price)
            .ToList();

        var asks = item.Asks
            .Where(x => x.Price > 0 && x.Amount > 0)
            .OrderBy(x => x.Price)
            .ToList();

        if (bids.Count == 0 || asks.Count == 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = item.TimestampMs > 0
            ? ConvertTimestamp(item.TimestampMs)
            : receivedAt;

        var snapshot = new OrderBookDepthSnapshot(
            ConnectorName: Connector,
            TradingPair: BingXSymbolMapper.FromBingXSymbol(item.Symbol),
            Bids: bids,
            Asks: asks,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _depthCache.Set(snapshot);

        if (Interlocked.CompareExchange(ref _successfulUpdatesLogged, 1, 0) == 0)
        {
            _logger.LogInformation(
                "BingX depth first update received. Symbol={Symbol}, Bids={Bids}, Asks={Asks}",
                item.Symbol,
                bids.Count,
                asks.Count);
        }
    }

    private static bool TryExtractDepth(
        JsonElement root,
        out BingXDepthItem item)
    {
        item = default;

        if (TryGetPropertyIgnoreCase(root, "data", out var data) &&
            data.ValueKind == JsonValueKind.Object &&
            TryExtractDepthFromObject(data, root, out item))
        {
            return true;
        }

        return TryExtractDepthFromObject(root, root, out item);
    }

    private static bool TryExtractDepthFromObject(
        JsonElement source,
        JsonElement root,
        out BingXDepthItem item)
    {
        item = default;

        var symbol =
            TryReadString(source, "symbol", out var sourceSymbol)
                ? sourceSymbol
                : TrySymbolFromDataType(root, out var dataTypeSymbol)
                    ? dataTypeSymbol
                    : "";

        if (string.IsNullOrWhiteSpace(symbol))
            return false;

        if (!TryGetPropertyIgnoreCase(source, "bids", out var bidsElement) ||
            !TryGetPropertyIgnoreCase(source, "asks", out var asksElement))
        {
            return false;
        }

        var bids = ReadDepthLevels(bidsElement);
        var asks = ReadDepthLevels(asksElement);

        if (bids.Count == 0 || asks.Count == 0)
            return false;

        var timestampMs = TryReadLongByAnyName(
            source,
            ["time", "T", "ts", "timestamp"],
            out var sourceTimestamp)
            ? sourceTimestamp
            : TryReadLongByAnyName(
                root,
                ["time", "T", "ts", "timestamp"],
                out var rootTimestamp)
                ? rootTimestamp
                : 0;

        item = new BingXDepthItem(
            Symbol: symbol.Trim().ToUpperInvariant(),
            Bids: bids,
            Asks: asks,
            TimestampMs: timestampMs);

        return true;
    }

    private static IReadOnlyList<OrderBookDepthLevel> ReadDepthLevels(
        JsonElement levelsElement)
    {
        if (levelsElement.ValueKind != JsonValueKind.Array)
            return [];

        var result = new List<OrderBookDepthLevel>();

        foreach (var level in levelsElement.EnumerateArray())
        {
            if (TryReadDepthLevel(level, out var parsed))
                result.Add(parsed);
        }

        return result;
    }

    private static bool TryReadDepthLevel(
        JsonElement level,
        out OrderBookDepthLevel parsed)
    {
        parsed = new OrderBookDepthLevel(
            Price: 0,
            Amount: 0);

        if (level.ValueKind == JsonValueKind.Array)
        {
            var values = level.EnumerateArray().ToList();

            if (values.Count < 2)
                return false;

            if (!TryReadDecimal(values[0], out var price))
                return false;

            if (!TryReadDecimal(values[1], out var amount))
                return false;

            if (price <= 0 || amount <= 0)
                return false;

            parsed = new OrderBookDepthLevel(
                Price: price,
                Amount: amount);

            return true;
        }

        if (level.ValueKind == JsonValueKind.Object)
        {
            if (!TryReadDecimalByAnyName(
                    level,
                    ["price", "p", "bidPrice", "askPrice"],
                    out var price))
            {
                return false;
            }

            if (!TryReadDecimalByAnyName(
                    level,
                    ["qty", "quantity", "amount", "volume", "q"],
                    out var amount))
            {
                return false;
            }

            if (price <= 0 || amount <= 0)
                return false;

            parsed = new OrderBookDepthLevel(
                Price: price,
                Amount: amount);

            return true;
        }

        return false;
    }

    private static bool TrySymbolFromDataType(
        JsonElement root,
        out string symbol)
    {
        symbol = "";

        if (!TryReadString(root, "dataType", out var dataType))
            return false;

        var atIndex = dataType.IndexOf('@');

        if (atIndex <= 0)
            return false;

        symbol = dataType[..atIndex]
            .Trim()
            .ToUpperInvariant();

        return !string.IsNullOrWhiteSpace(symbol);
    }

    private static DateTimeOffset ConvertTimestamp(long timestamp)
    {
        return timestamp > 10_000_000_000
            ? DateTimeOffset.FromUnixTimeMilliseconds(timestamp)
            : DateTimeOffset.FromUnixTimeSeconds(timestamp);
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

    private static bool TryReadString(
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

    private static bool TryReadDecimalByAnyName(
        JsonElement element,
        IReadOnlyList<string> propertyNames,
        out decimal value)
    {
        value = 0;

        foreach (var propertyName in propertyNames)
        {
            if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
                continue;

            if (TryReadDecimal(property, out value))
                return true;
        }

        return false;
    }

    private static bool TryReadLongByAnyName(
        JsonElement element,
        IReadOnlyList<string> propertyNames,
        out long value)
    {
        value = 0;

        foreach (var propertyName in propertyNames)
        {
            if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
                continue;

            if (property.ValueKind == JsonValueKind.Number &&
                property.TryGetInt64(out value))
            {
                return true;
            }

            if (property.ValueKind == JsonValueKind.String &&
                long.TryParse(
                    property.GetString(),
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out value))
            {
                return true;
            }
        }

        return false;
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

    private readonly record struct BingXDepthItem(
        string Symbol,
        IReadOnlyList<OrderBookDepthLevel> Bids,
        IReadOnlyList<OrderBookDepthLevel> Asks,
        long TimestampMs);
}
