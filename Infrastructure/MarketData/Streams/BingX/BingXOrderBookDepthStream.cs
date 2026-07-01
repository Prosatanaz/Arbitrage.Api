using System.Globalization;
using System.IO.Compression;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.BingX;

public sealed class BingXOrderBookDepthStream : IOrderBookDepthStream
{
    private const string Connector = "bingx_perpetual";
    private const string WsUrl = "wss://open-api-swap.bingx.com/swap-market";

    private readonly DepthSubscriptionTargetStore _targetStore;
    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger<BingXOrderBookDepthStream> _logger;

    private readonly HashSet<string> _subscribedSymbols =
        new(StringComparer.OrdinalIgnoreCase);

    private SemaphoreSlim? _sendLock;
    private int _successfulUpdatesLogged;

    public string ConnectorName => Connector;

    public BingXOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger<BingXOrderBookDepthStream> logger)
    {
        _targetStore = targetStore;
        _depthCache = depthCache;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await RunConnectionAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "BingX depth stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _sendLock = new SemaphoreSlim(1, 1);
        _subscribedSymbols.Clear();
        _successfulUpdatesLogged = 0;

        _logger.LogInformation(
            "Connecting to BingX depth stream: {Url}",
            WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation("Connected to BingX depth stream.");

        var receiveTask = ReceiveLoopAsync(socket, ct);
        var subscriptionTask = SubscriptionLoopAsync(socket, ct);

        await Task.WhenAny(receiveTask, subscriptionTask);
    }

    private async Task SubscriptionLoopAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            var desiredSymbols = _targetStore
                .GetPairsForConnector(Connector)
                .Select(BingXSymbolMapper.ToBingXSymbol)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var toSubscribe = desiredSymbols
                .Except(_subscribedSymbols, StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x)
                .ToList();

            var toUnsubscribe = _subscribedSymbols
                .Except(desiredSymbols, StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x)
                .ToList();

            foreach (var symbol in toSubscribe)
            {
                await SendSubscriptionAsync(
                    socket,
                    reqType: "sub",
                    symbol,
                    ct);

                _subscribedSymbols.Add(symbol);

                _logger.LogInformation(
                    "BingX depth subscribe requested. Symbol={Symbol}, Total={Total}",
                    symbol,
                    _subscribedSymbols.Count);

                await Task.Delay(100, ct);
            }

            foreach (var symbol in toUnsubscribe)
            {
                await SendSubscriptionAsync(
                    socket,
                    reqType: "unsub",
                    symbol,
                    ct);

                _subscribedSymbols.Remove(symbol);

                _depthCache.Remove(
                    Connector,
                    BingXSymbolMapper.FromBingXSymbol(symbol));

                _logger.LogInformation(
                    "BingX depth unsubscribe requested. Symbol={Symbol}, Total={Total}",
                    symbol,
                    _subscribedSymbols.Count);

                await Task.Delay(100, ct);
            }

            await Task.Delay(1000, ct);
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

    private async Task ReceiveLoopAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        var buffer = new byte[1024 * 512];

        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            var message = await ReceiveTextAsync(socket, buffer, ct);

            if (message is null)
                break;

            try
            {
                await ProcessMessageAsync(socket, message, ct);
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to parse BingX depth message. Raw={Raw}",
                    message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to process BingX depth message. Raw={Raw}",
                    message);
            }
        }
    }

    private async Task ProcessMessageAsync(
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

        if (!_subscribedSymbols.Contains(item.Symbol))
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

    private async Task SendTextAsync(
        ClientWebSocket socket,
        string payload,
        CancellationToken ct)
    {
        if (_sendLock is null)
            throw new InvalidOperationException("BingX send lock is not initialized.");

        await _sendLock.WaitAsync(ct);

        try
        {
            if (socket.State != WebSocketState.Open)
                return;

            var bytes = Encoding.UTF8.GetBytes(payload);

            await socket.SendAsync(
                bytes,
                WebSocketMessageType.Text,
                endOfMessage: true,
                cancellationToken: ct);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private static async Task<string?> ReceiveTextAsync(
        ClientWebSocket socket,
        byte[] buffer,
        CancellationToken ct)
    {
        using var memory = new MemoryStream();

        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, ct);

            if (result.MessageType == WebSocketMessageType.Close)
                return null;

            memory.Write(buffer, 0, result.Count);

            if (result.EndOfMessage)
                break;
        }

        var bytes = memory.ToArray();

        if (TryDecompressGzip(bytes, out var decompressed))
            return decompressed;

        return Encoding.UTF8.GetString(bytes);
    }

    private static bool TryDecompressGzip(
        byte[] bytes,
        out string value)
    {
        value = "";

        try
        {
            using var compressed = new MemoryStream(bytes);
            using var gzip = new GZipStream(compressed, CompressionMode.Decompress);
            using var reader = new StreamReader(gzip, Encoding.UTF8);

            value = reader.ReadToEnd();
            return true;
        }
        catch
        {
            return false;
        }
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