using System.Globalization;
using System.IO.Compression;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.BingX;

public sealed class BingXBboStream : IBestBidAskStream
{
    private const string Connector = "bingx_perpetual";
    private const string WsUrl = "wss://open-api-swap.bingx.com/swap-market";

    private readonly BestBidAskCache _cache;
    private readonly ILogger<BingXBboStream> _logger;

    private int _successfulUpdatesLogged;

    public string ConnectorName => Connector;

    public BingXBboStream(
        BestBidAskCache cache,
        ILogger<BingXBboStream> logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public async Task StartAsync(
        IReadOnlyList<string> tradingPairs,
        CancellationToken ct)
    {
        var pairs = tradingPairs
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        if (pairs.Count == 0)
        {
            _logger.LogWarning("BingX BBO stream has no trading pairs to subscribe.");
            return;
        }

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await RunConnectionAsync(pairs, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "BingX BBO stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(
        IReadOnlyList<string> tradingPairs,
        CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _successfulUpdatesLogged = 0;

        _logger.LogInformation(
            "Connecting to BingX BBO stream: {Url}",
            WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation(
            "Connected to BingX BBO stream. Pairs={Pairs}",
            tradingPairs.Count);

        await SubscribeAsync(socket, tradingPairs, ct);

        await ReceiveLoopAsync(socket, ct);
    }

    private async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> tradingPairs,
        CancellationToken ct)
    {
        var total = 0;

        foreach (var tradingPair in tradingPairs)
        {
            var symbol = BingXSymbolMapper.ToBingXSymbol(tradingPair);

            var payload = JsonSerializer.Serialize(new
            {
                id = $"bbo-{symbol}",
                reqType = "sub",
                dataType = $"{symbol}@depth5@500ms"
            });

            await SendTextAsync(socket, payload, ct);

            total++;

            if (total % 25 == 0)
            {
                _logger.LogInformation(
                    "BingX BBO subscribe requested. BatchSize={BatchSize}, Total={Total}",
                    25,
                    total);
            }

            await Task.Delay(25, ct);
        }

        _logger.LogInformation(
            "BingX BBO subscribe completed. Count={Count}, Sample={Sample}",
            tradingPairs.Count,
            string.Join(", ", tradingPairs.Take(10)));
    }

    private async Task ReceiveLoopAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        var buffer = new byte[1024 * 256];

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
                    "Failed to parse BingX BBO message. Raw={Raw}",
                    message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to process BingX BBO message. Raw={Raw}",
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
            await SendTextAsync(socket, JsonSerializer.Serialize(new { pong = pingString }), ct);
            return;
        }

        if (TryGetLongProperty(root, "ping", out var pingLong))
        {
            await SendTextAsync(socket, JsonSerializer.Serialize(new { pong = pingLong }), ct);
            return;
        }

        if (TryGetStringProperty(root, "code", out var codeString) &&
            !string.Equals(codeString, "0", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning("BingX BBO WS non-success message. Raw={Raw}", json);
            return;
        }

        if (TryGetLongProperty(root, "code", out var codeLong) && codeLong != 0)
        {
            _logger.LogWarning("BingX BBO WS non-success message. Raw={Raw}", json);
            return;
        }

        if (!TryExtractBbo(root, out var item))
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = item.TimestampMs > 0
            ? ConvertTimestamp(item.TimestampMs)
            : receivedAt;

        var snapshot = new BestBidAskSnapshot(
            ConnectorName: Connector,
            TradingPair: BingXSymbolMapper.FromBingXSymbol(item.Symbol),
            BestBidPrice: item.BidPrice,
            BestBidAmount: item.BidAmount,
            BestAskPrice: item.AskPrice,
            BestAskAmount: item.AskAmount,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _cache.Set(snapshot);

        if (Interlocked.CompareExchange(ref _successfulUpdatesLogged, 1, 0) == 0)
        {
            _logger.LogInformation(
                "BingX BBO first update received. Pair={Pair}, Bid={BidPrice}, Ask={AskPrice}",
                snapshot.TradingPair,
                snapshot.BestBidPrice,
                snapshot.BestAskPrice);
        }
    }

    private static bool TryExtractBbo(
        JsonElement root,
        out BingXBboItem item)
    {
        item = default;

        if (TryGetPropertyIgnoreCase(root, "data", out var data) &&
            data.ValueKind == JsonValueKind.Object)
        {
            if (TryExtractBboFromDepth(data, root, out item))
                return true;

            if (TryExtractBboFromObject(data, root, out item))
                return true;
        }

        if (TryExtractBboFromDepth(root, root, out item))
            return true;

        return TryExtractBboFromObject(root, root, out item);
    }
    private static bool TryExtractBboFromDepth(
        JsonElement source,
        JsonElement root,
    out BingXBboItem item)
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

        if (!TryGetPropertyIgnoreCase(source, "bids", out var bids) ||
            !TryGetPropertyIgnoreCase(source, "asks", out var asks))
        {
            return false;
        }

        if (!TryReadFirstDepthLevel(bids, out var bidPrice, out var bidAmount))
            return false;

        if (!TryReadFirstDepthLevel(asks, out var askPrice, out var askAmount))
            return false;

        if (bidPrice <= 0 || askPrice <= 0)
            return false;

        if (bidAmount <= 0)
            bidAmount = 1m;

        if (askAmount <= 0)
            askAmount = 1m;

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

        item = new BingXBboItem(
            Symbol: symbol,
            BidPrice: bidPrice,
            BidAmount: bidAmount,
            AskPrice: askPrice,
            AskAmount: askAmount,
            TimestampMs: timestampMs);

        return true;
    }
    private static bool TryReadFirstDepthLevel(
        JsonElement levels,
        out decimal price,
        out decimal amount)
    {
        price = 0;
        amount = 0;

        if (levels.ValueKind != JsonValueKind.Array)
            return false;

        var first = levels.EnumerateArray().FirstOrDefault();

        if (first.ValueKind == JsonValueKind.Undefined ||
            first.ValueKind == JsonValueKind.Null)
        {
            return false;
        }

        if (first.ValueKind == JsonValueKind.Array)
        {
            var values = first.EnumerateArray().ToList();

            if (values.Count < 2)
                return false;

            return TryReadDecimal(values[0], out price) &&
                   TryReadDecimal(values[1], out amount);
        }

        if (first.ValueKind == JsonValueKind.Object)
        {
            if (!TryReadDecimalByAnyName(
                    first,
                    ["price", "p", "bidPrice", "askPrice"],
                    out price))
            {
                return false;
            }

            if (!TryReadDecimalByAnyName(
                    first,
                    ["qty", "quantity", "amount", "volume", "q"],
                    out amount))
            {
                amount = 1m;
            }

            return true;
        }

        return false;
    }

    private static bool TryExtractBboFromObject(
        JsonElement source,
        JsonElement root,
        out BingXBboItem item)
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

        if (!TryReadDecimalByAnyName(
                source,
                ["bidPrice", "bid", "bestBidPrice", "b"],
                out var bidPrice))
        {
            return false;
        }

        if (!TryReadDecimalByAnyName(
                source,
                ["bidQty", "bidVolume", "bidAmount", "bestBidQty", "B"],
                out var bidAmount))
        {
            bidAmount = 1m;
        }

        if (!TryReadDecimalByAnyName(
                source,
                ["askPrice", "ask", "bestAskPrice", "a"],
                out var askPrice))
        {
            return false;
        }

        if (!TryReadDecimalByAnyName(
                source,
                ["askQty", "askVolume", "askAmount", "bestAskQty", "A"],
                out var askAmount))
        {
            askAmount = 1m;
        }

        if (bidPrice <= 0 || askPrice <= 0)
            return false;

        if (bidAmount <= 0)
            bidAmount = 1m;

        if (askAmount <= 0)
            askAmount = 1m;

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

        item = new BingXBboItem(
            Symbol: symbol,
            BidPrice: bidPrice,
            BidAmount: bidAmount,
            AskPrice: askPrice,
            AskAmount: askAmount,
            TimestampMs: timestampMs);

        return true;
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

        symbol = dataType[..atIndex].Trim().ToUpperInvariant();

        return !string.IsNullOrWhiteSpace(symbol);
    }

    private static async Task SendTextAsync(
        ClientWebSocket socket,
        string payload,
        CancellationToken ct)
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

    private static async Task<string?> ReceiveTextAsync(
        ClientWebSocket socket,
        byte[] buffer,
        CancellationToken ct)
    {
        using var memory = new MemoryStream();

        WebSocketMessageType? messageType = null;

        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, ct);

            if (result.MessageType == WebSocketMessageType.Close)
                return null;

            messageType ??= result.MessageType;

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

        if (property.ValueKind == JsonValueKind.String)
        {
            value = property.GetString() ?? "";
            return !string.IsNullOrWhiteSpace(value);
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

    private readonly record struct BingXBboItem(
        string Symbol,
        decimal BidPrice,
        decimal BidAmount,
        decimal AskPrice,
        decimal AskAmount,
        long TimestampMs);
}