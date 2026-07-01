using System.Globalization;
using System.IO.Compression;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Htx;

public sealed class HtxBboStream : IBestBidAskStream
{
    private const string Connector = "htx_perpetual";
    private const string WsUrl = "wss://api.hbdm.com/linear-swap-ws";

    private readonly BestBidAskCache _cache;
    private readonly ILogger<HtxBboStream> _logger;

    private int _successfulUpdatesLogged;

    public string ConnectorName => Connector;

    public HtxBboStream(
        BestBidAskCache cache,
        ILogger<HtxBboStream> logger)
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
            _logger.LogWarning("HTX BBO stream has no trading pairs to subscribe.");
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
                    "HTX BBO stream crashed. Reconnecting in 5 seconds...");

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
            "Connecting to HTX BBO stream: {Url}",
            WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation(
            "Connected to HTX BBO stream. Pairs={Pairs}",
            tradingPairs.Count);

        await SubscribeAsync(socket, tradingPairs, ct);

        await ReceiveLoopAsync(socket, ct);
    }

    private async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> tradingPairs,
        CancellationToken ct)
    {
        foreach (var tradingPair in tradingPairs)
        {
            var contractCode = HtxSymbolMapper.ToHtxContractCode(tradingPair);

            var payload = JsonSerializer.Serialize(new
            {
                sub = $"market.{contractCode}.bbo",
                id = $"bbo-{contractCode}"
            });

            await SendTextAsync(socket, payload, ct);
            await Task.Delay(25, ct);
        }

        _logger.LogInformation(
            "HTX BBO subscribe requested. Count={Count}, Sample={Sample}",
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
                    "Failed to parse HTX BBO message. Raw={Raw}",
                    message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to process HTX BBO message. Raw={Raw}",
                    message);
            }
        }
    }

    private async Task ProcessMessageAsync(
        ClientWebSocket socket,
        string json,
        CancellationToken ct)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (TryGetLongProperty(root, "ping", out var ping))
        {
            var pong = JsonSerializer.Serialize(new
            {
                pong = ping
            });

            await SendTextAsync(socket, pong, ct);
            return;
        }

        if (TryGetPropertyIgnoreCase(root, "status", out var statusElement) &&
            statusElement.ValueKind == JsonValueKind.String &&
            string.Equals(statusElement.GetString(), "error", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning("HTX BBO WS error. Raw={Raw}", json);
            return;
        }

        if (!TryGetStringProperty(root, "ch", out var channel))
            return;

        if (!channel.EndsWith(".bbo", StringComparison.OrdinalIgnoreCase))
            return;

        var tradingPair = ExtractTradingPairFromChannel(channel);

        if (tradingPair is null)
            return;

        if (!TryGetPropertyIgnoreCase(root, "tick", out var tick))
            return;

        if (!TryGetArrayProperty(tick, "bid", out var bid) ||
            !TryGetArrayProperty(tick, "ask", out var ask))
        {
            return;
        }

        if (!TryReadPriceAmount(bid, out var bidPrice, out var bidAmount))
            return;

        if (!TryReadPriceAmount(ask, out var askPrice, out var askAmount))
            return;

        if (bidPrice <= 0 || askPrice <= 0 || bidAmount <= 0 || askAmount <= 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = TryGetLongProperty(root, "ts", out var ts) && ts > 0
            ? ConvertTimestamp(ts)
            : receivedAt;

        var snapshot = new BestBidAskSnapshot(
            ConnectorName: Connector,
            TradingPair: tradingPair,
            BestBidPrice: bidPrice,
            BestBidAmount: bidAmount,
            BestAskPrice: askPrice,
            BestAskAmount: askAmount,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _cache.Set(snapshot);

        if (Interlocked.CompareExchange(ref _successfulUpdatesLogged, 1, 0) == 0)
        {
            _logger.LogInformation(
                "HTX BBO first update received. Pair={Pair}, Bid={BidPrice}, Ask={AskPrice}",
                tradingPair,
                bidPrice,
                askPrice);
        }
    }

    private static string? ExtractTradingPairFromChannel(string channel)
    {
        // market.BTC-USDT.bbo
        const string prefix = "market.";
        const string suffix = ".bbo";

        if (!channel.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) ||
            !channel.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        var symbol = channel[prefix.Length..^suffix.Length];

        return HtxSymbolMapper.FromHtxContractCode(symbol);
    }

    private static bool TryReadPriceAmount(
        JsonElement array,
        out decimal price,
        out decimal amount)
    {
        price = 0;
        amount = 0;

        if (array.ValueKind != JsonValueKind.Array)
            return false;

        var values = array.EnumerateArray().ToList();

        if (values.Count < 2)
            return false;

        return TryReadDecimal(values[0], out price) &&
               TryReadDecimal(values[1], out amount);
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

        if (messageType == WebSocketMessageType.Binary)
            return DecompressGzip(bytes);

        return Encoding.UTF8.GetString(bytes);
    }

    private static string DecompressGzip(byte[] bytes)
    {
        using var compressed = new MemoryStream(bytes);
        using var gzip = new GZipStream(compressed, CompressionMode.Decompress);
        using var reader = new StreamReader(gzip, Encoding.UTF8);

        return reader.ReadToEnd();
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

    private static bool TryGetArrayProperty(
        JsonElement element,
        string propertyName,
        out JsonElement value)
    {
        if (!TryGetPropertyIgnoreCase(element, propertyName, out value))
            return false;

        return value.ValueKind == JsonValueKind.Array;
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
}