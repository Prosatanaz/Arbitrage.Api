using System.Collections.Concurrent;
using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Bybit;

public sealed class BybitOrderBookDepthStream : IOrderBookDepthStream
{
    private const string Connector = "bybit_perpetual";
    private const string WsUrl = "wss://stream.bybit.com/v5/public/linear";
    private const int Depth = 50;

    private readonly DepthSubscriptionTargetStore _targetStore;
    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger<BybitOrderBookDepthStream> _logger;

    private readonly HashSet<string> _subscribedTopics =
        new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, LocalOrderBook> _books =
        new(StringComparer.OrdinalIgnoreCase);

    public string ConnectorName => Connector;

    public BybitOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger<BybitOrderBookDepthStream> logger)
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
                    "Bybit depth stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _subscribedTopics.Clear();
        _books.Clear();

        _logger.LogInformation("Connecting to Bybit depth stream: {Url}", WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation("Connected to Bybit depth stream.");

        var receiveTask = ReceiveLoopAsync(socket, ct);
        var subscriptionTask = SubscriptionLoopAsync(socket, ct);
        var pingTask = PingLoopAsync(socket, ct);

        await Task.WhenAny(receiveTask, subscriptionTask, pingTask);
    }

    private async Task SubscriptionLoopAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            var desiredTopics = _targetStore
                .GetPairsForConnector(Connector)
                .Select(ToTopicName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var toSubscribe = desiredTopics
                .Except(_subscribedTopics, StringComparer.OrdinalIgnoreCase)
                .ToList();

            var toUnsubscribe = _subscribedTopics
                .Except(desiredTopics, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (toSubscribe.Count > 0)
            {
                await SendSubscriptionAsync(
                    socket,
                    op: "subscribe",
                    topics: toSubscribe,
                    ct);

                foreach (var topic in toSubscribe)
                    _subscribedTopics.Add(topic);

                _logger.LogInformation(
                    "Bybit depth subscribed. Count={Count}, Total={Total}",
                    toSubscribe.Count,
                    _subscribedTopics.Count);
            }

            if (toUnsubscribe.Count > 0)
            {
                await SendSubscriptionAsync(
                    socket,
                    op: "unsubscribe",
                    topics: toUnsubscribe,
                    ct);

                foreach (var topic in toUnsubscribe)
                {
                    _subscribedTopics.Remove(topic);

                    var symbol = ExtractSymbolFromTopic(topic);

                    if (symbol is not null)
                    {
                        _books.TryRemove(symbol, out _);

                        var tradingPair = BybitSymbolMapper.FromBybitSymbol(symbol);

                        _depthCache.Remove(Connector, tradingPair);
                    }
                }

                _logger.LogInformation(
                    "Bybit depth unsubscribed. Count={Count}, Total={Total}",
                    toUnsubscribe.Count,
                    _subscribedTopics.Count);
            }

            await Task.Delay(1000, ct);
        }
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

            ProcessMessage(message);
        }
    }

    private async Task PingLoopAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            await Task.Delay(TimeSpan.FromSeconds(20), ct);

            if (socket.State != WebSocketState.Open)
                break;

            await SendTextAsync(socket, "{\"op\":\"ping\"}", ct);
        }
    }

    private void ProcessMessage(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (root.TryGetProperty("op", out var opElement))
        {
            var op = opElement.GetString();

            var success = root.TryGetProperty("success", out var successElement) &&
                          successElement.ValueKind == JsonValueKind.True;

            var retMsg = root.TryGetProperty("ret_msg", out var retMsgElement)
                ? retMsgElement.GetString()
                : null;

            _logger.LogInformation(
                "Bybit depth control message. Op={Op}, Success={Success}, RetMsg={RetMsg}",
                op,
                success,
                retMsg);

            return;
        }

        if (!root.TryGetProperty("topic", out var topicElement))
            return;

        var topic = topicElement.GetString();

        if (topic is null ||
            !topic.StartsWith("orderbook.", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var dto = JsonSerializer.Deserialize<BybitOrderBookMessage>(json);

        if (dto?.Data is null)
            return;

        ProcessOrderBook(dto);
    }

    private void ProcessOrderBook(BybitOrderBookMessage dto)
    {
        var data = dto.Data;

        if (string.IsNullOrWhiteSpace(data.Symbol))
            return;

        var book = _books.GetOrAdd(data.Symbol, _ => new LocalOrderBook());

        if (string.Equals(dto.Type, "snapshot", StringComparison.OrdinalIgnoreCase))
        {
            book.Replace(
                bids: data.Bids,
                asks: data.Asks);
        }
        else if (string.Equals(dto.Type, "delta", StringComparison.OrdinalIgnoreCase))
        {
            book.ApplyDelta(
                bids: data.Bids,
                asks: data.Asks);
        }
        else
        {
            return;
        }

        var bids = book.GetBids(Depth);
        var asks = book.GetAsks(Depth);

        if (bids.Count == 0 || asks.Count == 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = dto.TimestampMs > 0
            ? DateTimeOffset.FromUnixTimeMilliseconds(dto.TimestampMs)
            : receivedAt;

        var snapshot = new OrderBookDepthSnapshot(
            ConnectorName: Connector,
            TradingPair: BybitSymbolMapper.FromBybitSymbol(data.Symbol),
            Bids: bids,
            Asks: asks,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _depthCache.Set(snapshot);
    }

    private static string ToTopicName(string tradingPair)
    {
        var symbol = BybitSymbolMapper.ToBybitSymbol(tradingPair);

        return $"orderbook.{Depth}.{symbol}";
    }

    private static string? ExtractSymbolFromTopic(string topic)
    {
        // orderbook.50.BTCUSDT
        var parts = topic.Split('.');

        return parts.Length == 3
            ? parts[2]
            : null;
    }

    private static async Task SendSubscriptionAsync(
        ClientWebSocket socket,
        string op,
        IReadOnlyList<string> topics,
        CancellationToken ct)
    {
        const int batchSize = 20;

        foreach (var batch in topics.Chunk(batchSize))
        {
            var payload = JsonSerializer.Serialize(new
            {
                op,
                args = batch
            });

            await SendTextAsync(socket, payload, ct);

            await Task.Delay(100, ct);
        }
    }

    private static async Task SendTextAsync(
        ClientWebSocket socket,
        string payload,
        CancellationToken ct)
    {
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

        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, ct);

            if (result.MessageType == WebSocketMessageType.Close)
                return null;

            memory.Write(buffer, 0, result.Count);

            if (result.EndOfMessage)
                break;
        }

        return Encoding.UTF8.GetString(memory.ToArray());
    }

    private sealed class LocalOrderBook
    {
        private readonly SortedDictionary<decimal, decimal> _bids =
            new(Comparer<decimal>.Create((left, right) => right.CompareTo(left)));

        private readonly SortedDictionary<decimal, decimal> _asks =
            new();

        public void Replace(
            List<string[]> bids,
            List<string[]> asks)
        {
            _bids.Clear();
            _asks.Clear();

            ApplyLevels(_bids, bids);
            ApplyLevels(_asks, asks);
        }

        public void ApplyDelta(
            List<string[]> bids,
            List<string[]> asks)
        {
            ApplyLevels(_bids, bids);
            ApplyLevels(_asks, asks);
        }

        public IReadOnlyList<OrderBookDepthLevel> GetBids(int take)
        {
            return _bids
                .Take(take)
                .Select(x => new OrderBookDepthLevel(x.Key, x.Value))
                .ToList();
        }

        public IReadOnlyList<OrderBookDepthLevel> GetAsks(int take)
        {
            return _asks
                .Take(take)
                .Select(x => new OrderBookDepthLevel(x.Key, x.Value))
                .ToList();
        }

        private static void ApplyLevels(
            SortedDictionary<decimal, decimal> bookSide,
            List<string[]> levels)
        {
            foreach (var level in levels)
            {
                if (level.Length < 2)
                    continue;

                if (!TryParseDecimal(level[0], out var price))
                    continue;

                if (!TryParseDecimal(level[1], out var amount))
                    continue;

                if (price <= 0)
                    continue;

                if (amount <= 0)
                {
                    bookSide.Remove(price);
                }
                else
                {
                    bookSide[price] = amount;
                }
            }
        }
    }

    private static bool TryParseDecimal(
        string? value,
        out decimal result)
    {
        return decimal.TryParse(
            value,
            NumberStyles.Number,
            CultureInfo.InvariantCulture,
            out result);
    }

    private sealed class BybitOrderBookMessage
    {
        [JsonPropertyName("topic")]
        public string Topic { get; init; } = default!;

        [JsonPropertyName("type")]
        public string Type { get; init; } = default!;

        [JsonPropertyName("ts")]
        public long TimestampMs { get; init; }

        [JsonPropertyName("data")]
        public BybitOrderBookData Data { get; init; } = default!;
    }

    private sealed class BybitOrderBookData
    {
        [JsonPropertyName("s")]
        public string Symbol { get; init; } = default!;

        [JsonPropertyName("b")]
        public List<string[]> Bids { get; init; } = new();

        [JsonPropertyName("a")]
        public List<string[]> Asks { get; init; } = new();
    }
}