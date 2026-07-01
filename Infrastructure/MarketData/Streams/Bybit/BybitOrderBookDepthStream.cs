using System.Collections.Concurrent;
using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Bybit;

public sealed class BybitOrderBookDepthStream : DepthStreamBase
{
    private const string Connector = "bybit_perpetual";
    private const int Depth = 50;

    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger<BybitOrderBookDepthStream> _logger;

    private readonly ConcurrentDictionary<string, LocalOrderBook> _books =
        new(StringComparer.OrdinalIgnoreCase);

    public BybitOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger<BybitOrderBookDepthStream> logger)
        : base(targetStore, depthCache, logger)
    {
        _depthCache = depthCache;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://stream.bybit.com/v5/public/linear";

    protected override int ReceiveBufferSize => 1024 * 256;

    protected override void OnConnectionReset()
    {
        _books.Clear();
    }

    protected override string MapTradingPairToExchangeSymbol(string tradingPair)
    {
        var symbol = BybitSymbolMapper.ToBybitSymbol(tradingPair);

        return $"orderbook.{Depth}.{symbol}";
    }

    protected override string MapExchangeSymbolToTradingPair(string exchangeSymbol)
    {
        var symbol = ExtractSymbolFromTopic(exchangeSymbol) ?? exchangeSymbol;

        return BybitSymbolMapper.FromBybitSymbol(symbol);
    }

    protected override async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbolsToAdd,
        CancellationToken ct)
    {
        await SendSubscriptionAsync(socket, "subscribe", symbolsToAdd, ct);

        _logger.LogInformation(
            "Bybit depth subscribed. Count={Count}, Total={Total}",
            symbolsToAdd.Count,
            SubscribedSymbolCount);
    }

    protected override async Task UnsubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbolsToRemove,
        CancellationToken ct)
    {
        await SendSubscriptionAsync(socket, "unsubscribe", symbolsToRemove, ct);

        foreach (var topic in symbolsToRemove)
        {
            var symbol = ExtractSymbolFromTopic(topic);

            if (symbol is not null)
                _books.TryRemove(symbol, out _);
        }

        _logger.LogInformation(
            "Bybit depth unsubscribed. Count={Count}, Total={Total}",
            symbolsToRemove.Count,
            SubscribedSymbolCount);
    }

    private async Task SendSubscriptionAsync(
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

    protected override Task ProcessMessageAsync(
        ClientWebSocket socket,
        string json,
        CancellationToken ct)
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

            return Task.CompletedTask;
        }

        if (!root.TryGetProperty("topic", out var topicElement))
            return Task.CompletedTask;

        var topic = topicElement.GetString();

        if (topic is null ||
            !topic.StartsWith("orderbook.", StringComparison.OrdinalIgnoreCase))
        {
            return Task.CompletedTask;
        }

        var dto = JsonSerializer.Deserialize<BybitOrderBookMessage>(json);

        if (dto?.Data is null)
            return Task.CompletedTask;

        ProcessOrderBook(dto);

        return Task.CompletedTask;
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

    private static string? ExtractSymbolFromTopic(string topic)
    {
        // orderbook.50.BTCUSDT
        var parts = topic.Split('.');

        return parts.Length == 3
            ? parts[2]
            : null;
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
