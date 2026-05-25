using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Binance;

public sealed class BinanceOrderBookDepthStream : IOrderBookDepthStream
{
    private const string Connector = "binance_perpetual";
    private const string WsUrl = "wss://fstream.binance.com/ws";

    private readonly DepthSubscriptionTargetStore _targetStore;
    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger<BinanceOrderBookDepthStream> _logger;

    private readonly HashSet<string> _subscribedStreams =
        new(StringComparer.OrdinalIgnoreCase);

    public string ConnectorName => Connector;

    public BinanceOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger<BinanceOrderBookDepthStream> logger)
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
                    "Binance depth stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _subscribedStreams.Clear();

        _logger.LogInformation("Connecting to Binance depth stream: {Url}", WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation("Connected to Binance depth stream.");

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
            var desiredStreams = _targetStore
                .GetPairsForConnector(Connector)
                .Select(ToDepthStreamName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var toSubscribe = desiredStreams
                .Except(_subscribedStreams, StringComparer.OrdinalIgnoreCase)
                .ToList();

            var toUnsubscribe = _subscribedStreams
                .Except(desiredStreams, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (toSubscribe.Count > 0)
            {
                await SendSubscriptionAsync(
                    socket,
                    method: "SUBSCRIBE",
                    streams: toSubscribe,
                    ct);

                foreach (var stream in toSubscribe)
                    _subscribedStreams.Add(stream);

                _logger.LogInformation(
                    "Binance depth subscribed. Count={Count}, Total={Total}",
                    toSubscribe.Count,
                    _subscribedStreams.Count);
            }

            if (toUnsubscribe.Count > 0)
            {
                await SendSubscriptionAsync(
                    socket,
                    method: "UNSUBSCRIBE",
                    streams: toUnsubscribe,
                    ct);

                foreach (var stream in toUnsubscribe)
                {
                    _subscribedStreams.Remove(stream);

                    var tradingPair = TryGetTradingPairFromDepthStreamName(stream);

                    if (tradingPair is not null)
                    {
                        _depthCache.Remove(Connector, tradingPair);
                    }
                }

                _logger.LogInformation(
                    "Binance depth unsubscribed. Count={Count}, Total={Total}",
                    toUnsubscribe.Count,
                    _subscribedStreams.Count);
            }

            await Task.Delay(1000, ct);
        }
    }

    private async Task ReceiveLoopAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        var buffer = new byte[1024 * 128];

        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            var message = await ReceiveTextAsync(socket, buffer, ct);

            if (message is null)
                break;

            ProcessMessage(message);
        }
    }

    private void ProcessMessage(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        // Binance subscription ack:
        // {"result":null,"id":1}
        if (root.TryGetProperty("result", out _))
            return;

        var dto = JsonSerializer.Deserialize<BinancePartialDepthMessage>(json);

        if (dto is null || string.IsNullOrWhiteSpace(dto.EventType))
            return;

        var tradingPair = BinanceSymbolMapper.FromBinanceSymbol(dto.Symbol);

        var bids = dto.Bids
            .Select(ParseLevel)
            .Where(x => x.Price > 0 && x.Amount > 0)
            .OrderByDescending(x => x.Price)
            .ToList();

        var asks = dto.Asks
            .Select(ParseLevel)
            .Where(x => x.Price > 0 && x.Amount > 0)
            .OrderBy(x => x.Price)
            .ToList();

        if (bids.Count == 0 || asks.Count == 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = dto.EventTime > 0
            ? DateTimeOffset.FromUnixTimeMilliseconds(dto.EventTime)
            : receivedAt;

        _depthCache.Set(new OrderBookDepthSnapshot(
            ConnectorName: Connector,
            TradingPair: tradingPair,
            Bids: bids,
            Asks: asks,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt));
    }

    private static OrderBookDepthLevel ParseLevel(string[] values)
    {
        if (values.Length < 2)
            return new OrderBookDepthLevel(0, 0);

        var priceOk = decimal.TryParse(
            values[0],
            NumberStyles.Number,
            CultureInfo.InvariantCulture,
            out var price);

        var amountOk = decimal.TryParse(
            values[1],
            NumberStyles.Number,
            CultureInfo.InvariantCulture,
            out var amount);

        return priceOk && amountOk
            ? new OrderBookDepthLevel(price, amount)
            : new OrderBookDepthLevel(0, 0);
    }

    private static string ToDepthStreamName(string tradingPair)
    {
        // BTC-USDT -> btcusdt@depth20@100ms
        var symbol = BinanceSymbolMapper
            .ToBinanceSymbol(tradingPair)
            .ToLowerInvariant();

        return $"{symbol}@depth20@100ms";
    }

    private static string? TryGetTradingPairFromDepthStreamName(
        string streamName)
    {
        // btcusdt@depth20@100ms
        var symbol = streamName.Split('@', 2)[0];

        if (string.IsNullOrWhiteSpace(symbol))
            return null;

        return BinanceSymbolMapper.FromBinanceSymbol(symbol);
    }

    private async Task SendSubscriptionAsync(
        ClientWebSocket socket,
        string method,
        IReadOnlyList<string> streams,
        CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(new
        {
            method,
            @params = streams,
            id = Environment.TickCount
        });

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

    private sealed class BinancePartialDepthMessage
    {
        [JsonPropertyName("e")]
        public string EventType { get; init; } = default!;

        [JsonPropertyName("E")]
        public long EventTime { get; init; }

        [JsonPropertyName("s")]
        public string Symbol { get; init; } = default!;

        [JsonPropertyName("b")]
        public List<string[]> Bids { get; init; } = new();

        [JsonPropertyName("a")]
        public List<string[]> Asks { get; init; } = new();
    }
}