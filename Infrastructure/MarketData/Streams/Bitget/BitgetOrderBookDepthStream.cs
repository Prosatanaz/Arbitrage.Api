using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Bitget;

public sealed class BitgetOrderBookDepthStream : IOrderBookDepthStream
{
    private const string Connector = "bitget_perpetual";
    private const string WsUrl = "wss://ws.bitget.com/v2/ws/public";
    private const string InstType = "USDT-FUTURES";
    private const string Channel = "books5";

    private readonly DepthSubscriptionTargetStore _targetStore;
    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger<BitgetOrderBookDepthStream> _logger;

    private readonly HashSet<string> _subscribedSymbols =
        new(StringComparer.OrdinalIgnoreCase);

    private SemaphoreSlim? _sendLock;

    public string ConnectorName => Connector;

    public BitgetOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger<BitgetOrderBookDepthStream> logger)
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
                    "Bitget depth stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _sendLock = new SemaphoreSlim(1, 1);
        _subscribedSymbols.Clear();

        _logger.LogInformation("Connecting to Bitget depth stream: {Url}", WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation("Connected to Bitget depth stream.");

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
            var desiredSymbols = _targetStore
                .GetPairsForConnector(Connector)
                .Select(BitgetSymbolMapper.ToBitgetSymbol)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var toSubscribe = desiredSymbols
                .Except(_subscribedSymbols, StringComparer.OrdinalIgnoreCase)
                .ToList();

            var toUnsubscribe = _subscribedSymbols
                .Except(desiredSymbols, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (toSubscribe.Count > 0)
            {
                await SendSubscriptionAsync(
                    socket,
                    op: "subscribe",
                    symbols: toSubscribe,
                    ct);

                foreach (var symbol in toSubscribe)
                    _subscribedSymbols.Add(symbol);

                _logger.LogInformation(
                    "Bitget depth subscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                    toSubscribe.Count,
                    _subscribedSymbols.Count,
                    string.Join(", ", toSubscribe.Take(5)));
            }

            if (toUnsubscribe.Count > 0)
            {
                await SendSubscriptionAsync(
                    socket,
                    op: "unsubscribe",
                    symbols: toUnsubscribe,
                    ct);

                foreach (var symbol in toUnsubscribe)
                {
                    _subscribedSymbols.Remove(symbol);

                    _depthCache.Remove(
                        Connector,
                        BitgetSymbolMapper.FromBitgetSymbol(symbol));
                }

                _logger.LogInformation(
                    "Bitget depth unsubscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                    toUnsubscribe.Count,
                    _subscribedSymbols.Count,
                    string.Join(", ", toUnsubscribe.Take(5)));
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

            try
            {
                ProcessMessage(message);
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to parse Bitget depth message. Raw={Raw}",
                    message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to process Bitget depth message. Raw={Raw}",
                    message);
            }
        }
    }

    private async Task PingLoopAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            await Task.Delay(TimeSpan.FromSeconds(30), ct);

            if (socket.State != WebSocketState.Open)
                break;

            await SendTextAsync(socket, "ping", ct);
        }
    }

    private void ProcessMessage(string json)
    {
        if (string.Equals(json, "pong", StringComparison.OrdinalIgnoreCase))
            return;

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (root.TryGetProperty("event", out var eventElement))
        {
            var eventName = eventElement.GetString();

            if (string.Equals(eventName, "error", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning("Bitget depth WS error. Raw={Raw}", json);
            }
            else
            {
                _logger.LogDebug("Bitget depth WS control. Raw={Raw}", json);
            }

            return;
        }

        var dto = JsonSerializer.Deserialize<BitgetOrderBookMessage>(json);

        if (dto?.Data is null || dto.Data.Count == 0)
            return;

        var symbolFromArg = dto.Arg?.InstrumentId;

        foreach (var item in dto.Data)
        {
            ProcessOrderBookItem(symbolFromArg, item);
        }
    }

    private void ProcessOrderBookItem(
        string? symbolFromArg,
        BitgetOrderBookData item)
    {
        var symbol = !string.IsNullOrWhiteSpace(item.InstrumentId)
            ? item.InstrumentId
            : symbolFromArg;

        if (string.IsNullOrWhiteSpace(symbol))
            return;

        if (!_subscribedSymbols.Contains(symbol))
            return;

        var bids = item.Bids
            .Select(ParseLevel)
            .Where(x => x.Price > 0 && x.Amount > 0)
            .OrderByDescending(x => x.Price)
            .ToList();

        var asks = item.Asks
            .Select(ParseLevel)
            .Where(x => x.Price > 0 && x.Amount > 0)
            .OrderBy(x => x.Price)
            .ToList();

        if (bids.Count == 0 || asks.Count == 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = long.TryParse(
            item.TimestampMs,
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out var ts) && ts > 0
                ? DateTimeOffset.FromUnixTimeMilliseconds(ts)
                : receivedAt;

        var snapshot = new OrderBookDepthSnapshot(
            ConnectorName: Connector,
            TradingPair: BitgetSymbolMapper.FromBitgetSymbol(symbol),
            Bids: bids,
            Asks: asks,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _depthCache.Set(snapshot);
    }

    private async Task SendSubscriptionAsync(
        ClientWebSocket socket,
        string op,
        IReadOnlyList<string> symbols,
        CancellationToken ct)
    {
        const int batchSize = 20;

        foreach (var batch in symbols.Chunk(batchSize))
        {
            var payload = JsonSerializer.Serialize(new
            {
                op,
                args = batch.Select(symbol => new
                {
                    instType = InstType,
                    channel = Channel,
                    instId = symbol
                }).ToList()
            });

            await SendTextAsync(socket, payload, ct);

            await Task.Delay(1000, ct);
        }
    }

    private async Task SendTextAsync(
        ClientWebSocket socket,
        string payload,
        CancellationToken ct)
    {
        if (_sendLock is null)
            throw new InvalidOperationException("Bitget send lock is not initialized.");

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

    private static OrderBookDepthLevel ParseLevel(
        IReadOnlyList<string> values)
    {
        if (values.Count < 2)
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

    private sealed class BitgetOrderBookMessage
    {
        [JsonPropertyName("arg")]
        public BitgetArg? Arg { get; init; }

        [JsonPropertyName("action")]
        public string? Action { get; init; }

        [JsonPropertyName("data")]
        public List<BitgetOrderBookData> Data { get; init; } = new();
    }

    private sealed class BitgetArg
    {
        [JsonPropertyName("instType")]
        public string InstrumentType { get; init; } = "";

        [JsonPropertyName("channel")]
        public string Channel { get; init; } = "";

        [JsonPropertyName("instId")]
        public string InstrumentId { get; init; } = "";
    }

    private sealed class BitgetOrderBookData
    {
        [JsonPropertyName("instId")]
        public string? InstrumentId { get; init; }

        [JsonPropertyName("bids")]
        public List<List<string>> Bids { get; init; } = new();

        [JsonPropertyName("asks")]
        public List<List<string>> Asks { get; init; } = new();

        [JsonPropertyName("ts")]
        public string? TimestampMs { get; init; }
    }
}