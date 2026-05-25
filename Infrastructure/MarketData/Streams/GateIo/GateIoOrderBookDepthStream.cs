using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.GateIo;

public sealed class GateIoOrderBookDepthStream : IOrderBookDepthStream
{
    private const string Connector = "gate_io_perpetual";
    private const string WsUrl = "wss://fx-ws.gateio.ws/v4/ws/usdt";
    private const string Channel = "futures.order_book";

    private readonly DepthSubscriptionTargetStore _targetStore;
    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger<GateIoOrderBookDepthStream> _logger;

    private readonly HashSet<string> _subscribedContracts =
        new(StringComparer.OrdinalIgnoreCase);

    private SemaphoreSlim? _sendLock;

    public string ConnectorName => Connector;

    public GateIoOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger<GateIoOrderBookDepthStream> logger)
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
                    "Gate.io depth stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _sendLock = new SemaphoreSlim(1, 1);
        _subscribedContracts.Clear();

        _logger.LogInformation("Connecting to Gate.io depth stream: {Url}", WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation("Connected to Gate.io depth stream.");

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
            var desiredContracts = _targetStore
                .GetPairsForConnector(Connector)
                .Select(GateIoSymbolMapper.ToGateIoContract)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var toSubscribe = desiredContracts
                .Except(_subscribedContracts, StringComparer.OrdinalIgnoreCase)
                .ToList();

            var toUnsubscribe = _subscribedContracts
                .Except(desiredContracts, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (toSubscribe.Count > 0)
            {
                await SendSubscriptionAsync(
                    socket,
                    @event: "subscribe",
                    contracts: toSubscribe,
                    ct);

                foreach (var contract in toSubscribe)
                    _subscribedContracts.Add(contract);

                _logger.LogInformation(
                    "Gate.io depth subscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                    toSubscribe.Count,
                    _subscribedContracts.Count,
                    string.Join(", ", toSubscribe.Take(5)));
            }

            if (toUnsubscribe.Count > 0)
            {
                await SendSubscriptionAsync(
                    socket,
                    @event: "unsubscribe",
                    contracts: toUnsubscribe,
                    ct);

                foreach (var contract in toUnsubscribe)
                {
                    _subscribedContracts.Remove(contract);

                    _depthCache.Remove(
                        Connector,
                        GateIoSymbolMapper.FromGateIoContract(contract));
                }

                _logger.LogInformation(
                    "Gate.io depth unsubscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                    toUnsubscribe.Count,
                    _subscribedContracts.Count,
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
                    "Failed to parse Gate.io depth message. Raw={Raw}",
                    message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to process Gate.io depth message. Raw={Raw}",
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
            await Task.Delay(TimeSpan.FromSeconds(20), ct);

            if (socket.State != WebSocketState.Open)
                break;

            var payload = JsonSerializer.Serialize(new
            {
                time = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                channel = "futures.ping"
            });

            await SendTextAsync(socket, payload, ct);
        }
    }

    private void ProcessMessage(string json)
    {
        var dto = JsonSerializer.Deserialize<GateIoWsMessage>(json);

        if (dto is null)
            return;

        if (!string.Equals(dto.Channel, Channel, StringComparison.OrdinalIgnoreCase))
            return;

        var isOrderBookEvent =
            string.Equals(dto.Event, "all", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(dto.Event, "update", StringComparison.OrdinalIgnoreCase);

        if (!isOrderBookEvent)
        {
            if (string.Equals(dto.Event, "error", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning("Gate.io depth WS error. Raw={Raw}", json);
            }

            return;
        }

        if (dto.Result.ValueKind == JsonValueKind.Undefined ||
            dto.Result.ValueKind == JsonValueKind.Null)
        {
            return;
        }

        var item = dto.Result.Deserialize<GateIoOrderBookData>();

        if (item is null)
            return;

        ProcessOrderBookItem(item);
    }

    private void ProcessOrderBookItem(GateIoOrderBookData item)
    {
        if (string.IsNullOrWhiteSpace(item.Contract))
            return;

        if (!_subscribedContracts.Contains(item.Contract))
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

        var exchangeTimestamp = item.Timestamp > 0
            ? DateTimeOffset.FromUnixTimeMilliseconds(
                item.Timestamp > 10_000_000_000
                    ? item.Timestamp
                    : item.Timestamp * 1000)
            : receivedAt;

        var snapshot = new OrderBookDepthSnapshot(
            ConnectorName: Connector,
            TradingPair: GateIoSymbolMapper.FromGateIoContract(item.Contract),
            Bids: bids,
            Asks: asks,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _depthCache.Set(snapshot);
    }

    private async Task SendSubscriptionAsync(
        ClientWebSocket socket,
        string @event,
        IReadOnlyList<string> contracts,
        CancellationToken ct)
    {
        const int batchSize = 10;

        foreach (var contract in contracts)
        {
            var payload = JsonSerializer.Serialize(new
            {
                time = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                channel = Channel,
                @event,
                payload = new object[]
                {
                    contract,
                    "20",
                    "0"
                }
            });

            await SendTextAsync(socket, payload, ct);
            await Task.Delay(200, ct);
        }
    }

    private async Task SendTextAsync(
        ClientWebSocket socket,
        string payload,
        CancellationToken ct)
    {
        if (_sendLock is null)
            throw new InvalidOperationException("Gate.io send lock is not initialized.");

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

    private static bool TryReadDecimal(
    JsonElement element,
    out decimal value)
    {
        value = 0;

        if (element.ValueKind == JsonValueKind.Number)
        {
            return element.TryGetDecimal(out value);
        }

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

    private static OrderBookDepthLevel ParseLevel(
    GateIoOrderBookLevel level)
    {
        if (!TryReadDecimal(level.Price, out var price))
        {
            return new OrderBookDepthLevel(
                Price: 0,
                Amount: 0);
        }

        if (!TryReadDecimal(level.Size, out var size))
        {
            return new OrderBookDepthLevel(
                Price: 0,
                Amount: 0);
        }

        return new OrderBookDepthLevel(
            Price: price,
            Amount: Math.Abs(size));
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

    private sealed class GateIoWsMessage
    {
        [JsonPropertyName("channel")]
        public string Channel { get; init; } = "";

        [JsonPropertyName("event")]
        public string Event { get; init; } = "";

        [JsonPropertyName("result")]
        public JsonElement Result { get; init; }
    }

    private sealed class GateIoOrderBookData
    {
        [JsonPropertyName("contract")]
        public string Contract { get; init; } = "";

        [JsonPropertyName("t")]
        public long Timestamp { get; init; }

        [JsonPropertyName("bids")]
        public List<GateIoOrderBookLevel> Bids { get; init; } = new();

        [JsonPropertyName("asks")]
        public List<GateIoOrderBookLevel> Asks { get; init; } = new();
    }

    private sealed class GateIoOrderBookLevel
    {
        [JsonPropertyName("p")]
        public JsonElement Price { get; init; }

        [JsonPropertyName("s")]
        public JsonElement Size { get; init; }
    }
}