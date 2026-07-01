using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.GateIo;

public sealed class GateIoOrderBookDepthStream : DepthStreamBase
{
    private const string Connector = "gate_io_perpetual";
    private const string Channel = "futures.order_book";

    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger<GateIoOrderBookDepthStream> _logger;

    public GateIoOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger<GateIoOrderBookDepthStream> logger)
        : base(targetStore, depthCache, logger)
    {
        _depthCache = depthCache;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://fx-ws.gateio.ws/v4/ws/usdt";

    protected override int ReceiveBufferSize => 1024 * 256;

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        GateIoSymbolMapper.ToGateIoContract(tradingPair);

    protected override string MapExchangeSymbolToTradingPair(string exchangeSymbol) =>
        GateIoSymbolMapper.FromGateIoContract(exchangeSymbol);

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
        string @event,
        IReadOnlyList<string> contracts,
        CancellationToken ct,
        bool isSubscribe)
    {
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

        if (isSubscribe)
        {
            _logger.LogInformation(
                "Gate.io depth subscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                contracts.Count,
                SubscribedSymbolCount,
                string.Join(", ", contracts.Take(5)));
        }
        else
        {
            _logger.LogInformation(
                "Gate.io depth unsubscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                contracts.Count,
                SubscribedSymbolCount,
                string.Join(", ", contracts.Take(5)));
        }
    }

    protected override Task? RunPingLoopAsync(ClientWebSocket socket, CancellationToken ct) =>
        PingLoopAsync(socket, ct);

    private async Task PingLoopAsync(ClientWebSocket socket, CancellationToken ct)
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
                "Failed to parse Gate.io depth message. Raw={Raw}",
                json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to process Gate.io depth message. Raw={Raw}",
                json);
        }

        await Task.CompletedTask;
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

        if (!IsSubscribed(item.Contract))
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

    private static OrderBookDepthLevel ParseLevel(GateIoOrderBookLevel level)
    {
        if (!TryReadDecimal(level.Price, out var price))
            return new OrderBookDepthLevel(Price: 0, Amount: 0);

        if (!TryReadDecimal(level.Size, out var size))
            return new OrderBookDepthLevel(Price: 0, Amount: 0);

        return new OrderBookDepthLevel(Price: price, Amount: Math.Abs(size));
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
