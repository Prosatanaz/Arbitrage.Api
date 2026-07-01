using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Bitget;

public sealed class BitgetOrderBookDepthStream : DepthStreamBase
{
    private const string Connector = "bitget_perpetual";
    private const string InstType = "USDT-FUTURES";
    private const string Channel = "books5";

    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger<BitgetOrderBookDepthStream> _logger;

    public BitgetOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger<BitgetOrderBookDepthStream> logger)
        : base(targetStore, depthCache, logger)
    {
        _depthCache = depthCache;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://ws.bitget.com/v2/ws/public";

    protected override int ReceiveBufferSize => 1024 * 256;

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        BitgetSymbolMapper.ToBitgetSymbol(tradingPair);

    protected override string MapExchangeSymbolToTradingPair(string exchangeSymbol) =>
        BitgetSymbolMapper.FromBitgetSymbol(exchangeSymbol);

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
        string op,
        IReadOnlyList<string> symbols,
        CancellationToken ct,
        bool isSubscribe)
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

        if (isSubscribe)
        {
            _logger.LogInformation(
                "Bitget depth subscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                symbols.Count,
                SubscribedSymbolCount,
                string.Join(", ", symbols.Take(5)));
        }
        else
        {
            _logger.LogInformation(
                "Bitget depth unsubscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                symbols.Count,
                SubscribedSymbolCount,
                string.Join(", ", symbols.Take(5)));
        }
    }

    protected override Task? RunPingLoopAsync(ClientWebSocket socket, CancellationToken ct) =>
        PingLoopAsync(socket, ct);

    private async Task PingLoopAsync(ClientWebSocket socket, CancellationToken ct)
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
                "Failed to parse Bitget depth message. Raw={Raw}",
                json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to process Bitget depth message. Raw={Raw}",
                json);
        }

        await Task.CompletedTask;
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

        if (!IsSubscribed(symbol))
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
