using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Okx;

public sealed class OkxOrderBookDepthStream : DepthStreamBase
{
    private const string Connector = "okx_perpetual";

    // books5 = легкий top-5 стакан. Для первого production-like depth validation достаточно.
    private const string Channel = "books5";

    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger<OkxOrderBookDepthStream> _logger;

    public OkxOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger<OkxOrderBookDepthStream> logger)
        : base(targetStore, depthCache, logger)
    {
        _depthCache = depthCache;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://ws.okx.com:8443/ws/v5/public";

    protected override int ReceiveBufferSize => 1024 * 256;

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        OkxSymbolMapper.ToOkxInstrumentId(tradingPair);

    protected override string MapExchangeSymbolToTradingPair(string exchangeSymbol) =>
        OkxSymbolMapper.FromOkxInstrumentId(exchangeSymbol);

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
        IReadOnlyList<string> instrumentIds,
        CancellationToken ct,
        bool isSubscribe)
    {
        // OKX чувствителен к частоте команд. Не шлем пачки слишком быстро.
        // Обычно targets <= 20, но batch оставляем для защиты.
        const int batchSize = 10;

        foreach (var batch in instrumentIds.Chunk(batchSize))
        {
            var payload = JsonSerializer.Serialize(new
            {
                op,
                args = batch.Select(instrumentId => new
                {
                    channel = Channel,
                    instId = instrumentId
                }).ToList()
            });

            await SendTextAsync(socket, payload, ct);

            // Не 100ms. Для OKX безопаснее не спамить control messages.
            await Task.Delay(1000, ct);
        }

        if (isSubscribe)
        {
            _logger.LogInformation(
                "OKX depth subscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                instrumentIds.Count,
                SubscribedSymbolCount,
                string.Join(", ", instrumentIds.Take(5)));
        }
        else
        {
            _logger.LogInformation(
                "OKX depth unsubscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                instrumentIds.Count,
                SubscribedSymbolCount,
                string.Join(", ", instrumentIds.Take(5)));
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
                "Failed to parse OKX depth message. Raw={Raw}",
                json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to process OKX depth message. Raw={Raw}",
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

            var code = root.TryGetProperty("code", out var codeElement)
                ? codeElement.GetString()
                : null;

            var msg = root.TryGetProperty("msg", out var msgElement)
                ? msgElement.GetString()
                : null;

            var arg = root.TryGetProperty("arg", out var argElement)
                ? argElement.ToString()
                : null;

            if (string.Equals(eventName, "error", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning(
                    "OKX depth WS error. Code={Code}, Msg={Msg}, Arg={Arg}, Raw={Raw}",
                    code,
                    msg,
                    arg,
                    json);
            }
            else
            {
                _logger.LogInformation(
                    "OKX depth WS control. Event={Event}, Code={Code}, Msg={Msg}, Arg={Arg}",
                    eventName,
                    code,
                    msg,
                    arg);
            }

            return;
        }

        var dto = JsonSerializer.Deserialize<OkxOrderBookMessage>(json);

        if (dto?.Data is null || dto.Data.Count == 0)
            return;

        var instrumentIdFromArg = dto.Arg?.InstrumentId;

        foreach (var item in dto.Data)
        {
            ProcessOrderBookItem(
                instrumentIdFromArg,
                item);
        }
    }

    private void ProcessOrderBookItem(
        string? instrumentIdFromArg,
        OkxOrderBookData item)
    {
        var instrumentId = !string.IsNullOrWhiteSpace(item.InstrumentId)
            ? item.InstrumentId
            : instrumentIdFromArg;

        if (string.IsNullOrWhiteSpace(instrumentId))
            return;

        if (!IsSubscribed(instrumentId))
        {
            _logger.LogDebug(
                "OKX depth message ignored because instrument is not subscribed. InstrumentId={InstrumentId}",
                instrumentId);

            return;
        }

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
        {
            _logger.LogDebug(
                "OKX depth message ignored because bids or asks are empty. InstrumentId={InstrumentId}, Bids={Bids}, Asks={Asks}",
                instrumentId,
                bids.Count,
                asks.Count);

            return;
        }

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = long.TryParse(
            item.TimestampMs,
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out var ts) && ts > 0
                ? DateTimeOffset.FromUnixTimeMilliseconds(ts)
                : receivedAt;

        var tradingPair = OkxSymbolMapper.FromOkxInstrumentId(instrumentId);

        var snapshot = new OrderBookDepthSnapshot(
            ConnectorName: Connector,
            TradingPair: tradingPair,
            Bids: bids,
            Asks: asks,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _depthCache.Set(snapshot);
    }

    private static OrderBookDepthLevel ParseLevel(
        IReadOnlyList<string> values)
    {
        // OKX level:
        // [price, size, liquidated_orders, orders_count]
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

    private sealed class OkxOrderBookMessage
    {
        [JsonPropertyName("arg")]
        public OkxArg? Arg { get; init; }

        [JsonPropertyName("data")]
        public List<OkxOrderBookData> Data { get; init; } = new();
    }

    private sealed class OkxArg
    {
        [JsonPropertyName("channel")]
        public string Channel { get; init; } = default!;

        [JsonPropertyName("instId")]
        public string InstrumentId { get; init; } = default!;
    }

    private sealed class OkxOrderBookData
    {
        // В OKX instId обычно лежит в arg, но оставляем fallback.
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
