using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Okx;

public sealed class OkxOrderBookDepthStream : IOrderBookDepthStream
{
    private const string Connector = "okx_perpetual";
    private const string WsUrl = "wss://ws.okx.com:8443/ws/v5/public";

    // books5 = легкий top-5 стакан. Для первого production-like depth validation достаточно.
    private const string Channel = "books5";

    private readonly DepthSubscriptionTargetStore _targetStore;
    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger<OkxOrderBookDepthStream> _logger;

    private readonly HashSet<string> _subscribedInstrumentIds =
        new(StringComparer.OrdinalIgnoreCase);

    private SemaphoreSlim? _sendLock;

    public string ConnectorName => Connector;

    public OkxOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger<OkxOrderBookDepthStream> logger)
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
                    "OKX depth stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _sendLock = new SemaphoreSlim(1, 1);
        _subscribedInstrumentIds.Clear();

        _logger.LogInformation("Connecting to OKX depth stream: {Url}", WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation("Connected to OKX depth stream.");

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
            var desiredInstrumentIds = _targetStore
                .GetPairsForConnector(Connector)
                .Select(OkxSymbolMapper.ToOkxInstrumentId)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var toSubscribe = desiredInstrumentIds
                .Except(_subscribedInstrumentIds, StringComparer.OrdinalIgnoreCase)
                .ToList();

            var toUnsubscribe = _subscribedInstrumentIds
                .Except(desiredInstrumentIds, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (toSubscribe.Count > 0)
            {
                await SendSubscriptionAsync(
                    socket,
                    op: "subscribe",
                    instrumentIds: toSubscribe,
                    ct);

                foreach (var instrumentId in toSubscribe)
                    _subscribedInstrumentIds.Add(instrumentId);

                _logger.LogInformation(
                    "OKX depth subscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                    toSubscribe.Count,
                    _subscribedInstrumentIds.Count,
                    string.Join(", ", toSubscribe.Take(5)));
            }

            if (toUnsubscribe.Count > 0)
            {
                await SendSubscriptionAsync(
                    socket,
                    op: "unsubscribe",
                    instrumentIds: toUnsubscribe,
                    ct);

                foreach (var instrumentId in toUnsubscribe)
                {
                    _subscribedInstrumentIds.Remove(instrumentId);

                    var tradingPair = OkxSymbolMapper.FromOkxInstrumentId(instrumentId);

                    _depthCache.Remove(Connector, tradingPair);
                }

                _logger.LogInformation(
                    "OKX depth unsubscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                    toUnsubscribe.Count,
                    _subscribedInstrumentIds.Count,
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
                    "Failed to parse OKX depth message. Raw={Raw}",
                    message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to process OKX depth message. Raw={Raw}",
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

        if (!_subscribedInstrumentIds.Contains(instrumentId))
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

    private async Task SendSubscriptionAsync(
        ClientWebSocket socket,
        string op,
        IReadOnlyList<string> instrumentIds,
        CancellationToken ct)
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
    }

    private async Task SendTextAsync(
        ClientWebSocket socket,
        string payload,
        CancellationToken ct)
    {
        if (_sendLock is null)
            throw new InvalidOperationException("OKX send lock is not initialized.");

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