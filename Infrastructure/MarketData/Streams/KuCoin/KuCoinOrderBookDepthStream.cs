using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.KuCoin;

public sealed class KuCoinOrderBookDepthStream : IOrderBookDepthStream
{
    private const string Connector = "kucoin_perpetual";

    private readonly KuCoinFuturesWebSocketTokenProvider _tokenProvider;
    private readonly DepthSubscriptionTargetStore _targetStore;
    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger<KuCoinOrderBookDepthStream> _logger;

    private readonly HashSet<string> _subscribedSymbols =
        new(StringComparer.OrdinalIgnoreCase);

    private SemaphoreSlim? _sendLock;
    private int _successfulUpdatesLogged;

    public string ConnectorName => Connector;

    public KuCoinOrderBookDepthStream(
        KuCoinFuturesWebSocketTokenProvider tokenProvider,
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger<KuCoinOrderBookDepthStream> logger)
    {
        _tokenProvider = tokenProvider;
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
                    "KuCoin depth stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(CancellationToken ct)
    {
        var connectionInfo = await _tokenProvider.GetConnectionInfoAsync(ct);

        using var socket = new ClientWebSocket();

        _sendLock = new SemaphoreSlim(1, 1);
        _subscribedSymbols.Clear();
        _successfulUpdatesLogged = 0;

        _logger.LogInformation("Connecting to KuCoin depth stream.");

        await socket.ConnectAsync(new Uri(connectionInfo.WebSocketUrl), ct);

        _logger.LogInformation("Connected to KuCoin depth stream.");

        var receiveTask = ReceiveLoopAsync(socket, ct);
        var pingTask = PingLoopAsync(socket, connectionInfo.PingIntervalMs, ct);
        var subscriptionTask = SubscriptionLoopAsync(socket, ct);

        await Task.WhenAny(receiveTask, pingTask, subscriptionTask);
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
                .Select(KuCoinSymbolMapper.ToKuCoinSymbol)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var toSubscribe = desiredSymbols
                .Except(_subscribedSymbols, StringComparer.OrdinalIgnoreCase)
                .ToList();

            var toUnsubscribe = _subscribedSymbols
                .Except(desiredSymbols, StringComparer.OrdinalIgnoreCase)
                .ToList();

            foreach (var symbol in toSubscribe)
            {
                await SendSubscriptionAsync(
                    socket,
                    type: "subscribe",
                    symbol,
                    ct);

                _subscribedSymbols.Add(symbol);

                _logger.LogInformation(
                    "KuCoin depth subscribe requested. Symbol={Symbol}, Total={Total}",
                    symbol,
                    _subscribedSymbols.Count);

                await Task.Delay(50, ct);
            }

            foreach (var symbol in toUnsubscribe)
            {
                await SendSubscriptionAsync(
                    socket,
                    type: "unsubscribe",
                    symbol,
                    ct);

                _subscribedSymbols.Remove(symbol);

                _depthCache.Remove(
                    Connector,
                    KuCoinSymbolMapper.FromKuCoinSymbol(symbol));

                _logger.LogInformation(
                    "KuCoin depth unsubscribe requested. Symbol={Symbol}, Total={Total}",
                    symbol,
                    _subscribedSymbols.Count);

                await Task.Delay(50, ct);
            }

            await Task.Delay(1000, ct);
        }
    }

    private async Task SendSubscriptionAsync(
        ClientWebSocket socket,
        string type,
        string symbol,
        CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(new
        {
            id = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(
                CultureInfo.InvariantCulture),
            type,
            topic = $"/contractMarket/level2Depth5:{symbol}",
            response = true
        });

        await SendTextAsync(socket, payload, ct);
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
                    "Failed to parse KuCoin depth message. Raw={Raw}",
                    message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to process KuCoin depth message. Raw={Raw}",
                    message);
            }
        }
    }

    private async Task PingLoopAsync(
        ClientWebSocket socket,
        int pingIntervalMs,
        CancellationToken ct)
    {
        var delayMs = Math.Max(5000, pingIntervalMs - 1000);

        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            await Task.Delay(delayMs, ct);

            if (socket.State != WebSocketState.Open)
                break;

            var payload = JsonSerializer.Serialize(new
            {
                id = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(
                    CultureInfo.InvariantCulture),
                type = "ping"
            });

            await SendTextAsync(socket, payload, ct);
        }
    }

    private void ProcessMessage(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (!TryGetStringProperty(root, "type", out var type))
            return;

        if (string.Equals(type, "ack", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(type, "pong", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(type, "welcome", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (!string.Equals(type, "message", StringComparison.OrdinalIgnoreCase))
            return;

        if (!TryGetStringProperty(root, "topic", out var topic))
            return;

        if (!topic.StartsWith(
                "/contractMarket/level2Depth5:",
                StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var symbol = topic.Split(':').LastOrDefault() ?? "";

        if (string.IsNullOrWhiteSpace(symbol))
            return;

        if (!_subscribedSymbols.Contains(symbol))
            return;

        if (!TryGetPropertyIgnoreCase(root, "data", out var data))
            return;

        ProcessOrderBook(symbol, data);
    }

    private void ProcessOrderBook(
        string symbol,
        JsonElement data)
    {
        if (!TryGetPropertyIgnoreCase(data, "bids", out var bidsElement))
            return;

        if (!TryGetPropertyIgnoreCase(data, "asks", out var asksElement))
            return;

        var bids = ReadLevels(bidsElement)
            .OrderByDescending(x => x.Price)
            .ToList();

        var asks = ReadLevels(asksElement)
            .OrderBy(x => x.Price)
            .ToList();

        if (bids.Count == 0 || asks.Count == 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = TryGetLongProperty(data, "ts", out var ts) && ts > 0
            ? ConvertKuCoinTimestamp(ts)
            : receivedAt;

        var snapshot = new OrderBookDepthSnapshot(
            ConnectorName: Connector,
            TradingPair: KuCoinSymbolMapper.FromKuCoinSymbol(symbol),
            Bids: bids,
            Asks: asks,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _depthCache.Set(snapshot);

        if (Interlocked.CompareExchange(ref _successfulUpdatesLogged, 1, 0) == 0)
        {
            _logger.LogInformation(
                "KuCoin depth first update received. Symbol={Symbol}, Bids={Bids}, Asks={Asks}",
                symbol,
                bids.Count,
                asks.Count);
        }
    }

    private static IReadOnlyList<OrderBookDepthLevel> ReadLevels(
        JsonElement levelsElement)
    {
        if (levelsElement.ValueKind != JsonValueKind.Array)
            return [];

        var result = new List<OrderBookDepthLevel>();

        foreach (var level in levelsElement.EnumerateArray())
        {
            if (level.ValueKind != JsonValueKind.Array)
                continue;

            var values = level.EnumerateArray().ToList();

            if (values.Count < 2)
                continue;

            if (!TryReadDecimal(values[0], out var price))
                continue;

            if (!TryReadDecimal(values[1], out var amount))
                continue;

            if (price <= 0 || amount <= 0)
                continue;

            result.Add(new OrderBookDepthLevel(price, amount));
        }

        return result;
    }

    private async Task SendTextAsync(
        ClientWebSocket socket,
        string payload,
        CancellationToken ct)
    {
        if (_sendLock is null)
            throw new InvalidOperationException("KuCoin send lock is not initialized.");

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

    private static DateTimeOffset ConvertKuCoinTimestamp(long timestamp)
    {
        if (timestamp > 1_000_000_000_000_000)
            return DateTimeOffset.FromUnixTimeMilliseconds(timestamp / 1_000_000);

        if (timestamp > 10_000_000_000)
            return DateTimeOffset.FromUnixTimeMilliseconds(timestamp);

        return DateTimeOffset.FromUnixTimeSeconds(timestamp);
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

    private static bool TryGetPropertyIgnoreCase(
        JsonElement element,
        string propertyName,
        out JsonElement value)
    {
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