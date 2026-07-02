using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.KuCoin;

public sealed class KuCoinOrderBookDepthStream : DepthStreamBase
{
    private const string Connector = "kucoin_perpetual";

    private readonly KuCoinFuturesWebSocketTokenProvider _tokenProvider;
    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger<KuCoinOrderBookDepthStream> _logger;

    private int _pingIntervalMs = 18000;
    private int _successfulUpdatesLogged;

    public KuCoinOrderBookDepthStream(
        KuCoinFuturesWebSocketTokenProvider tokenProvider,
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger<KuCoinOrderBookDepthStream> logger)
        : base(targetStore, depthCache, logger)
    {
        _tokenProvider = tokenProvider;
        _depthCache = depthCache;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override int ReceiveBufferSize => 1024 * 256;

    protected override void OnConnectionReset()
    {
        _successfulUpdatesLogged = 0;
    }

    protected override void OnConnecting()
    {
        _logger.LogInformation("Connecting to KuCoin depth stream.");
    }

    protected override async Task<Uri> ResolveWebSocketUrlAsync(CancellationToken ct)
    {
        var connectionInfo = await _tokenProvider.GetConnectionInfoAsync(ct);

        _pingIntervalMs = connectionInfo.PingIntervalMs;

        return new Uri(connectionInfo.WebSocketUrl);
    }

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        KuCoinSymbolMapper.ToKuCoinSymbol(tradingPair);

    protected override string MapExchangeSymbolToTradingPair(string exchangeSymbol) =>
        KuCoinSymbolMapper.FromKuCoinSymbol(exchangeSymbol);

    protected override async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbolsToAdd,
        CancellationToken ct)
    {
        foreach (var symbol in symbolsToAdd)
        {
            await SendSubscriptionAsync(socket, "subscribe", symbol, ct);

            _logger.LogInformation(
                "KuCoin depth subscribe requested. Symbol={Symbol}, Total={Total}",
                symbol,
                SubscribedSymbolCount);

            await Task.Delay(50, ct);
        }
    }

    protected override async Task UnsubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbolsToRemove,
        CancellationToken ct)
    {
        foreach (var symbol in symbolsToRemove)
        {
            await SendSubscriptionAsync(socket, "unsubscribe", symbol, ct);

            _logger.LogInformation(
                "KuCoin depth unsubscribe requested. Symbol={Symbol}, Total={Total}",
                symbol,
                SubscribedSymbolCount);

            await Task.Delay(50, ct);
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

    protected override Task? RunPingLoopAsync(ClientWebSocket socket, CancellationToken ct) =>
        PingLoopAsync(socket, ct);

    private async Task PingLoopAsync(ClientWebSocket socket, CancellationToken ct)
    {
        var delayMs = Math.Max(5000, _pingIntervalMs - 1000);

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
                "Failed to parse KuCoin depth message. Raw={Raw}",
                json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to process KuCoin depth message. Raw={Raw}",
                json);
        }

        await Task.CompletedTask;
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

        if (!IsSubscribed(symbol))
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

    private static DateTimeOffset ConvertKuCoinTimestamp(long timestamp)
    {
        if (timestamp > 1_000_000_000_000_000)
            return DateTimeOffset.FromUnixTimeMilliseconds(timestamp / 1_000_000);

        if (timestamp > 10_000_000_000)
            return DateTimeOffset.FromUnixTimeMilliseconds(timestamp);

        return DateTimeOffset.FromUnixTimeSeconds(timestamp);
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
