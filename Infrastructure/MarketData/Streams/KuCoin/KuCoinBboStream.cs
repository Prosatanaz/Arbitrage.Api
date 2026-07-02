using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.KuCoin;

public sealed class KuCoinBboStream : BboStreamBase
{
    private const string Connector = "kucoin_perpetual";

    private readonly KuCoinFuturesWebSocketTokenProvider _tokenProvider;
    private readonly BestBidAskCache _cache;
    private readonly ILogger<KuCoinBboStream> _logger;

    private int _pingIntervalMs = 18000;
    private int _successfulUpdatesLogged;

    public KuCoinBboStream(
        KuCoinFuturesWebSocketTokenProvider tokenProvider,
        BestBidAskCache cache,
        ILogger<KuCoinBboStream> logger)
        : base(logger)
    {
        _tokenProvider = tokenProvider;
        _cache = cache;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override int ReceiveBufferSize => 1024 * 256;

    protected override bool SubscribeConcurrently => true;

    protected override void OnConnecting()
    {
        _successfulUpdatesLogged = 0;
        _logger.LogInformation("Connecting to KuCoin BBO stream.");
    }

    protected override async Task<Uri> ResolveWebSocketUrlAsync(CancellationToken ct)
    {
        var connectionInfo = await _tokenProvider.GetConnectionInfoAsync(ct);

        _pingIntervalMs = connectionInfo.PingIntervalMs;

        return new Uri(connectionInfo.WebSocketUrl);
    }

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        KuCoinSymbolMapper.ToKuCoinSymbol(tradingPair);

    protected override async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbols,
        CancellationToken ct)
    {
        foreach (var symbol in symbols)
        {
            var payload = JsonSerializer.Serialize(new
            {
                id = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(
                    CultureInfo.InvariantCulture),
                type = "subscribe",
                topic = $"/contractMarket/tickerV2:{symbol}",
                response = true
            });

            await SendTextAsync(socket, payload, ct);

            await Task.Delay(25, ct);
        }

        _logger.LogInformation(
            "KuCoin BBO subscription completed. Total={Total}",
            symbols.Count);
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
                "Failed to parse KuCoin BBO message. Raw={Raw}",
                json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to process KuCoin BBO message. Raw={Raw}",
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
                "/contractMarket/tickerV2:",
                StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (!TryGetPropertyIgnoreCase(root, "data", out var data))
            return;

        ProcessTicker(data);
    }

    private void ProcessTicker(JsonElement data)
    {
        if (!TryGetStringProperty(data, "symbol", out var symbol))
            return;

        if (!AllowedSymbols.Contains(symbol))
            return;

        if (!TryGetDecimalProperty(data, "bestBidPrice", out var bidPrice))
            return;

        if (!TryGetDecimalProperty(data, "bestAskPrice", out var askPrice))
            return;

        if (!TryGetDecimalProperty(data, "bestBidSize", out var bidAmount))
            bidAmount = 0;

        if (!TryGetDecimalProperty(data, "bestAskSize", out var askAmount))
            askAmount = 0;

        if (bidPrice <= 0 || askPrice <= 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = TryGetLongProperty(data, "ts", out var ts) && ts > 0
            ? ConvertKuCoinTimestamp(ts)
            : receivedAt;

        var snapshot = new BestBidAskSnapshot(
            ConnectorName: Connector,
            TradingPair: KuCoinSymbolMapper.FromKuCoinSymbol(symbol),
            BestBidPrice: bidPrice,
            BestBidAmount: bidAmount,
            BestAskPrice: askPrice,
            BestAskAmount: askAmount,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _cache.Set(snapshot);

        if (Interlocked.CompareExchange(ref _successfulUpdatesLogged, 1, 0) == 0)
        {
            _logger.LogInformation(
                "KuCoin BBO first update received. Symbol={Symbol}, Bid={Bid}, Ask={Ask}",
                symbol,
                bidPrice,
                askPrice);
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

    private static bool TryGetDecimalProperty(
        JsonElement element,
        string propertyName,
        out decimal value)
    {
        value = 0;

        if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
            return false;

        if (property.ValueKind == JsonValueKind.Number)
            return property.TryGetDecimal(out value);

        if (property.ValueKind == JsonValueKind.String)
        {
            return decimal.TryParse(
                property.GetString(),
                NumberStyles.Number,
                CultureInfo.InvariantCulture,
                out value);
        }

        return false;
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
}
