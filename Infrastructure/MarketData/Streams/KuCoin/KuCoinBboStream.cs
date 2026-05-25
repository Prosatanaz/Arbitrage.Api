using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.KuCoin;

public sealed class KuCoinBboStream : IBestBidAskStream
{
    private const string Connector = "kucoin_perpetual";

    private readonly KuCoinFuturesWebSocketTokenProvider _tokenProvider;
    private readonly BestBidAskCache _cache;
    private readonly ILogger<KuCoinBboStream> _logger;

    private readonly HashSet<string> _subscribedSymbols =
        new(StringComparer.OrdinalIgnoreCase);

    private SemaphoreSlim? _sendLock;
    private int _successfulUpdatesLogged;

    public string ConnectorName => Connector;

    public KuCoinBboStream(
        KuCoinFuturesWebSocketTokenProvider tokenProvider,
        BestBidAskCache cache,
        ILogger<KuCoinBboStream> logger)
    {
        _tokenProvider = tokenProvider;
        _cache = cache;
        _logger = logger;
    }

    public async Task StartAsync(
        IReadOnlyList<string> tradingPairs,
        CancellationToken ct)
    {
        var symbols = tradingPairs
            .Select(KuCoinSymbolMapper.ToKuCoinSymbol)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await RunConnectionAsync(symbols, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "KuCoin BBO stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(
        IReadOnlyList<string> symbols,
        CancellationToken ct)
    {
        var connectionInfo = await _tokenProvider.GetConnectionInfoAsync(ct);

        using var socket = new ClientWebSocket();

        _sendLock = new SemaphoreSlim(1, 1);
        _subscribedSymbols.Clear();
        _successfulUpdatesLogged = 0;

        _logger.LogInformation("Connecting to KuCoin BBO stream.");

        await socket.ConnectAsync(new Uri(connectionInfo.WebSocketUrl), ct);

        _logger.LogInformation(
            "Connected to KuCoin BBO stream. Symbols={Symbols}",
            symbols.Count);

        var receiveTask = ReceiveLoopAsync(socket, ct);
        var pingTask = PingLoopAsync(socket, connectionInfo.PingIntervalMs, ct);
        var subscriptionTask = SubscribeAsync(socket, symbols, ct);

        while (!ct.IsCancellationRequested)
        {
            var completedTask = await Task.WhenAny(
                receiveTask,
                pingTask,
                subscriptionTask);

            if (completedTask == subscriptionTask)
            {
                await subscriptionTask;

                _logger.LogInformation(
                    "KuCoin BBO subscription completed. Total={Total}",
                    _subscribedSymbols.Count);

                subscriptionTask = Task.Delay(
                    Timeout.InfiniteTimeSpan,
                    ct);

                continue;
            }

            await completedTask;
            break;
        }
    }

    private async Task SubscribeAsync(
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

            _subscribedSymbols.Add(symbol);

            await Task.Delay(25, ct);
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
                    "Failed to parse KuCoin BBO message. Raw={Raw}",
                    message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to process KuCoin BBO message. Raw={Raw}",
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

        if (!_subscribedSymbols.Contains(symbol))
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