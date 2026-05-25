using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Mexc;

public sealed class MexcBboStream : IBestBidAskStream
{
    private const string Connector = "mexc_perpetual";
    private const string WsUrl = "wss://contract.mexc.com/edge";
    private const string Channel = "push.tickers";

    private readonly BestBidAskCache _cache;
    private readonly ILogger<MexcBboStream> _logger;

    private readonly HashSet<string> _allowedSymbols =
        new(StringComparer.OrdinalIgnoreCase);

    private SemaphoreSlim? _sendLock;
    private int _successfulUpdatesLogged;

    public string ConnectorName => Connector;

    public MexcBboStream(
        BestBidAskCache cache,
        ILogger<MexcBboStream> logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public async Task StartAsync(
        IReadOnlyList<string> tradingPairs,
        CancellationToken ct)
    {
        var symbols = tradingPairs
            .Select(MexcSymbolMapper.ToMexcSymbol)
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
                    "MEXC BBO stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(
        IReadOnlyList<string> symbols,
        CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _sendLock = new SemaphoreSlim(1, 1);
        _allowedSymbols.Clear();
        _successfulUpdatesLogged = 0;

        foreach (var symbol in symbols)
        {
            _allowedSymbols.Add(symbol);
        }

        _logger.LogInformation("Connecting to MEXC BBO stream: {Url}", WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation(
            "Connected to MEXC BBO stream. AllowedSymbols={AllowedSymbols}",
            _allowedSymbols.Count);

        var receiveTask = ReceiveLoopAsync(socket, ct);
        var pingTask = PingLoopAsync(socket, ct);
        var subscriptionTask = SubscribeAsync(socket, ct);

        while (!ct.IsCancellationRequested)
        {
            var completedTask = await Task.WhenAny(
                receiveTask,
                pingTask,
                subscriptionTask);

            if (completedTask == subscriptionTask)
            {
                await subscriptionTask;

                _logger.LogInformation("MEXC BBO subscription completed.");

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
        CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(new
        {
            method = "sub.tickers",
            param = new { },
            gzip = false
        });

        await SendTextAsync(socket, payload, ct);
    }

    private async Task ReceiveLoopAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        var buffer = new byte[1024 * 512];

        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            var message = await ReceiveTextAsync(socket, buffer, ct);

            if (message is null)
            {
                break;
            }

            try
            {
                ProcessMessage(message);
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to parse MEXC BBO message. Raw={Raw}",
                    message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to process MEXC BBO message. Raw={Raw}",
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
            await Task.Delay(TimeSpan.FromSeconds(15), ct);

            if (socket.State != WebSocketState.Open)
            {
                break;
            }

            var payload = JsonSerializer.Serialize(new
            {
                method = "ping"
            });

            await SendTextAsync(socket, payload, ct);
        }
    }

    private void ProcessMessage(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (!TryGetStringProperty(root, "channel", out var channel))
        {
            return;
        }

        if (string.Equals(channel, "pong", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (!string.Equals(channel, Channel, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (!TryGetPropertyIgnoreCase(root, "data", out var dataElement))
        {
            return;
        }

        if (dataElement.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in dataElement.EnumerateArray())
            {
                if (item.ValueKind == JsonValueKind.Object)
                {
                    ProcessTicker(item);
                }
            }

            return;
        }

        if (dataElement.ValueKind == JsonValueKind.Object)
        {
            ProcessTicker(dataElement);
        }
    }

    private void ProcessTicker(JsonElement item)
    {
        if (!TryGetStringProperty(item, "symbol", out var symbol))
        {
            return;
        }

        if (!_allowedSymbols.Contains(symbol))
        {
            return;
        }

        if (!TryGetDecimalProperty(item, "maxBidPrice", out var bidPrice))
        {
            return;
        }

        if (!TryGetDecimalProperty(item, "minAskPrice", out var askPrice))
        {
            return;
        }

        if (bidPrice <= 0 || askPrice <= 0)
        {
            return;
        }

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = TryGetLongProperty(item, "timestamp", out var timestamp) &&
                                timestamp > 0
            ? ConvertTimestamp(timestamp)
            : receivedAt;

        var snapshot = new BestBidAskSnapshot(
            ConnectorName: Connector,
            TradingPair: MexcSymbolMapper.FromMexcSymbol(symbol),
            BestBidPrice: bidPrice,
            BestBidAmount: 0,
            BestAskPrice: askPrice,
            BestAskAmount: 0,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _cache.Set(snapshot);

        if (Interlocked.CompareExchange(ref _successfulUpdatesLogged, 1, 0) == 0)
        {
            _logger.LogInformation(
                "MEXC BBO first update received. Symbol={Symbol}, Bid={Bid}, Ask={Ask}",
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
        {
            throw new InvalidOperationException("MEXC send lock is not initialized.");
        }

        await _sendLock.WaitAsync(ct);

        try
        {
            if (socket.State != WebSocketState.Open)
            {
                return;
            }

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
            {
                return null;
            }

            memory.Write(buffer, 0, result.Count);

            if (result.EndOfMessage)
            {
                break;
            }
        }

        return Encoding.UTF8.GetString(memory.ToArray());
    }

    private static DateTimeOffset ConvertTimestamp(long timestamp)
    {
        if (timestamp > 10_000_000_000)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(timestamp);
        }

        return DateTimeOffset.FromUnixTimeSeconds(timestamp);
    }

    private static bool TryGetPropertyIgnoreCase(
        JsonElement element,
        string propertyName,
        out JsonElement value)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            value = default;
            return false;
        }

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