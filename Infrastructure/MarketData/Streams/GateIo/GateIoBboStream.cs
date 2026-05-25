using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.GateIo;

public sealed class GateIoBboStream : IBestBidAskStream
{
    private const string Connector = "gate_io_perpetual";
    private const string WsUrl = "wss://fx-ws.gateio.ws/v4/ws/usdt";
    private const string Channel = "futures.book_ticker";

    private readonly BestBidAskCache _cache;
    private readonly ILogger<GateIoBboStream> _logger;

    private readonly HashSet<string> _subscribedContracts =
        new(StringComparer.OrdinalIgnoreCase);

    private SemaphoreSlim? _sendLock;
    private int _successfulUpdatesLogged;

    public string ConnectorName => Connector;

    public GateIoBboStream(
        BestBidAskCache cache,
        ILogger<GateIoBboStream> logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public async Task StartAsync(
        IReadOnlyList<string> tradingPairs,
        CancellationToken ct)
    {
        var contracts = tradingPairs
            .Select(GateIoSymbolMapper.ToGateIoContract)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await RunConnectionAsync(contracts, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Gate.io BBO stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(
        IReadOnlyList<string> contracts,
        CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _sendLock = new SemaphoreSlim(1, 1);
        _subscribedContracts.Clear();
        _successfulUpdatesLogged = 0;

        _logger.LogInformation("Connecting to Gate.io BBO stream: {Url}", WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation(
            "Connected to Gate.io BBO stream. Contracts={Contracts}",
            contracts.Count);

        await SubscribeAsync(socket, contracts, ct);

        var receiveTask = ReceiveLoopAsync(socket, ct);
        var pingTask = PingLoopAsync(socket, ct);

        await Task.WhenAny(receiveTask, pingTask);
    }

    private async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> contracts,
        CancellationToken ct)
    {
        const int batchSize = 20;

        foreach (var batch in contracts.Chunk(batchSize))
        {
            var payload = JsonSerializer.Serialize(new
            {
                time = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                channel = Channel,
                @event = "subscribe",
                payload = batch
            });

            await SendTextAsync(socket, payload, ct);

            foreach (var contract in batch)
            {
                _subscribedContracts.Add(contract);
            }

            _logger.LogInformation(
                "Gate.io BBO subscribe requested. BatchSize={BatchSize}, Total={Total}",
                batch.Length,
                _subscribedContracts.Count);

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
                    "Failed to parse Gate.io BBO message. Raw={Raw}",
                    message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to process Gate.io BBO message. Raw={Raw}",
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
            {
                break;
            }

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
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (!TryGetStringProperty(root, "channel", out var channel))
        {
            return;
        }

        if (!string.Equals(channel, Channel, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (!TryGetStringProperty(root, "event", out var eventName))
        {
            return;
        }

        if (!string.Equals(eventName, "update", StringComparison.OrdinalIgnoreCase))
        {
            if (string.Equals(eventName, "subscribe", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogDebug("Gate.io BBO subscribed. Raw={Raw}", json);
            }

            if (string.Equals(eventName, "error", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning("Gate.io BBO WS error. Raw={Raw}", json);
            }

            return;
        }

        if (!TryGetPropertyIgnoreCase(root, "result", out var resultElement))
        {
            return;
        }

        if (resultElement.ValueKind == JsonValueKind.Object)
        {
            ProcessTicker(resultElement);
            return;
        }

        if (resultElement.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in resultElement.EnumerateArray())
            {
                if (item.ValueKind == JsonValueKind.Object)
                {
                    ProcessTicker(item);
                }
            }
        }
    }

    private void ProcessTicker(JsonElement ticker)
    {
        // Gate.io futures.book_ticker:
        // result.s = contract name
        // result.b = best bid price
        // result.B = best bid size
        // result.a = best ask price
        // result.A = best ask size
        // result.t = timestamp ms

        var contract = ReadContract(ticker);

        if (string.IsNullOrWhiteSpace(contract))
        {
            return;
        }

        if (!_subscribedContracts.Contains(contract))
        {
            return;
        }

        if (!TryGetDecimalProperty(ticker, "b", out var bidPrice))
        {
            return;
        }

        if (!TryGetDecimalProperty(ticker, "a", out var askPrice))
        {
            return;
        }

        if (!TryGetDecimalProperty(ticker, "B", out var bidAmount))
        {
            bidAmount = 0;
        }

        if (!TryGetDecimalProperty(ticker, "A", out var askAmount))
        {
            askAmount = 0;
        }

        if (bidPrice <= 0 || askPrice <= 0)
        {
            return;
        }

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = TryGetLongProperty(ticker, "t", out var timestampMs) &&
                                timestampMs > 0
            ? DateTimeOffset.FromUnixTimeMilliseconds(timestampMs)
            : receivedAt;

        var snapshot = new BestBidAskSnapshot(
            ConnectorName: Connector,
            TradingPair: GateIoSymbolMapper.FromGateIoContract(contract),
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
                "Gate.io BBO first update received. Contract={Contract}, Bid={Bid}, Ask={Ask}",
                contract,
                bidPrice,
                askPrice);
        }
    }

    private static string ReadContract(JsonElement ticker)
    {
        // Official futures.book_ticker uses "s".
        if (TryGetStringProperty(ticker, "s", out var symbol))
        {
            return symbol.ToUpperInvariant();
        }

        // Fallback for other Gate channels / future compatibility.
        if (TryGetStringProperty(ticker, "contract", out var contract))
        {
            return contract.ToUpperInvariant();
        }

        return "";
    }

    private async Task SendTextAsync(
        ClientWebSocket socket,
        string payload,
        CancellationToken ct)
    {
        if (_sendLock is null)
        {
            throw new InvalidOperationException("Gate.io send lock is not initialized.");
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

    private static bool TryGetPropertyIgnoreCase(
        JsonElement element,
        string propertyName,
        out JsonElement value)
    {
        if (element.TryGetProperty(propertyName, out value))
        {
            return true;
        }

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
        {
            return false;
        }

        if (property.ValueKind == JsonValueKind.String)
        {
            value = property.GetString() ?? "";
            return !string.IsNullOrWhiteSpace(value);
        }

        return false;
    }

    private static bool TryGetDecimalProperty(
        JsonElement element,
        string propertyName,
        out decimal value)
    {
        value = 0;

        if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
        {
            return false;
        }

        if (property.ValueKind == JsonValueKind.Number)
        {
            return property.TryGetDecimal(out value);
        }

        if (property.ValueKind == JsonValueKind.String)
        {
            var text = property.GetString();

            return decimal.TryParse(
                text,
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
        {
            return false;
        }

        if (property.ValueKind == JsonValueKind.Number)
        {
            return property.TryGetInt64(out value);
        }

        if (property.ValueKind == JsonValueKind.String)
        {
            var text = property.GetString();

            return long.TryParse(
                text,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out value);
        }

        return false;
    }
}