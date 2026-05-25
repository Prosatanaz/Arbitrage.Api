using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Bitget;

public sealed class BitgetBboStream : IBestBidAskStream
{
    private const string Connector = "bitget_perpetual";
    private const string WsUrl = "wss://ws.bitget.com/v2/ws/public";
    private const string InstType = "USDT-FUTURES";
    private const string Channel = "ticker";

    private readonly BestBidAskCache _cache;
    private readonly ILogger<BitgetBboStream> _logger;

    private readonly HashSet<string> _subscribedSymbols =
        new(StringComparer.OrdinalIgnoreCase);

    private SemaphoreSlim? _sendLock;

    public string ConnectorName => Connector;

    public BitgetBboStream(
        BestBidAskCache cache,
        ILogger<BitgetBboStream> logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public async Task StartAsync(
        IReadOnlyList<string> tradingPairs,
        CancellationToken ct)
    {
        var symbols = tradingPairs
            .Select(BitgetSymbolMapper.ToBitgetSymbol)
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
                    "Bitget BBO stream crashed. Reconnecting in 5 seconds...");

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
        _subscribedSymbols.Clear();

        _logger.LogInformation("Connecting to Bitget BBO stream: {Url}", WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation(
            "Connected to Bitget BBO stream. Symbols={Symbols}",
            symbols.Count);

        await SubscribeAsync(socket, symbols, ct);

        var receiveTask = ReceiveLoopAsync(socket, ct);
        var pingTask = PingLoopAsync(socket, ct);

        await Task.WhenAny(receiveTask, pingTask);
    }

    private async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbols,
        CancellationToken ct)
    {
        // Bitget recommends fewer than 50 channels per connection for stability.
        const int batchSize = 30;

        foreach (var batch in symbols.Chunk(batchSize))
        {
            var payload = JsonSerializer.Serialize(new
            {
                op = "subscribe",
                args = batch.Select(symbol => new
                {
                    instType = InstType,
                    channel = Channel,
                    instId = symbol
                }).ToList()
            });

            await SendTextAsync(socket, payload, ct);

            foreach (var symbol in batch)
                _subscribedSymbols.Add(symbol);

            _logger.LogInformation(
                "Bitget BBO subscribe requested. BatchSize={BatchSize}, Total={Total}",
                batch.Length,
                _subscribedSymbols.Count);

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
                    "Failed to parse Bitget BBO message. Raw={Raw}",
                    message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to process Bitget BBO message. Raw={Raw}",
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
            await Task.Delay(TimeSpan.FromSeconds(30), ct);

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

            if (string.Equals(eventName, "error", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning("Bitget BBO WS error. Raw={Raw}", json);
            }
            else
            {
                _logger.LogDebug("Bitget BBO WS control. Raw={Raw}", json);
            }

            return;
        }

        var dto = JsonSerializer.Deserialize<BitgetTickerMessage>(json);

        if (dto?.Data is null || dto.Data.Count == 0)
            return;

        foreach (var item in dto.Data)
        {
            ProcessTicker(item);
        }
    }

    private void ProcessTicker(BitgetTickerData item)
    {
        var symbol = !string.IsNullOrWhiteSpace(item.InstrumentId)
            ? item.InstrumentId
            : item.Symbol;

        if (string.IsNullOrWhiteSpace(symbol))
            return;

        if (!_subscribedSymbols.Contains(symbol))
            return;

        if (!TryParseDecimal(item.BidPrice, out var bidPrice))
            return;

        if (!TryParseDecimal(item.AskPrice, out var askPrice))
            return;

        if (!TryParseDecimal(item.BidSize, out var bidAmount))
            bidAmount = 0;

        if (!TryParseDecimal(item.AskSize, out var askAmount))
            askAmount = 0;

        if (bidPrice <= 0 || askPrice <= 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = long.TryParse(
            item.TimestampMs,
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out var ts) && ts > 0
                ? DateTimeOffset.FromUnixTimeMilliseconds(ts)
                : receivedAt;

        var snapshot = new BestBidAskSnapshot(
            ConnectorName: Connector,
            TradingPair: BitgetSymbolMapper.FromBitgetSymbol(symbol),
            BestBidPrice: bidPrice,
            BestBidAmount: bidAmount,
            BestAskPrice: askPrice,
            BestAskAmount: askAmount,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _cache.Set(snapshot);
    }

    private async Task SendTextAsync(
        ClientWebSocket socket,
        string payload,
        CancellationToken ct)
    {
        if (_sendLock is null)
            throw new InvalidOperationException("Bitget send lock is not initialized.");

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

    private static bool TryParseDecimal(
        string? value,
        out decimal result)
    {
        return decimal.TryParse(
            value,
            NumberStyles.Number,
            CultureInfo.InvariantCulture,
            out result);
    }

    private sealed class BitgetTickerMessage
    {
        [JsonPropertyName("arg")]
        public BitgetArg? Arg { get; init; }

        [JsonPropertyName("action")]
        public string? Action { get; init; }

        [JsonPropertyName("data")]
        public List<BitgetTickerData> Data { get; init; } = new();
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

    private sealed class BitgetTickerData
    {
        [JsonPropertyName("instId")]
        public string InstrumentId { get; init; } = "";

        [JsonPropertyName("symbol")]
        public string Symbol { get; init; } = "";

        [JsonPropertyName("bidPr")]
        public string? BidPrice { get; init; }

        [JsonPropertyName("askPr")]
        public string? AskPrice { get; init; }

        [JsonPropertyName("bidSz")]
        public string? BidSize { get; init; }

        [JsonPropertyName("askSz")]
        public string? AskSize { get; init; }

        [JsonPropertyName("ts")]
        public string? TimestampMs { get; init; }
    }
}