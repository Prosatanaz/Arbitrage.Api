using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Bybit;

public sealed class BybitBboStream : IBestBidAskStream
{
    private const string Connector = "bybit_perpetual";
    private const string WsUrl = "wss://stream.bybit.com/v5/public/linear";

    private readonly BestBidAskCache _cache;
    private readonly ILogger<BybitBboStream> _logger;

    private HashSet<string> _allowedSymbols = new(StringComparer.OrdinalIgnoreCase);

    public string ConnectorName => Connector;

    public BybitBboStream(
        BestBidAskCache cache,
        ILogger<BybitBboStream> logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public async Task StartAsync(
        IReadOnlyList<string> tradingPairs,
        CancellationToken ct)
    {
        _allowedSymbols = tradingPairs
            .Select(BybitSymbolMapper.ToBybitSymbol)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

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
                    "Bybit BBO stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _logger.LogInformation("Connecting to Bybit BBO stream: {Url}", WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation(
            "Connected to Bybit BBO stream. AllowedSymbols={Count}",
            _allowedSymbols.Count);

        await SubscribeAsync(socket, ct);

        var receiveTask = ReceiveLoopAsync(socket, ct);
        var pingTask = PingLoopAsync(socket, ct);

        await Task.WhenAny(receiveTask, pingTask);
    }

    private async Task SubscribeAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        var topics = _allowedSymbols
            .Select(symbol => $"tickers.{symbol}")
            .ToArray();

        // Bybit обычно нормально принимает массив topics,
        // но при большом universe позже можем разбить на batches.
        var payload = JsonSerializer.Serialize(new
        {
            op = "subscribe",
            args = topics
        });

        await SendTextAsync(socket, payload, ct);

        _logger.LogInformation(
            "Subscribed to Bybit ticker topics. Count={Count}",
            topics.Length);
    }

    private async Task ReceiveLoopAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        var buffer = new byte[1024 * 64];

        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            var message = await ReceiveTextAsync(socket, buffer, ct);

            if (message is null)
                break;

            ProcessMessage(message);
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

            await SendTextAsync(socket, "{\"op\":\"ping\"}", ct);
        }
    }

    private void ProcessMessage(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (root.TryGetProperty("op", out var opElement))
        {
            var op = opElement.GetString();

            var success = root.TryGetProperty("success", out var successElement) &&
                          successElement.ValueKind == JsonValueKind.True;

            var retMsg = root.TryGetProperty("ret_msg", out var retMsgElement)
                ? retMsgElement.GetString()
                : null;

            _logger.LogInformation(
                "Bybit WS control message. Op={Op}, Success={Success}, RetMsg={RetMsg}, Raw={Raw}",
                op,
                success,
                retMsg,
                json);

            return;
        }

        if (!root.TryGetProperty("topic", out var topicElement))
            return;

        var topic = topicElement.GetString();

        if (topic is null ||
            !topic.StartsWith("tickers.", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var dto = JsonSerializer.Deserialize<BybitTickerMessage>(json);

        if (dto?.Data is null)
            return;

        var symbol = dto.Data.Symbol;

        if (string.IsNullOrWhiteSpace(symbol))
            return;

        if (!_allowedSymbols.Contains(symbol))
            return;

        if (!TryParseDecimal(dto.Data.Bid1Price, out var bidPrice))
            return;

        if (!TryParseDecimal(dto.Data.Bid1Size, out var bidAmount))
            bidAmount = 0;

        if (!TryParseDecimal(dto.Data.Ask1Price, out var askPrice))
            return;

        if (!TryParseDecimal(dto.Data.Ask1Size, out var askAmount))
            askAmount = 0;

        if (bidPrice <= 0 || askPrice <= 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = dto.TimestampMs > 0
            ? DateTimeOffset.FromUnixTimeMilliseconds(dto.TimestampMs)
            : receivedAt;

        var snapshot = new BestBidAskSnapshot(
            ConnectorName: ConnectorName,
            TradingPair: BybitSymbolMapper.FromBybitSymbol(symbol),
            BestBidPrice: bidPrice,
            BestBidAmount: bidAmount,
            BestAskPrice: askPrice,
            BestAskAmount: askAmount,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _cache.Set(snapshot);
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

    private static async Task SendTextAsync(
        ClientWebSocket socket,
        string payload,
        CancellationToken ct)
    {
        var bytes = Encoding.UTF8.GetBytes(payload);

        await socket.SendAsync(
            bytes,
            WebSocketMessageType.Text,
            endOfMessage: true,
            cancellationToken: ct);
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

    private sealed class BybitTickerMessage
    {
        [JsonPropertyName("topic")]
        public string Topic { get; init; } = default!;

        [JsonPropertyName("type")]
        public string Type { get; init; } = default!;

        [JsonPropertyName("ts")]
        public long TimestampMs { get; init; }

        [JsonPropertyName("data")]
        public BybitTickerData Data { get; init; } = default!;
    }

    private sealed class BybitTickerData
    {
        [JsonPropertyName("symbol")]
        public string Symbol { get; init; } = default!;

        [JsonPropertyName("bid1Price")]
        public string? Bid1Price { get; init; }

        [JsonPropertyName("bid1Size")]
        public string? Bid1Size { get; init; }

        [JsonPropertyName("ask1Price")]
        public string? Ask1Price { get; init; }

        [JsonPropertyName("ask1Size")]
        public string? Ask1Size { get; init; }
    }
}