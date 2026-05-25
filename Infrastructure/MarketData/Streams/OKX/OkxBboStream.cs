using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Okx;

public sealed class OkxBboStream : IBestBidAskStream
{
    private const string Connector = "okx_perpetual";
    private const string WsUrl = "wss://ws.okx.com:8443/ws/v5/public";

    private readonly BestBidAskCache _cache;
    private readonly ILogger<OkxBboStream> _logger;

    private HashSet<string> _allowedInstrumentIds =
        new(StringComparer.OrdinalIgnoreCase);

    public string ConnectorName => Connector;

    public OkxBboStream(
        BestBidAskCache cache,
        ILogger<OkxBboStream> logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public async Task StartAsync(
        IReadOnlyList<string> tradingPairs,
        CancellationToken ct)
    {
        _allowedInstrumentIds = tradingPairs
            .Select(OkxSymbolMapper.ToOkxInstrumentId)
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
                    "OKX BBO stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _logger.LogInformation("Connecting to OKX BBO stream: {Url}", WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation(
            "Connected to OKX BBO stream. AllowedInstruments={Count}",
            _allowedInstrumentIds.Count);

        await SubscribeAsync(socket, ct);

        var receiveTask = ReceiveLoopAsync(socket, ct);
        var pingTask = PingLoopAsync(socket, ct);

        await Task.WhenAny(receiveTask, pingTask);
    }

    private async Task SubscribeAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        var args = _allowedInstrumentIds
            .Select(instId => new
            {
                channel = "tickers",
                instId
            })
            .ToList();

        const int batchSize = 50;

        foreach (var batch in args.Chunk(batchSize))
        {
            var payload = JsonSerializer.Serialize(new
            {
                op = "subscribe",
                args = batch
            });

            await SendTextAsync(socket, payload, ct);

            _logger.LogInformation(
                "Subscribed to OKX ticker batch. BatchSize={BatchSize}",
                batch.Length);

            await Task.Delay(100, ct);
        }

        _logger.LogInformation(
            "Subscribed to OKX ticker topics. TotalCount={Count}",
            args.Count);
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

            // OKX public WS uses plain "ping" / "pong".
            await SendTextAsync(socket, "ping", ct);
        }
    }

    private void ProcessMessage(string json)
    {
        if (json == "pong")
            return;

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (root.TryGetProperty("event", out var eventElement))
        {
            var eventName = eventElement.GetString();

            if (string.Equals(eventName, "error", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning("OKX WS error message: {Raw}", json);
            }
            else
            {
                _logger.LogInformation("OKX WS control message: {Raw}", json);
            }

            return;
        }

        var dto = JsonSerializer.Deserialize<OkxTickerMessage>(json);

        if (dto?.Data is null || dto.Data.Count == 0)
            return;

        foreach (var item in dto.Data)
        {
            ProcessTicker(item);
        }
    }

    private void ProcessTicker(OkxTickerData item)
    {
        if (string.IsNullOrWhiteSpace(item.InstrumentId))
            return;

        if (!_allowedInstrumentIds.Contains(item.InstrumentId))
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

        var exchangeTimestamp = long.TryParse(item.TimestampMs, out var ts) && ts > 0
            ? DateTimeOffset.FromUnixTimeMilliseconds(ts)
            : receivedAt;

        var snapshot = new BestBidAskSnapshot(
            ConnectorName: ConnectorName,
            TradingPair: OkxSymbolMapper.FromOkxInstrumentId(item.InstrumentId),
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

    private sealed class OkxTickerMessage
    {
        [JsonPropertyName("arg")]
        public OkxArg? Arg { get; init; }

        [JsonPropertyName("data")]
        public List<OkxTickerData> Data { get; init; } = new();
    }

    private sealed class OkxArg
    {
        [JsonPropertyName("channel")]
        public string Channel { get; init; } = default!;

        [JsonPropertyName("instId")]
        public string InstrumentId { get; init; } = default!;
    }

    private sealed class OkxTickerData
    {
        [JsonPropertyName("instId")]
        public string InstrumentId { get; init; } = default!;

        [JsonPropertyName("bidPx")]
        public string? BidPrice { get; init; }

        [JsonPropertyName("bidSz")]
        public string? BidSize { get; init; }

        [JsonPropertyName("askPx")]
        public string? AskPrice { get; init; }

        [JsonPropertyName("askSz")]
        public string? AskSize { get; init; }

        [JsonPropertyName("ts")]
        public string? TimestampMs { get; init; }
    }
}