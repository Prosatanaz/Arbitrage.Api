using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Okx;

public sealed class OkxBboStream : BboStreamBase
{
    private const string Connector = "okx_perpetual";

    private readonly BestBidAskCache _cache;
    private readonly ILogger<OkxBboStream> _logger;

    public OkxBboStream(
        BestBidAskCache cache,
        ILogger<OkxBboStream> logger)
        : base(logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://ws.okx.com:8443/ws/v5/public";

    protected override int ReceiveBufferSize => 1024 * 64;

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        OkxSymbolMapper.ToOkxInstrumentId(tradingPair);

    protected override async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbols,
        CancellationToken ct)
    {
        var args = symbols
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

    protected override Task? RunPingLoopAsync(ClientWebSocket socket, CancellationToken ct) =>
        PingLoopAsync(socket, ct);

    private async Task PingLoopAsync(ClientWebSocket socket, CancellationToken ct)
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

    protected override Task ProcessMessageAsync(
        ClientWebSocket socket,
        string json,
        CancellationToken ct)
    {
        if (json == "pong")
            return Task.CompletedTask;

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

            return Task.CompletedTask;
        }

        var dto = JsonSerializer.Deserialize<OkxTickerMessage>(json);

        if (dto?.Data is null || dto.Data.Count == 0)
            return Task.CompletedTask;

        foreach (var item in dto.Data)
        {
            ProcessTicker(item);
        }

        return Task.CompletedTask;
    }

    private void ProcessTicker(OkxTickerData item)
    {
        if (string.IsNullOrWhiteSpace(item.InstrumentId))
            return;

        if (!AllowedSymbols.Contains(item.InstrumentId))
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
