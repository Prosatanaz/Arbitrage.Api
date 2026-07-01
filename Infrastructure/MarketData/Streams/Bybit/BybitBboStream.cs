using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Bybit;

public sealed class BybitBboStream : BboStreamBase
{
    private const string Connector = "bybit_perpetual";

    private readonly BestBidAskCache _cache;
    private readonly ILogger<BybitBboStream> _logger;

    public BybitBboStream(
        BestBidAskCache cache,
        ILogger<BybitBboStream> logger)
        : base(logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://stream.bybit.com/v5/public/linear";

    protected override int ReceiveBufferSize => 1024 * 64;

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        BybitSymbolMapper.ToBybitSymbol(tradingPair);

    protected override async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbols,
        CancellationToken ct)
    {
        var topics = symbols
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

            await SendTextAsync(socket, "{\"op\":\"ping\"}", ct);
        }
    }

    protected override Task ProcessMessageAsync(
        ClientWebSocket socket,
        string json,
        CancellationToken ct)
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

            return Task.CompletedTask;
        }

        if (!root.TryGetProperty("topic", out var topicElement))
            return Task.CompletedTask;

        var topic = topicElement.GetString();

        if (topic is null ||
            !topic.StartsWith("tickers.", StringComparison.OrdinalIgnoreCase))
        {
            return Task.CompletedTask;
        }

        var dto = JsonSerializer.Deserialize<BybitTickerMessage>(json);

        if (dto?.Data is null)
            return Task.CompletedTask;

        var symbol = dto.Data.Symbol;

        if (string.IsNullOrWhiteSpace(symbol))
            return Task.CompletedTask;

        if (!AllowedSymbols.Contains(symbol))
            return Task.CompletedTask;

        if (!TryParseDecimal(dto.Data.Bid1Price, out var bidPrice))
            return Task.CompletedTask;

        if (!TryParseDecimal(dto.Data.Bid1Size, out var bidAmount))
            bidAmount = 0;

        if (!TryParseDecimal(dto.Data.Ask1Price, out var askPrice))
            return Task.CompletedTask;

        if (!TryParseDecimal(dto.Data.Ask1Size, out var askAmount))
            askAmount = 0;

        if (bidPrice <= 0 || askPrice <= 0)
            return Task.CompletedTask;

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

        return Task.CompletedTask;
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
