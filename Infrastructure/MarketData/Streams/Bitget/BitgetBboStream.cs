using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Bitget;

public sealed class BitgetBboStream : BboStreamBase
{
    private const string Connector = "bitget_perpetual";
    private const string InstType = "USDT-FUTURES";
    private const string Channel = "ticker";

    private readonly BestBidAskCache _cache;
    private readonly ILogger<BitgetBboStream> _logger;

    public BitgetBboStream(
        BestBidAskCache cache,
        ILogger<BitgetBboStream> logger)
        : base(logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://ws.bitget.com/v2/ws/public";

    protected override int ReceiveBufferSize => 1024 * 256;

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        BitgetSymbolMapper.ToBitgetSymbol(tradingPair);

    protected override async Task SubscribeAsync(
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

            _logger.LogInformation(
                "Bitget BBO subscribe requested. BatchSize={BatchSize}, Total={Total}",
                batch.Length,
                AllowedSymbols.Count);

            await Task.Delay(1000, ct);
        }
    }

    protected override Task? RunPingLoopAsync(ClientWebSocket socket, CancellationToken ct) =>
        PingLoopAsync(socket, ct);

    private async Task PingLoopAsync(ClientWebSocket socket, CancellationToken ct)
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
                "Failed to parse Bitget BBO message. Raw={Raw}",
                json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to process Bitget BBO message. Raw={Raw}",
                json);
        }

        await Task.CompletedTask;
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

        if (!AllowedSymbols.Contains(symbol))
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
