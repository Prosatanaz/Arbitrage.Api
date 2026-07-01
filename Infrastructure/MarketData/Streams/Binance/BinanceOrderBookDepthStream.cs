using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Binance;

public sealed class BinanceOrderBookDepthStream : DepthStreamBase
{
    private const string Connector = "binance_perpetual";

    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger<BinanceOrderBookDepthStream> _logger;

    public BinanceOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger<BinanceOrderBookDepthStream> logger)
        : base(targetStore, depthCache, logger)
    {
        _depthCache = depthCache;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://fstream.binance.com/ws";

    protected override string MapTradingPairToExchangeSymbol(string tradingPair)
    {
        // BTC-USDT -> btcusdt@depth20@100ms
        var symbol = BinanceSymbolMapper
            .ToBinanceSymbol(tradingPair)
            .ToLowerInvariant();

        return $"{symbol}@depth20@100ms";
    }

    protected override string MapExchangeSymbolToTradingPair(string exchangeSymbol)
    {
        // btcusdt@depth20@100ms
        var symbol = exchangeSymbol.Split('@', 2)[0];

        return BinanceSymbolMapper.FromBinanceSymbol(symbol);
    }

    protected override async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbolsToAdd,
        CancellationToken ct)
    {
        await SendSubscriptionAsync(socket, "SUBSCRIBE", symbolsToAdd, ct);

        _logger.LogInformation(
            "Binance depth subscribed. Count={Count}, Total={Total}",
            symbolsToAdd.Count,
            SubscribedSymbolCount);
    }

    protected override async Task UnsubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbolsToRemove,
        CancellationToken ct)
    {
        await SendSubscriptionAsync(socket, "UNSUBSCRIBE", symbolsToRemove, ct);

        _logger.LogInformation(
            "Binance depth unsubscribed. Count={Count}, Total={Total}",
            symbolsToRemove.Count,
            SubscribedSymbolCount);
    }

    private async Task SendSubscriptionAsync(
        ClientWebSocket socket,
        string method,
        IReadOnlyList<string> streams,
        CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(new
        {
            method,
            @params = streams,
            id = Environment.TickCount
        });

        await SendTextAsync(socket, payload, ct);
    }

    protected override Task ProcessMessageAsync(
        ClientWebSocket socket,
        string json,
        CancellationToken ct)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        // Binance subscription ack:
        // {"result":null,"id":1}
        if (root.TryGetProperty("result", out _))
            return Task.CompletedTask;

        var dto = JsonSerializer.Deserialize<BinancePartialDepthMessage>(json);

        if (dto is null || string.IsNullOrWhiteSpace(dto.EventType))
            return Task.CompletedTask;

        var tradingPair = BinanceSymbolMapper.FromBinanceSymbol(dto.Symbol);

        var bids = dto.Bids
            .Select(ParseLevel)
            .Where(x => x.Price > 0 && x.Amount > 0)
            .OrderByDescending(x => x.Price)
            .ToList();

        var asks = dto.Asks
            .Select(ParseLevel)
            .Where(x => x.Price > 0 && x.Amount > 0)
            .OrderBy(x => x.Price)
            .ToList();

        if (bids.Count == 0 || asks.Count == 0)
            return Task.CompletedTask;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = dto.EventTime > 0
            ? DateTimeOffset.FromUnixTimeMilliseconds(dto.EventTime)
            : receivedAt;

        _depthCache.Set(new OrderBookDepthSnapshot(
            ConnectorName: Connector,
            TradingPair: tradingPair,
            Bids: bids,
            Asks: asks,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt));

        return Task.CompletedTask;
    }

    private static OrderBookDepthLevel ParseLevel(string[] values)
    {
        if (values.Length < 2)
            return new OrderBookDepthLevel(0, 0);

        var priceOk = decimal.TryParse(
            values[0],
            NumberStyles.Number,
            CultureInfo.InvariantCulture,
            out var price);

        var amountOk = decimal.TryParse(
            values[1],
            NumberStyles.Number,
            CultureInfo.InvariantCulture,
            out var amount);

        return priceOk && amountOk
            ? new OrderBookDepthLevel(price, amount)
            : new OrderBookDepthLevel(0, 0);
    }

    private sealed class BinancePartialDepthMessage
    {
        [JsonPropertyName("e")]
        public string EventType { get; init; } = default!;

        [JsonPropertyName("E")]
        public long EventTime { get; init; }

        [JsonPropertyName("s")]
        public string Symbol { get; init; } = default!;

        [JsonPropertyName("b")]
        public List<string[]> Bids { get; init; } = new();

        [JsonPropertyName("a")]
        public List<string[]> Asks { get; init; } = new();
    }
}
