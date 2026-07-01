using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Binance;

public sealed class BinanceBboStream : BboStreamBase
{
    private const string Connector = "binance_perpetual";

    private readonly BestBidAskCache _cache;

    public BinanceBboStream(
        BestBidAskCache cache,
        ILogger<BinanceBboStream> logger)
        : base(logger)
    {
        _cache = cache;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://fstream.binance.com/ws/!bookTicker";

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        BinanceSymbolMapper.ToBinanceSymbol(tradingPair);

    protected override Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbols,
        CancellationToken ct)
    {
        // Binance's !bookTicker stream broadcasts every symbol; no explicit
        // subscribe message is needed, incoming ticks are just filtered by symbol.
        return Task.CompletedTask;
    }

    protected override Task ProcessMessageAsync(
        ClientWebSocket socket,
        string json,
        CancellationToken ct)
    {
        ProcessMessage(json);
        return Task.CompletedTask;
    }

    internal void ProcessMessage(string json)
    {
        var dto = JsonSerializer.Deserialize<BinanceBookTickerMessage>(json);

        if (dto is null)
            return;

        if (!AllowedSymbols.Contains(dto.Symbol))
            return;

        if (!decimal.TryParse(dto.BestBidPrice, NumberStyles.Number, CultureInfo.InvariantCulture, out var bidPrice))
            return;

        if (!decimal.TryParse(dto.BestBidQuantity, NumberStyles.Number, CultureInfo.InvariantCulture, out var bidAmount))
            return;

        if (!decimal.TryParse(dto.BestAskPrice, NumberStyles.Number, CultureInfo.InvariantCulture, out var askPrice))
            return;

        if (!decimal.TryParse(dto.BestAskQuantity, NumberStyles.Number, CultureInfo.InvariantCulture, out var askAmount))
            return;

        if (bidPrice <= 0 || askPrice <= 0)
            return;

        var now = DateTimeOffset.UtcNow;

        var snapshot = new BestBidAskSnapshot(
            ConnectorName: ConnectorName,
            TradingPair: BinanceSymbolMapper.FromBinanceSymbol(dto.Symbol),
            BestBidPrice: bidPrice,
            BestBidAmount: bidAmount,
            BestAskPrice: askPrice,
            BestAskAmount: askAmount,
            ExchangeTimestamp: now,
            ReceivedAt: now);

        _cache.Set(snapshot);
    }

    private sealed class BinanceBookTickerMessage
    {
        [JsonPropertyName("s")]
        public string Symbol { get; init; } = default!;

        [JsonPropertyName("b")]
        public string BestBidPrice { get; init; } = default!;

        [JsonPropertyName("B")]
        public string BestBidQuantity { get; init; } = default!;

        [JsonPropertyName("a")]
        public string BestAskPrice { get; init; } = default!;

        [JsonPropertyName("A")]
        public string BestAskQuantity { get; init; } = default!;
    }
}
