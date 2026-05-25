namespace Arbitrage.Api.Domain.MarketData;

public sealed record OrderBookSnapshot(
    string ConnectorName,
    string TradingPair,
    IReadOnlyList<OrderBookLevel> Bids,
    IReadOnlyList<OrderBookLevel> Asks,
    DateTimeOffset ExchangeTimestamp,
    DateTimeOffset ReceivedAt)
{
    public OrderBookLevel? BestBid => Bids.Count > 0 ? Bids[0] : null;

    public OrderBookLevel? BestAsk => Asks.Count > 0 ? Asks[0] : null;

    public decimal? BestBidPrice => BestBid?.Price;

    public decimal? BestAskPrice => BestAsk?.Price;

    public decimal? Spread =>
        BestBidPrice is not null && BestAskPrice is not null
            ? BestAskPrice.Value - BestBidPrice.Value
            : null;

    public decimal? SpreadPct =>
        BestBidPrice is not null && BestAskPrice is not null && BestAskPrice.Value > 0
            ? (BestAskPrice.Value - BestBidPrice.Value) / BestAskPrice.Value * 100m
            : null;
}