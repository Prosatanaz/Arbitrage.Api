namespace Arbitrage.Api.Domain.MarketData;

public sealed record OrderBookDepthSnapshot(
    string ConnectorName,
    string TradingPair,
    IReadOnlyList<OrderBookDepthLevel> Bids,
    IReadOnlyList<OrderBookDepthLevel> Asks,
    DateTimeOffset ExchangeTimestamp,
    DateTimeOffset ReceivedAt)
{
    public decimal BestBidPrice => Bids.Count > 0 ? Bids[0].Price : 0m;

    public decimal BestAskPrice => Asks.Count > 0 ? Asks[0].Price : 0m;
}