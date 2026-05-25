namespace Arbitrage.Api.Domain.MarketData;

public sealed record BestBidAskSnapshot(
    string ConnectorName,
    string TradingPair,
    decimal BestBidPrice,
    decimal BestBidAmount,
    decimal BestAskPrice,
    decimal BestAskAmount,
    DateTimeOffset ExchangeTimestamp,
    DateTimeOffset ReceivedAt)
{
    public decimal Spread =>
        BestAskPrice - BestBidPrice;

    public decimal SpreadPct =>
        BestBidPrice > 0
            ? Spread / BestBidPrice * 100m
            : 0m;
}