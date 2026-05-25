namespace Arbitrage.Api.Domain.MarketData;

public sealed record BestBidAskKey(
    string ConnectorName,
    string TradingPair);