namespace Arbitrage.Api.Application.MarketData.Depth;

public sealed record DepthSubscriptionKey(
    string ConnectorName,
    string TradingPair);