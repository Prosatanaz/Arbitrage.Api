namespace Arbitrage.Api.Domain.MarketData;

public sealed record OrderBookDepthLevel(
    decimal Price,
    decimal Amount);