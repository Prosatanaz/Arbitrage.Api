namespace Arbitrage.Api.Domain.MarketData;

public sealed record OrderBookLevel(
    decimal Price,
    decimal Amount);