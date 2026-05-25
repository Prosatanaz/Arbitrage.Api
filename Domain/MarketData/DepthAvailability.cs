namespace Arbitrage.Api.Domain.MarketData;

public sealed record DepthAvailability(
    TradeSide Side,
    decimal MaxBaseAmount,
    decimal QuoteAmount,
    decimal BestPrice,
    decimal PriceLimit,
    decimal MaxSlippagePct,
    int LevelsUsed);