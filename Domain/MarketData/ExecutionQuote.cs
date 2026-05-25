namespace Arbitrage.Api.Domain.MarketData;

public sealed record ExecutionQuote(
    TradeSide Side,
    decimal RequestedBaseAmount,
    decimal FilledBaseAmount,
    decimal AveragePrice,
    decimal BestPrice,
    decimal WorstPrice,
    decimal QuoteAmount,
    decimal SlippagePct,
    int LevelsUsed,
    bool IsFullyFillable);