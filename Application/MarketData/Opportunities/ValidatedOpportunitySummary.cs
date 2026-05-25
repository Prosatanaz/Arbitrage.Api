namespace Arbitrage.Api.Application.MarketData.Opportunities;

public sealed record ValidatedOpportunitySummary(
    string TradingPair,
    string Direction,
    string LongConnector,
    string ShortConnector,
    decimal NotionalUsd,
    decimal BuyAveragePrice,
    decimal SellAveragePrice,
    decimal GrossSpreadPct,
    decimal EstimatedFeesPct,
    decimal NetEdgePct,
    decimal BuySlippagePct,
    decimal SellSlippagePct,
    int BuyLevelsUsed,
    int SellLevelsUsed,
    decimal RequestedBaseAmount,
    decimal BuyQuoteAmount,
    decimal SellQuoteAmount,
    decimal EstimatedProfitUsd,
    DateTimeOffset DetectedAt,
    DateTimeOffset? ValidatedAt);