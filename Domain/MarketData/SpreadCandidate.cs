namespace Arbitrage.Api.Domain.MarketData;

public sealed record SpreadCandidate(
    string TradingPair,
    string LongConnector,
    string ShortConnector,
    decimal BuyPrice,
    decimal SellPrice,
    decimal GrossSpread,
    decimal GrossSpreadPct,
    DateTimeOffset DetectedAt);