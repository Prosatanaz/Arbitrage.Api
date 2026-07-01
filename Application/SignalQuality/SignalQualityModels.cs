namespace Arbitrage.Api.Application.SignalQuality;

public sealed record SignalQualityInput(
    string TradingPair,
    string LongConnector,
    string ShortConnector,
    decimal? NetEdgePct,
    decimal? BuySlippagePct,
    decimal? SellSlippagePct,
    int? EpisodeSamples,
    TimeSpan? EpisodeDuration);

public sealed record SignalQualityResult(
    SignalDecision Decision,
    string Reason);