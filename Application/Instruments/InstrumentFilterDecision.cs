namespace Arbitrage.Api.Application.Instruments;

public enum InstrumentFilterDecision
{
    Allowed = 1,
    Blocked = 2
}

public sealed record InstrumentFilterResult(
    InstrumentFilterDecision Decision,
    string Reason,
    string TradingPair,
    string BaseAsset,
    string QuoteAsset);