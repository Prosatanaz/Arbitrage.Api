namespace Arbitrage.Api.Application.Execution.CarryTrades;

public enum CarryTradeStatus
{
    Open,
    Closing,
    Closed,
    Failed
}

public sealed record CarryTrade(
    Guid Id,
    Guid AttemptId,
    string TradingPair,
    string LongConnector,
    string ShortConnector,
    decimal NotionalUsd,
    decimal BaseQuantity,
    decimal EntryLongPrice,
    decimal EntryShortPrice,
    decimal EntryFeesUsd,
    decimal EntryNetEdgePct,
    DateTimeOffset OpenedAt,
    CarryTradeStatus Status,
    decimal? ExitLongPrice,
    decimal? ExitShortPrice,
    decimal? ExitFeesUsd,
    string? CloseReason,
    DateTimeOffset? ClosedAt,
    decimal? RealizedPnlUsd,
    string? Error);

public sealed record OpenCarryTradeRequest(
    Guid AttemptId,
    string TradingPair,
    string LongConnector,
    string ShortConnector,
    decimal NotionalUsd,
    decimal BaseQuantity,
    decimal EntryLongPrice,
    decimal EntryShortPrice,
    decimal EntryFeesUsd,
    decimal EntryNetEdgePct);

public sealed record CloseCarryTradeRequest(
    Guid Id,
    decimal ExitLongPrice,
    decimal ExitShortPrice,
    decimal ExitFeesUsd,
    decimal RealizedPnlUsd,
    string CloseReason);
