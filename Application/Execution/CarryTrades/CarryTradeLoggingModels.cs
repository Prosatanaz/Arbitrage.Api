namespace Arbitrage.Api.Application.Execution.CarryTrades;

/// <summary>
/// Which stage of a carry trade produced a given exchange order. Entry = opening both legs,
/// Exit = closing both legs, Unwind = a corrective order the leg executor fired to trim a size
/// mismatch or roll back a partially-filled entry.
/// </summary>
public enum CarryTradeLegPhase
{
    Entry,
    Exit,
    Unwind
}

/// <summary>
/// One real exchange order that was part of a carry trade. This is the per-leg, per-exchange
/// detail (fees, prices, order id, fill status) that the aggregated <see cref="CarryTrade"/> row
/// collapses away. Append-only - a leg row is never mutated once written.
/// </summary>
public sealed record CarryTradeLeg(
    long Id,
    Guid TradeId,
    string Phase,
    string Connector,
    string Role,
    string Side,
    bool ReduceOnly,
    decimal RequestedQuantity,
    decimal FilledQuantity,
    decimal AverageFillPrice,
    decimal FeePaidUsd,
    string ExchangeOrderId,
    string ClientOrderId,
    string Status,
    DateTimeOffset FilledAt,
    DateTimeOffset CreatedAt);

public sealed record RecordCarryTradeLegRequest(
    Guid TradeId,
    CarryTradeLegPhase Phase,
    string Connector,
    string Role,
    string Side,
    bool ReduceOnly,
    decimal RequestedQuantity,
    decimal FilledQuantity,
    decimal AverageFillPrice,
    decimal FeePaidUsd,
    string ExchangeOrderId,
    string ClientOrderId,
    string Status,
    DateTimeOffset FilledAt);

/// <summary>
/// Canonical event-type tags for the carry-trade action log. Kept as string constants (not an
/// enum column) so historical rows never break if new event types are added later.
/// </summary>
public static class CarryTradeEventTypes
{
    public const string EntrySignal = "EntrySignal";
    public const string EntryFilled = "EntryFilled";
    public const string EntryFailed = "EntryFailed";
    public const string ExitSignal = "ExitSignal";
    public const string ExitFilled = "ExitFilled";
    public const string ExitFailed = "ExitFailed";
    public const string CloseRequested = "CloseRequested";
    public const string ManualInterventionRequired = "ManualInterventionRequired";

    /// <summary>
    /// A close whose orders did not fill, but a live position read confirmed both legs are already
    /// flat - so the trade was marked Closed by reconciliation rather than looping/Failed.
    /// </summary>
    public const string CloseReconciled = "CloseReconciled";
}

/// <summary>
/// One entry in the append-only carry-trade action log: what happened, why, and the full numeric
/// basis of the decision (in <see cref="DetailsJson"/>). This is the durable "on what basis was
/// each decision made" record. Never updated or deleted.
/// </summary>
public sealed record CarryTradeEvent(
    long Id,
    Guid? TradeId,
    Guid? AttemptId,
    string EventType,
    string Reason,
    string? DetailsJson,
    DateTimeOffset CreatedAt);

public sealed record RecordCarryTradeEventRequest(
    Guid? TradeId,
    Guid? AttemptId,
    string EventType,
    string Reason,
    object? Details);
