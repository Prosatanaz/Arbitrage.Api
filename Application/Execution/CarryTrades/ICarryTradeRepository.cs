namespace Arbitrage.Api.Application.Execution.CarryTrades;

public interface ICarryTradeRepository
{
    Task<CarryTrade> InsertOpenAsync(
        OpenCarryTradeRequest request,
        CancellationToken ct);

    Task<CarryTrade?> GetOpenAsync(CancellationToken ct);

    Task<CarryTrade?> GetByIdAsync(
        Guid id,
        CancellationToken ct);

    Task<bool> MarkClosingAsync(
        Guid id,
        CancellationToken ct);

    Task<CarryTrade> MarkClosedAsync(
        CloseCarryTradeRequest request,
        CancellationToken ct);

    Task<CarryTrade> MarkFailedAsync(
        Guid id,
        string error,
        CancellationToken ct);

    /// <summary>
    /// Marks an Open/Closing trade Closed when a live position read confirmed both legs are already
    /// flat even though the close orders did not fill. Exit prices are left null (no reliable exit
    /// fill was captured); realized PnL/fees are recorded only if partial fills were captured, else
    /// null. Returns false if the trade was no longer Open/Closing.
    /// </summary>
    Task<bool> MarkReconciledClosedAsync(
        Guid id,
        string closeReason,
        decimal? realizedPnlUsd,
        decimal? exitFeesUsd,
        decimal? exitNetEdgePct,
        CancellationToken ct);

    Task<IReadOnlyList<CarryTrade>> ListRecentAsync(
        int hours,
        int limit,
        CancellationToken ct);

    /// <summary>Persists one exchange-order leg. Append-only, never mutated.</summary>
    Task RecordLegAsync(
        RecordCarryTradeLegRequest request,
        CancellationToken ct);

    /// <summary>Appends one decision/action entry to the durable action log.</summary>
    Task RecordEventAsync(
        RecordCarryTradeEventRequest request,
        CancellationToken ct);

    Task<IReadOnlyList<CarryTradeLeg>> ListLegsAsync(
        Guid tradeId,
        CancellationToken ct);

    Task<IReadOnlyList<CarryTradeEvent>> ListEventsAsync(
        Guid tradeId,
        CancellationToken ct);
}
