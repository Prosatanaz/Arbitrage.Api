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
