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
}
