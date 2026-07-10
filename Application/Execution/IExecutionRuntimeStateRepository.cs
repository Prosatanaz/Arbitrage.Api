namespace Arbitrage.Api.Application.Execution;

public interface IExecutionRuntimeStateRepository
{
    Task<ExecutionRuntimeState> GetAsync(CancellationToken ct);

    Task<ExecutionRuntimeState> ArmOnceAsync(
        decimal maxNotionalUsd,
        DateTimeOffset armedUntil,
        string reason,
        CancellationToken ct);

    Task<ExecutionRuntimeState> DisarmAsync(
        string reason,
        CancellationToken ct);

    Task<ExecutionRuntimeState> SetKillSwitchAsync(
        bool enabled,
        string reason,
        CancellationToken ct);

    Task<ExecutionRuntimeState?> TryAcquireAttemptAsync(
        Guid attemptId,
        decimal notionalUsd,
        string tradingPair,
        string longConnector,
        string shortConnector,
        string reason,
        CancellationToken ct);

    Task<ExecutionRuntimeState> FinishAttemptAsync(
        Guid attemptId,
        bool succeeded,
        string reason,
        CancellationToken ct);

    Task<ExecutionRuntimeState> ResetUnsafeStateOnStartupAsync(
        bool killSwitchEnabled,
        string reason,
        CancellationToken ct);
}