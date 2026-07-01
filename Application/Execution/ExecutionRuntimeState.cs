namespace Arbitrage.Api.Application.Execution;

public sealed record ExecutionRuntimeState(
    ExecutionRuntimeStatus RuntimeStatus,
    bool KillSwitchEnabled,
    int RemainingAttempts,
    decimal? MaxNotionalUsd,
    DateTimeOffset? ArmedUntil,
    Guid? LastAttemptId,
    string? LastStatusReason,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);