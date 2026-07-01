namespace Arbitrage.Api.Application.Execution;

public sealed class ArmOnceRequest
{
    public decimal MaxNotionalUsd { get; init; } = 10m;

    public int? ExpiresAfterSeconds { get; init; }
}

public sealed class TryAcquireExecutionAttemptRequest
{
    public string TradingPair { get; init; } = "";

    public string LongConnector { get; init; } = "";

    public string ShortConnector { get; init; } = "";

    public decimal NotionalUsd { get; init; }
}

public sealed class FinishExecutionAttemptRequest
{
    public Guid AttemptId { get; init; }

    public bool Succeeded { get; init; }

    public string Reason { get; init; } = "";
}

public sealed record ExecutionGateResult(
    bool IsAllowed,
    string Reason,
    Guid? AttemptId,
    ExecutionRuntimeState? State);