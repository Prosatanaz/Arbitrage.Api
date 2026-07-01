namespace Arbitrage.Api.Application.Execution;

public enum ExecutionRuntimeStatus
{
    Disabled = 0,
    Armed = 1,
    Executing = 2,
    DisarmedAfterAttempt = 3,
    LockedByError = 4,
    KillSwitch = 5
}