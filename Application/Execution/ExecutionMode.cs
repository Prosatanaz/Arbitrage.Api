namespace Arbitrage.Api.Application.Execution;

public enum ExecutionMode
{
    Disabled = 0,
    ReadOnly = 1,
    ArmedOneShot = 2,
    AutoLive = 3
}