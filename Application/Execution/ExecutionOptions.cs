namespace Arbitrage.Api.Application.Execution;

public sealed class ExecutionOptions
{
    public const string SectionName = "Execution";

    public ExecutionMode Mode { get; init; } = ExecutionMode.Disabled;

    public decimal MaxLiveNotionalUsd { get; init; } = 10m;

    public int MaxAttemptsPerArm { get; init; } = 1;

    public int ArmExpiresAfterSeconds { get; init; } = 600;

    public int MaxLegDelayMs { get; init; } = 1500;

    public decimal MaxAllowedSlippagePct { get; init; } = 0.15m;

    public bool KillSwitchEnabledOnStartup { get; init; } = true;

    public bool RequireOneShotArm { get; init; } = true;

    public string[] EnabledConnectors { get; init; } = [];
}