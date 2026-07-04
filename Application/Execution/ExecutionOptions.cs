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

    public decimal MinEntryNetEdgePct { get; init; } = 0.2m;

    public decimal ExitNetEdgePct { get; init; } = 0.05m;

    public int MaxHoldMinutes { get; init; } = 720;

    public int EntryPollIntervalMs { get; init; } = 1000;

    public int ExitPollIntervalMs { get; init; } = 1000;

    public int ExitConfirmSamples { get; init; } = 3;
}