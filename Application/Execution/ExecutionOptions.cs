namespace Arbitrage.Api.Application.Execution;

public sealed class ExecutionOptions
{
    public const string SectionName = "Execution";

    public ExecutionMode Mode { get; init; } = ExecutionMode.Disabled;

    public decimal MaxLiveNotionalUsd { get; init; } = 10m;

    public int MaxAttemptsPerArm { get; init; } = 1;

    public int ArmExpiresAfterSeconds { get; init; } = 600;

    // Per-leg budget covering BOTH order placement AND fill confirmation (the leg executor wraps
    // the whole PlaceOrderAsync in this timeout). It must exceed a connector's worst-case
    // place + fill-poll round-trip chain, or a filled order gets canceled mid-confirmation and
    // reported as "failed to fill" (HTX's poll loop alone can need >1.5s). This is a
    // confirmation-read budget, not a market-exposure bound - the IOC legs are terminal on the
    // exchange almost immediately, so the naked-exposure window is far shorter than this value.
    public int MaxLegDelayMs { get; init; } = 3500;

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