using Arbitrage.Api.Application.Execution;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Controllers;

[ApiController]
[Route("api/execution")]
public sealed class ExecutionController : ControllerBase
{
    private readonly ExecutionOptions _options;
    private readonly ExecutionGateService _executionGate;

    public ExecutionController(
        IOptions<ExecutionOptions> options,
        ExecutionGateService executionGate)
    {
        _options = options.Value;
        _executionGate = executionGate;
    }

    [HttpGet("config")]
    public object Config()
    {
        return new
        {
            _options.Mode,
            _options.MaxLiveNotionalUsd,
            _options.MaxAttemptsPerArm,
            _options.ArmExpiresAfterSeconds,
            _options.MaxLegDelayMs,
            _options.MaxAllowedSlippagePct,
            _options.KillSwitchEnabledOnStartup,
            _options.RequireOneShotArm,
            _options.EnabledConnectors
        };
    }

    [HttpGet("state")]
    public async Task<object> State(CancellationToken ct)
    {
        var state = await _executionGate.GetStateAsync(ct);

        return new
        {
            _options.Mode,
            state.RuntimeStatus,
            state.KillSwitchEnabled,
            state.RemainingAttempts,
            state.MaxNotionalUsd,
            state.ArmedUntil,
            state.LastAttemptId,
            state.LastStatusReason,
            state.CreatedAt,
            state.UpdatedAt
        };
    }

    [HttpPost("arm-once")]
    public async Task<IActionResult> ArmOnce(
        [FromBody] ArmOnceRequest request,
        CancellationToken ct)
    {
        var result = await _executionGate.ArmOnceAsync(
            request,
            ct);

        return result.IsAllowed
            ? Ok(result)
            : BadRequest(result);
    }

    [HttpPost("disarm")]
    public async Task<object> Disarm(CancellationToken ct)
    {
        return await _executionGate.DisarmAsync(ct);
    }

    [HttpPost("kill-switch/enable")]
    public async Task<object> EnableKillSwitch(CancellationToken ct)
    {
        return await _executionGate.SetKillSwitchAsync(
            enabled: true,
            ct);
    }

    [HttpPost("kill-switch/disable")]
    public async Task<object> DisableKillSwitch(CancellationToken ct)
    {
        return await _executionGate.SetKillSwitchAsync(
            enabled: false,
            ct);
    }

    [HttpPost("try-acquire-attempt")]
    public async Task<IActionResult> TryAcquireAttempt(
        [FromBody] TryAcquireExecutionAttemptRequest request,
        CancellationToken ct)
    {
        var result = await _executionGate.TryAcquireAttemptAsync(
            request,
            ct);

        return result.IsAllowed
            ? Ok(result)
            : BadRequest(result);
    }

    [HttpPost("attempts/finish")]
    public async Task<object> FinishAttempt(
        [FromBody] FinishExecutionAttemptRequest request,
        CancellationToken ct)
    {
        return await _executionGate.FinishAttemptAsync(
            request,
            ct);
    }
}