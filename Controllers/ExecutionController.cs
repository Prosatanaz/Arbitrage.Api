using Arbitrage.Api.Application.Execution;
using Arbitrage.Api.Application.Execution.CarryTrades;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Controllers;

[ApiController]
[Route("api/execution")]
public sealed class ExecutionController : ControllerBase
{
    private readonly ExecutionOptions _options;
    private readonly ExecutionGateService _executionGate;
    private readonly ICarryTradeRepository _carryTradeRepository;

    public ExecutionController(
        IOptions<ExecutionOptions> options,
        ExecutionGateService executionGate,
        ICarryTradeRepository carryTradeRepository)
    {
        _options = options.Value;
        _executionGate = executionGate;
        _carryTradeRepository = carryTradeRepository;
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

    [HttpGet("trades")]
    public async Task<IReadOnlyList<CarryTrade>> Trades(
        [FromQuery] int hours = 168,
        [FromQuery] int limit = 100,
        CancellationToken ct = default)
    {
        var safeHours = Math.Clamp(hours, 1, 24 * 30);
        var safeLimit = Math.Clamp(limit, 1, 500);

        return await _carryTradeRepository.ListRecentAsync(
            safeHours,
            safeLimit,
            ct);
    }

    /// <summary>
    /// Full audit view of a single trade: the aggregated row plus every per-leg exchange order
    /// and every logged decision/action event. Read-only.
    /// </summary>
    [HttpGet("trades/{id:guid}/detail")]
    public async Task<IActionResult> TradeDetail(
        Guid id,
        CancellationToken ct)
    {
        var trade = await _carryTradeRepository.GetByIdAsync(id, ct);

        if (trade is null)
            return NotFound(new { Error = "Carry trade not found." });

        var legs = await _carryTradeRepository.ListLegsAsync(id, ct);
        var events = await _carryTradeRepository.ListEventsAsync(id, ct);

        return Ok(new
        {
            trade,
            legs,
            events
        });
    }

    [HttpPost("trades/{id:guid}/close")]
    public async Task<IActionResult> CloseTrade(
        Guid id,
        CancellationToken ct)
    {
        var trade = await _carryTradeRepository.GetByIdAsync(id, ct);

        if (trade is null)
            return NotFound(new { Error = "Carry trade not found." });

        if (trade.Status != CarryTradeStatus.Open)
        {
            return BadRequest(new
            {
                Error = $"Carry trade is not open (status={trade.Status}); cannot request a manual close."
            });
        }

        var flagged = await _carryTradeRepository.MarkClosingAsync(id, ct);

        if (!flagged)
        {
            return BadRequest(new
            {
                Error = "Carry trade could not be flagged for closing (it may have changed status concurrently)."
            });
        }

        await _carryTradeRepository.RecordEventAsync(
            new RecordCarryTradeEventRequest(
                TradeId: id,
                AttemptId: trade.AttemptId,
                EventType: CarryTradeEventTypes.CloseRequested,
                Reason: "Manual close requested via API; monitor will close on next tick.",
                Details: new { trade.TradingPair, trade.LongConnector, trade.ShortConnector }),
            ct);

        return Ok(new
        {
            Id = id,
            Status = "Closing",
            Message = "Close requested; the position monitor will close it on its next tick."
        });
    }
}