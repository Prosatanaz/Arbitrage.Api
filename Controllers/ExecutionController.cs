using Arbitrage.Api.Application.Execution;
using Arbitrage.Api.Application.Execution.CarryTrades;
using Arbitrage.Api.Application.MarketData.Opportunities;
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
    private readonly CarryTradeEntryService _entryService;
    private readonly LatestValidatedOpportunityStore _opportunityStore;

    public ExecutionController(
        IOptions<ExecutionOptions> options,
        ExecutionGateService executionGate,
        ICarryTradeRepository carryTradeRepository,
        CarryTradeEntryService entryService,
        LatestValidatedOpportunityStore opportunityStore)
    {
        _options = options.Value;
        _executionGate = executionGate;
        _carryTradeRepository = carryTradeRepository;
        _entryService = entryService;
        _opportunityStore = opportunityStore;
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
            state.ManualTradingEnabled,
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

    [HttpPost("manual/enable")]
    public async Task<object> EnableManualTrading(CancellationToken ct)
    {
        return await _executionGate.SetManualTradingAsync(enabled: true, ct);
    }

    [HttpPost("manual/disable")]
    public async Task<object> DisableManualTrading(CancellationToken ct)
    {
        return await _executionGate.SetManualTradingAsync(enabled: false, ct);
    }

    /// <summary>
    /// The candidate a manual open would act on right now: the highest-net-edge validated
    /// opportunity on enabled connectors, or null when nothing qualifies.
    /// </summary>
    [HttpGet("opportunity/current")]
    public object CurrentOpportunity()
    {
        var candidate = _entryService.SelectBestCandidate(_opportunityStore.Get());

        if (candidate is null)
            return new { available = false };

        return new
        {
            available = true,
            tradingPair = candidate.Candidate.TradingPair,
            longConnector = candidate.Candidate.LongConnector,
            shortConnector = candidate.Candidate.ShortConnector,
            netEdgePct = candidate.NetEdgePct,
            referencePrice = candidate.BuyQuote?.AveragePrice
        };
    }

    /// <summary>
    /// Manually opens the current best validated opportunity. Requires the manual-trading toggle
    /// to be on AND passes through the same execution gate as the auto worker (armed notional,
    /// kill switch, notional cap, one-shot attempt) - so arming is still required and the kill
    /// switch still overrides everything.
    /// </summary>
    [HttpPost("trades/open")]
    public async Task<IActionResult> OpenTrade(CancellationToken ct)
    {
        var state = await _executionGate.GetStateAsync(ct);

        if (!state.ManualTradingEnabled)
            return BadRequest(new { success = false, reason = "Manual trading is disabled. Enable it before opening." });

        if (state.KillSwitchEnabled)
            return BadRequest(new { success = false, reason = "Kill switch is enabled; manual open is blocked." });

        var existing = await _carryTradeRepository.GetOpenAsync(ct);

        if (existing is not null)
            return BadRequest(new { success = false, reason = "A position is already open; only one position at a time." });

        if (state.MaxNotionalUsd is null || state.MaxNotionalUsd.Value <= 0)
            return BadRequest(new { success = false, reason = "Gate is not armed with a notional. Arm once before opening." });

        var candidate = _entryService.SelectBestCandidate(_opportunityStore.Get());

        if (candidate is null)
            return BadRequest(new { success = false, reason = "No qualifying validated opportunity is available right now." });

        var tradingPair = candidate.Candidate.TradingPair;
        var longConnector = candidate.Candidate.LongConnector;
        var shortConnector = candidate.Candidate.ShortConnector;
        var notionalUsd = state.MaxNotionalUsd.Value;

        var gateResult = await _executionGate.TryAcquireAttemptAsync(
            new TryAcquireExecutionAttemptRequest
            {
                TradingPair = tradingPair,
                LongConnector = longConnector,
                ShortConnector = shortConnector,
                NotionalUsd = notionalUsd
            },
            ct);

        if (!gateResult.IsAllowed || gateResult.AttemptId is null)
            return BadRequest(new { success = false, reason = gateResult.Reason });

        var attemptId = gateResult.AttemptId.Value;

        try
        {
            var result = await _entryService.OpenPositionAsync(
                attemptId,
                tradingPair,
                longConnector,
                shortConnector,
                notionalUsd,
                candidate.NetEdgePct!.Value,
                candidate.BuyQuote!.AveragePrice,
                ct);

            await _executionGate.FinishAttemptAsync(
                new FinishExecutionAttemptRequest
                {
                    AttemptId = attemptId,
                    Succeeded = result.Success,
                    Reason = result.Reason
                },
                ct);

            return result.Success
                ? Ok(new { success = true, reason = result.Reason, tradingPair, longConnector, shortConnector })
                : BadRequest(new { success = false, reason = result.Reason });
        }
        catch (Exception ex)
        {
            await _executionGate.FinishAttemptAsync(
                new FinishExecutionAttemptRequest
                {
                    AttemptId = attemptId,
                    Succeeded = false,
                    Reason = "Unhandled exception during manual entry."
                },
                ct);

            return StatusCode(500, new { success = false, reason = ex.Message });
        }
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

        return Ok(new
        {
            Id = id,
            Status = "Closing",
            Message = "Close requested; the position monitor will close it on its next tick."
        });
    }
}