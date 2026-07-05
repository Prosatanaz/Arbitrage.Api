using Arbitrage.Api.Application.Instruments;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Application.Execution;

public sealed class ExecutionGateService
{
    private readonly ExecutionOptions _options;
    private readonly IExecutionRuntimeStateRepository _repository;
    private readonly InstrumentFilterService _instrumentFilter;

    public ExecutionGateService(
        IOptions<ExecutionOptions> options,
        IExecutionRuntimeStateRepository repository,
        InstrumentFilterService instrumentFilter)
    {
        _options = options.Value;
        _repository = repository;
        _instrumentFilter = instrumentFilter;
    }

    public Task<ExecutionRuntimeState> GetStateAsync(CancellationToken ct)
    {
        return _repository.GetAsync(ct);
    }

    public async Task<ExecutionGateResult> ArmOnceAsync(
        ArmOnceRequest request,
        CancellationToken ct)
    {
        if (_options.Mode != ExecutionMode.ArmedOneShot)
        {
            var state = await _repository.GetAsync(ct);

            return new ExecutionGateResult(
                false,
                $"Execution mode must be {ExecutionMode.ArmedOneShot}. Current mode={_options.Mode}.",
                null,
                state);
        }

        if (request.MaxNotionalUsd <= 0)
        {
            var state = await _repository.GetAsync(ct);

            return new ExecutionGateResult(
                false,
                "Max notional must be positive.",
                null,
                state);
        }

        if (request.MaxNotionalUsd > _options.MaxLiveNotionalUsd)
        {
            var state = await _repository.GetAsync(ct);

            return new ExecutionGateResult(
                false,
                $"Requested max notional exceeds configured limit. Requested={request.MaxNotionalUsd}, Limit={_options.MaxLiveNotionalUsd}.",
                null,
                state);
        }

        var expiresAfterSeconds = request.ExpiresAfterSeconds ?? _options.ArmExpiresAfterSeconds;

        if (expiresAfterSeconds <= 0)
        {
            var state = await _repository.GetAsync(ct);

            return new ExecutionGateResult(
                false,
                "Arm expiration must be positive.",
                null,
                state);
        }

        if (expiresAfterSeconds > _options.ArmExpiresAfterSeconds)
        {
            var state = await _repository.GetAsync(ct);

            return new ExecutionGateResult(
                false,
                $"Requested expiration exceeds configured limit. Requested={expiresAfterSeconds}s, Limit={_options.ArmExpiresAfterSeconds}s.",
                null,
                state);
        }

        var current = await _repository.GetAsync(ct);

        if (current.RuntimeStatus == ExecutionRuntimeStatus.Executing)
        {
            return new ExecutionGateResult(
                false,
                "Cannot arm while an execution attempt is already running.",
                null,
                current);
        }

        var armedUntil = DateTimeOffset.UtcNow.AddSeconds(expiresAfterSeconds);

        var armedState = await _repository.ArmOnceAsync(
            maxNotionalUsd: request.MaxNotionalUsd,
            armedUntil: armedUntil,
            reason: "Armed one-shot live execution by user.",
            ct: ct);

        return new ExecutionGateResult(
            true,
            "Execution gate armed for one attempt.",
            null,
            armedState);
    }

    public async Task<ExecutionGateResult> DisarmAsync(CancellationToken ct)
    {
        var state = await _repository.DisarmAsync(
            "Execution gate manually disarmed.",
            ct);

        return new ExecutionGateResult(
            true,
            "Execution gate disarmed.",
            null,
            state);
    }

    public async Task<ExecutionGateResult> SetKillSwitchAsync(
        bool enabled,
        CancellationToken ct)
    {
        var state = await _repository.SetKillSwitchAsync(
            enabled,
            enabled
                ? "Kill switch enabled by user."
                : "Kill switch disabled by user.",
            ct);

        return new ExecutionGateResult(
            true,
            enabled ? "Kill switch enabled." : "Kill switch disabled.",
            null,
            state);
    }

    public async Task<ExecutionGateResult> SetManualTradingAsync(
        bool enabled,
        CancellationToken ct)
    {
        var state = await _repository.SetManualTradingEnabledAsync(
            enabled,
            enabled
                ? "Manual trading enabled by user."
                : "Manual trading disabled by user.",
            ct);

        return new ExecutionGateResult(
            true,
            enabled ? "Manual trading enabled." : "Manual trading disabled.",
            null,
            state);
    }

    public async Task<ExecutionGateResult> TryAcquireAttemptAsync(
        TryAcquireExecutionAttemptRequest request,
        CancellationToken ct)
    {
        if (_options.Mode != ExecutionMode.ArmedOneShot)
        {
            var state = await _repository.GetAsync(ct);

            return new ExecutionGateResult(
                false,
                $"Execution mode must be {ExecutionMode.ArmedOneShot}. Current mode={_options.Mode}.",
                null,
                state);
        }

        if (request.NotionalUsd <= 0)
        {
            var state = await _repository.GetAsync(ct);

            return new ExecutionGateResult(
                false,
                "Notional must be positive.",
                null,
                state);
        }

        if (request.NotionalUsd > _options.MaxLiveNotionalUsd)
        {
            var state = await _repository.GetAsync(ct);

            return new ExecutionGateResult(
                false,
                $"Notional exceeds configured limit. Requested={request.NotionalUsd}, Limit={_options.MaxLiveNotionalUsd}.",
                null,
                state);
        }

        var instrumentDecision = _instrumentFilter.EvaluateTradingPair(request.TradingPair);

        if (instrumentDecision.Decision == InstrumentFilterDecision.Blocked)
        {
            var state = await _repository.GetAsync(ct);

            return new ExecutionGateResult(
                false,
                $"Instrument blocked. Reason={instrumentDecision.Reason}",
                null,
                state);
        }

        if (!IsConnectorEnabled(request.LongConnector))
        {
            var state = await _repository.GetAsync(ct);

            return new ExecutionGateResult(
                false,
                $"Long connector is not enabled for execution. Connector={request.LongConnector}.",
                null,
                state);
        }

        if (!IsConnectorEnabled(request.ShortConnector))
        {
            var state = await _repository.GetAsync(ct);

            return new ExecutionGateResult(
                false,
                $"Short connector is not enabled for execution. Connector={request.ShortConnector}.",
                null,
                state);
        }

        var attemptId = Guid.NewGuid();

        var acquiredState = await _repository.TryAcquireAttemptAsync(
            attemptId,
            request.NotionalUsd,
            InstrumentFilterService.NormalizeTradingPair(request.TradingPair),
            request.LongConnector,
            request.ShortConnector,
            "One-shot execution permission acquired.",
            ct);

        if (acquiredState is null)
        {
            var state = await _repository.GetAsync(ct);

            return new ExecutionGateResult(
                false,
                "Execution permission was not acquired. Gate is not armed, expired, locked, kill switch is enabled, or attempt was already consumed.",
                null,
                state);
        }

        return new ExecutionGateResult(
            true,
            "Execution permission acquired.",
            attemptId,
            acquiredState);
    }

    public async Task<ExecutionGateResult> FinishAttemptAsync(
        FinishExecutionAttemptRequest request,
        CancellationToken ct)
    {
        var state = await _repository.FinishAttemptAsync(
            request.AttemptId,
            request.Succeeded,
            string.IsNullOrWhiteSpace(request.Reason)
                ? "Execution attempt finished."
                : request.Reason,
            ct);

        return new ExecutionGateResult(
            true,
            request.Succeeded
                ? "Execution attempt finished successfully. Gate disarmed."
                : "Execution attempt failed. Gate locked by error.",
            request.AttemptId,
            state);
    }

    private bool IsConnectorEnabled(string connectorName)
    {
        if (_options.EnabledConnectors.Length == 0)
            return false;

        return _options.EnabledConnectors.Contains(
            connectorName,
            StringComparer.OrdinalIgnoreCase);
    }
}