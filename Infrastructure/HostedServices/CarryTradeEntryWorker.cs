using Arbitrage.Api.Application.Execution;
using Arbitrage.Api.Application.Execution.CarryTrades;
using Arbitrage.Api.Application.MarketData.Opportunities;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.HostedServices;

/// <summary>
/// Watches validated opportunities and opens a real two-leg carry trade when one qualifies.
/// Only ever acts while armed one-shot (ExecutionGateService enforces kill switch / notional
/// caps) and only ever opens one position at a time in v1.
/// </summary>
public sealed class CarryTradeEntryWorker : BackgroundService
{
    private readonly LatestValidatedOpportunityStore _opportunityStore;
    private readonly ExecutionGateService _executionGate;
    private readonly CarryTradeEntryService _entryService;
    private readonly ICarryTradeRepository _repository;
    private readonly ExecutionOptions _options;
    private readonly ILogger<CarryTradeEntryWorker> _logger;

    public CarryTradeEntryWorker(
        LatestValidatedOpportunityStore opportunityStore,
        ExecutionGateService executionGate,
        CarryTradeEntryService entryService,
        ICarryTradeRepository repository,
        IOptions<ExecutionOptions> options,
        ILogger<CarryTradeEntryWorker> logger)
    {
        _opportunityStore = opportunityStore;
        _executionGate = executionGate;
        _entryService = entryService;
        _repository = repository;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await TickAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Carry trade entry tick failed.");
            }

            await Task.Delay(_options.EntryPollIntervalMs, stoppingToken);
        }
    }

    private async Task TickAsync(CancellationToken ct)
    {
        if (_options.Mode != ExecutionMode.ArmedOneShot)
            return;

        var existing = await _repository.GetOpenAsync(ct);

        if (existing is not null)
            return;

        var snapshot = _opportunityStore.Get();

        var candidate = _entryService.SelectBestCandidate(snapshot);

        if (candidate is null)
            return;

        var tradingPair = candidate.Candidate.TradingPair;
        var longConnector = candidate.Candidate.LongConnector;
        var shortConnector = candidate.Candidate.ShortConnector;

        var state = await _executionGate.GetStateAsync(ct);

        if (state.MaxNotionalUsd is null || state.MaxNotionalUsd.Value <= 0)
            return;

        var armedNotionalUsd = state.MaxNotionalUsd.Value;

        var gateResult = await _executionGate.TryAcquireAttemptAsync(
            new TryAcquireExecutionAttemptRequest
            {
                TradingPair = tradingPair,
                LongConnector = longConnector,
                ShortConnector = shortConnector,
                NotionalUsd = armedNotionalUsd
            },
            ct);

        if (!gateResult.IsAllowed || gateResult.AttemptId is null)
            return;

        var attemptId = gateResult.AttemptId.Value;

        try
        {
            var result = await _entryService.OpenPositionAsync(
                attemptId,
                tradingPair,
                longConnector,
                shortConnector,
                armedNotionalUsd,
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
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled error executing carry trade entry attempt {AttemptId}.", attemptId);

            await _executionGate.FinishAttemptAsync(
                new FinishExecutionAttemptRequest
                {
                    AttemptId = attemptId,
                    Succeeded = false,
                    Reason = "Unhandled exception during entry."
                },
                ct);
        }
    }
}
