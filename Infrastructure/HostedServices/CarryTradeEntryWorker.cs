using Arbitrage.Api.Application.Execution;
using Arbitrage.Api.Application.Execution.CarryTrades;
using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Arbitrage.Api.Application.MarketData.Depth;
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
    private readonly ExchangeCredentialService _credentialService;
    private readonly ExchangeTradingClientRegistry _tradingClientRegistry;
    private readonly CarryTradeLegExecutor _legExecutor;
    private readonly ICarryTradeRepository _repository;
    private readonly ExecutionOptions _options;
    private readonly ILogger<CarryTradeEntryWorker> _logger;

    public CarryTradeEntryWorker(
        LatestValidatedOpportunityStore opportunityStore,
        ExecutionGateService executionGate,
        ExchangeCredentialService credentialService,
        ExchangeTradingClientRegistry tradingClientRegistry,
        CarryTradeLegExecutor legExecutor,
        ICarryTradeRepository repository,
        IOptions<ExecutionOptions> options,
        ILogger<CarryTradeEntryWorker> logger)
    {
        _opportunityStore = opportunityStore;
        _executionGate = executionGate;
        _credentialService = credentialService;
        _tradingClientRegistry = tradingClientRegistry;
        _legExecutor = legExecutor;
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

        var candidate = snapshot.Items
            .Where(x => x.Status == DepthCandidateValidationStatus.Valid)
            .Where(x => x.NetEdgePct is not null && x.NetEdgePct.Value >= _options.MinEntryNetEdgePct)
            .Where(x => x.BuyQuote is not null && x.SellQuote is not null)
            .Where(x => IsEnabledConnector(x.Candidate.LongConnector) && IsEnabledConnector(x.Candidate.ShortConnector))
            .OrderByDescending(x => x.NetEdgePct)
            .FirstOrDefault();

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
            await OpenPositionAsync(
                attemptId,
                tradingPair,
                longConnector,
                shortConnector,
                armedNotionalUsd,
                candidate.NetEdgePct!.Value,
                candidate.BuyQuote!.AveragePrice,
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

    private async Task OpenPositionAsync(
        Guid attemptId,
        string tradingPair,
        string longConnector,
        string shortConnector,
        decimal armedNotionalUsd,
        decimal entryNetEdgePct,
        decimal referencePrice,
        CancellationToken ct)
    {
        var longCredentials = await _credentialService.GetSecretAsync(longConnector, ct);
        var shortCredentials = await _credentialService.GetSecretAsync(shortConnector, ct);

        if (longCredentials is null || shortCredentials is null)
        {
            await _executionGate.FinishAttemptAsync(
                new FinishExecutionAttemptRequest { AttemptId = attemptId, Succeeded = false, Reason = "Missing credentials for one or both connectors." },
                ct);
            return;
        }

        if (!_tradingClientRegistry.TryGetClient(longConnector, out var longClient) ||
            !_tradingClientRegistry.TryGetClient(shortConnector, out var shortClient))
        {
            await _executionGate.FinishAttemptAsync(
                new FinishExecutionAttemptRequest { AttemptId = attemptId, Succeeded = false, Reason = "Trading client not implemented for one or both connectors." },
                ct);
            return;
        }

        var longRules = await longClient.GetSymbolRulesAsync(tradingPair, ct);
        var shortRules = await shortClient.GetSymbolRulesAsync(tradingPair, ct);

        var quantityStep = Math.Max(longRules?.QuantityStep ?? 0m, shortRules?.QuantityStep ?? 0m);
        var minQuantity = Math.Max(longRules?.MinQuantity ?? 0m, shortRules?.MinQuantity ?? 0m);

        var rawQuantity = referencePrice > 0 ? armedNotionalUsd / referencePrice : 0m;

        var quantity = quantityStep > 0
            ? OrderSizeRounding.RoundDownToStep(rawQuantity, quantityStep)
            : rawQuantity;

        if (quantity <= 0 || quantity < minQuantity)
        {
            await _executionGate.FinishAttemptAsync(
                new FinishExecutionAttemptRequest { AttemptId = attemptId, Succeeded = false, Reason = "Rounded quantity is below exchange minimum." },
                ct);
            return;
        }

        var legResult = await _legExecutor.ExecuteAsync(
            new LegExecutionRequest(
                TradingPair: tradingPair,
                LongConnector: longConnector,
                ShortConnector: shortConnector,
                LongCredentials: longCredentials,
                ShortCredentials: shortCredentials,
                LongSide: "Buy",
                ShortSide: "Sell",
                Quantity: quantity,
                ReduceOnly: false,
                ClientOrderId: attemptId.ToString(),
                MaxLegDelayMs: _options.MaxLegDelayMs),
            ct);

        if (!legResult.Success)
        {
            _logger.LogError(
                "Carry trade entry failed for attempt {AttemptId}: {Reason}",
                attemptId,
                legResult.FailureReason);

            await _executionGate.FinishAttemptAsync(
                new FinishExecutionAttemptRequest { AttemptId = attemptId, Succeeded = false, Reason = legResult.FailureReason ?? "Entry failed." },
                ct);
            return;
        }

        var entryFeesUsd = (legResult.LongFill?.FeePaidUsd ?? 0m) + (legResult.ShortFill?.FeePaidUsd ?? 0m);

        await _repository.InsertOpenAsync(
            new OpenCarryTradeRequest(
                AttemptId: attemptId,
                TradingPair: tradingPair,
                LongConnector: longConnector,
                ShortConnector: shortConnector,
                NotionalUsd: armedNotionalUsd,
                BaseQuantity: legResult.MatchedQuantity,
                EntryLongPrice: legResult.LongFill!.AverageFillPrice,
                EntryShortPrice: legResult.ShortFill!.AverageFillPrice,
                EntryFeesUsd: entryFeesUsd,
                EntryNetEdgePct: entryNetEdgePct),
            ct);

        await _executionGate.FinishAttemptAsync(
            new FinishExecutionAttemptRequest { AttemptId = attemptId, Succeeded = true, Reason = "Carry trade opened." },
            ct);

        _logger.LogInformation(
            "Carry trade opened. AttemptId={AttemptId}, Pair={Pair}, Long={Long}, Short={Short}, Qty={Qty}",
            attemptId,
            tradingPair,
            longConnector,
            shortConnector,
            legResult.MatchedQuantity);
    }

    private bool IsEnabledConnector(string connectorName)
    {
        return _options.EnabledConnectors.Contains(
            connectorName,
            StringComparer.OrdinalIgnoreCase);
    }
}
