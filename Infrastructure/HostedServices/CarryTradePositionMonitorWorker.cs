using Arbitrage.Api.Application.Execution;
using Arbitrage.Api.Application.Execution.CarryTrades;
using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Application.MarketData.Opportunities;
using Arbitrage.Api.Application.MarketData.Streaming;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.HostedServices;

/// <summary>
/// Always-on monitor for the currently open carry trade. Runs independently of the execution
/// gate/kill switch - closing risk must never be blockable by the same switch that blocks
/// opening new risk. Exactly two triggers: take-profit (debounced, depth-confirmed) and a
/// timeout backstop that force-closes regardless of current edge.
/// </summary>
public sealed class CarryTradePositionMonitorWorker : BackgroundService
{
    private readonly ICarryTradeRepository _repository;
    private readonly BestBidAskCache _bboCache;
    private readonly DepthCandidateEvaluator _depthEvaluator;
    private readonly CarryTradeLegExecutor _legExecutor;
    private readonly ExchangeCredentialService _credentialService;
    private readonly ExecutionOptions _executionOptions;
    private readonly ValidatedOpportunityOptions _opportunityOptions;
    private readonly ILogger<CarryTradePositionMonitorWorker> _logger;

    private Guid? _confirmTradeId;
    private int _confirmCount;

    public CarryTradePositionMonitorWorker(
        ICarryTradeRepository repository,
        BestBidAskCache bboCache,
        DepthCandidateEvaluator depthEvaluator,
        CarryTradeLegExecutor legExecutor,
        ExchangeCredentialService credentialService,
        IOptions<ExecutionOptions> executionOptions,
        IOptions<ValidatedOpportunityOptions> opportunityOptions,
        ILogger<CarryTradePositionMonitorWorker> logger)
    {
        _repository = repository;
        _bboCache = bboCache;
        _depthEvaluator = depthEvaluator;
        _legExecutor = legExecutor;
        _credentialService = credentialService;
        _executionOptions = executionOptions.Value;
        _opportunityOptions = opportunityOptions.Value;
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
                _logger.LogWarning(ex, "Carry trade position monitor tick failed.");
            }

            await Task.Delay(_executionOptions.ExitPollIntervalMs, stoppingToken);
        }
    }

    private async Task TickAsync(CancellationToken ct)
    {
        var trade = await _repository.GetOpenAsync(ct);

        if (trade is null)
        {
            ResetConfirmState();
            return;
        }

        if (trade.Status == CarryTradeStatus.Closing)
        {
            await SafeRecordEventAsync(new RecordCarryTradeEventRequest(
                TradeId: trade.Id,
                AttemptId: trade.AttemptId,
                EventType: CarryTradeEventTypes.ExitSignal,
                Reason: "Manual close requested; closing position.",
                Details: new { trigger = "Manual" }), ct);

            await ClosePositionAsync(trade, "Manual", null, ct);
            ResetConfirmState();
            return;
        }

        var heldFor = DateTimeOffset.UtcNow - trade.OpenedAt;

        if (heldFor >= TimeSpan.FromMinutes(_executionOptions.MaxHoldMinutes))
        {
            await SafeRecordEventAsync(new RecordCarryTradeEventRequest(
                TradeId: trade.Id,
                AttemptId: trade.AttemptId,
                EventType: CarryTradeEventTypes.ExitSignal,
                Reason: $"Max-hold timeout reached ({_executionOptions.MaxHoldMinutes} min); force-closing.",
                Details: new
                {
                    trigger = "Timeout",
                    heldMinutes = heldFor.TotalMinutes,
                    maxHoldMinutes = _executionOptions.MaxHoldMinutes
                }), ct);

            await ClosePositionAsync(trade, "Timeout", null, ct);
            ResetConfirmState();
            return;
        }

        if (_confirmTradeId != trade.Id)
        {
            _confirmTradeId = trade.Id;
            _confirmCount = 0;
        }

        // Stage 1: cheap BBO gate - skip the expensive depth walk unless we're plausibly near
        // the exit threshold already.
        if (!_bboCache.TryGet(trade.LongConnector, trade.TradingPair, out var longBbo) || longBbo is null ||
            !_bboCache.TryGet(trade.ShortConnector, trade.TradingPair, out var shortBbo) || shortBbo is null)
        {
            _logger.LogWarning(
                "Carry trade monitor: BBO data missing for open trade {TradeId} ({Pair}). Not acting.",
                trade.Id,
                trade.TradingPair);
            return;
        }

        if (longBbo.BestAskPrice <= 0)
            return;

        var approxEdgePct = (shortBbo.BestBidPrice - longBbo.BestAskPrice) / longBbo.BestAskPrice * 100m;

        if (approxEdgePct > _executionOptions.ExitNetEdgePct * 3m)
        {
            _confirmCount = 0;
            return;
        }

        // Stage 2: only fire on a depth-confirmed edge, sized to the position's actual open
        // quantity - never act on raw BBO alone.
        var request = new EvaluateDepthCandidatesRequest
        {
            NotionalUsd = trade.NotionalUsd,
            MinNetEdgePct = 0m,
            MaxDepthAgeMs = _opportunityOptions.MaxDepthAgeMs,
            DefaultTakerFeePct = _opportunityOptions.DefaultTakerFeePct,
            CloseFeeBufferPct = _opportunityOptions.CloseFeeBufferPct,
            SafetyBufferPct = _opportunityOptions.SafetyBufferPct,
            TakerFeesPct = _opportunityOptions.TakerFeesPct
        };

        var evaluation = _depthEvaluator.EvaluateForOpenPosition(
            trade.TradingPair,
            trade.LongConnector,
            trade.ShortConnector,
            trade.BaseQuantity,
            request);

        if (evaluation.NetEdgePct is null)
        {
            _logger.LogWarning(
                "Carry trade monitor: depth stale/missing for open trade {TradeId} ({Pair}), status={Status}. Not acting.",
                trade.Id,
                trade.TradingPair,
                evaluation.Status);
            return;
        }

        if (evaluation.NetEdgePct.Value <= _executionOptions.ExitNetEdgePct)
        {
            _confirmCount++;

            if (_confirmCount >= _executionOptions.ExitConfirmSamples)
            {
                await SafeRecordEventAsync(new RecordCarryTradeEventRequest(
                    TradeId: trade.Id,
                    AttemptId: trade.AttemptId,
                    EventType: CarryTradeEventTypes.ExitSignal,
                    Reason: $"Take-profit confirmed: depth net edge {evaluation.NetEdgePct.Value:0.###}% <= exit threshold {_executionOptions.ExitNetEdgePct:0.###}% for {_confirmCount} samples.",
                    Details: new
                    {
                        trigger = "TakeProfit",
                        approxBboEdgePct = approxEdgePct,
                        depthNetEdgePct = evaluation.NetEdgePct.Value,
                        exitThresholdPct = _executionOptions.ExitNetEdgePct,
                        confirmSamples = _confirmCount,
                        requiredSamples = _executionOptions.ExitConfirmSamples
                    }), ct);

                await ClosePositionAsync(trade, "TakeProfit", evaluation.NetEdgePct.Value, ct);
                ResetConfirmState();
            }
        }
        else
        {
            _confirmCount = 0;
        }
    }

    private async Task ClosePositionAsync(
        CarryTrade trade,
        string reason,
        decimal? exitNetEdgePct,
        CancellationToken ct)
    {
        var longCredentials = await _credentialService.GetSecretAsync(trade.LongConnector, ct);
        var shortCredentials = await _credentialService.GetSecretAsync(trade.ShortConnector, ct);

        if (longCredentials is null || shortCredentials is null)
        {
            _logger.LogCritical(
                "Carry trade monitor: cannot close trade {TradeId}, missing credentials. MANUAL INTERVENTION REQUIRED - position remains open.",
                trade.Id);

            await SafeRecordEventAsync(new RecordCarryTradeEventRequest(
                TradeId: trade.Id,
                AttemptId: trade.AttemptId,
                EventType: CarryTradeEventTypes.ManualInterventionRequired,
                Reason: "Cannot close: missing credentials for one or both connectors. Position remains open.",
                Details: new { reason, longConnector = trade.LongConnector, shortConnector = trade.ShortConnector }), ct);
            return;
        }

        var legResult = await _legExecutor.ExecuteAsync(
            new LegExecutionRequest(
                TradingPair: trade.TradingPair,
                LongConnector: trade.LongConnector,
                ShortConnector: trade.ShortConnector,
                LongCredentials: longCredentials,
                ShortCredentials: shortCredentials,
                LongSide: "Sell", // closing the long position
                ShortSide: "Buy", // closing the short position
                Quantity: trade.BaseQuantity,
                ReduceOnly: true,
                ClientOrderId: $"{trade.AttemptId}-close",
                MaxLegDelayMs: _executionOptions.MaxLegDelayMs),
            ct);

        // Record whatever legs did fill BEFORE branching on success, so partial-close detail is
        // never lost even when the close as a whole is reported as failed.
        await SafeRecordLegAsync(trade.Id, "Long", "Sell", trade.BaseQuantity, legResult.LongFill, ct);
        await SafeRecordLegAsync(trade.Id, "Short", "Buy", trade.BaseQuantity, legResult.ShortFill, ct);

        if (!legResult.Success)
        {
            _logger.LogCritical(
                "Carry trade monitor: failed to close trade {TradeId}: {Reason}. MANUAL INTERVENTION REQUIRED - position may still be open.",
                trade.Id,
                legResult.FailureReason);

            await SafeRecordEventAsync(new RecordCarryTradeEventRequest(
                TradeId: trade.Id,
                AttemptId: trade.AttemptId,
                EventType: CarryTradeEventTypes.ManualInterventionRequired,
                Reason: $"Close failed: {legResult.FailureReason}. Position may still be open.",
                Details: new { reason, failureReason = legResult.FailureReason }), ct);
            return;
        }

        var exitFeesUsd = (legResult.LongFill?.FeePaidUsd ?? 0m) + (legResult.ShortFill?.FeePaidUsd ?? 0m);
        var exitLongPrice = legResult.LongFill!.AverageFillPrice;
        var exitShortPrice = legResult.ShortFill!.AverageFillPrice;

        var realizedPnlUsd = CarryTradePnlCalculator.Calculate(
            trade.EntryLongPrice,
            trade.EntryShortPrice,
            exitLongPrice,
            exitShortPrice,
            trade.BaseQuantity,
            trade.EntryFeesUsd,
            exitFeesUsd);

        await _repository.MarkClosedAsync(
            new CloseCarryTradeRequest(
                Id: trade.Id,
                ExitLongPrice: exitLongPrice,
                ExitShortPrice: exitShortPrice,
                ExitFeesUsd: exitFeesUsd,
                RealizedPnlUsd: realizedPnlUsd,
                ExitNetEdgePct: exitNetEdgePct,
                CloseReason: reason),
            ct);

        await SafeRecordEventAsync(new RecordCarryTradeEventRequest(
            TradeId: trade.Id,
            AttemptId: trade.AttemptId,
            EventType: CarryTradeEventTypes.ExitFilled,
            Reason: $"Position closed ({reason}). Realized PnL {realizedPnlUsd:0.####} USDT, exit fees {exitFeesUsd:0.####} USDT.",
            Details: new
            {
                reason,
                realizedPnlUsd,
                exitLongPrice,
                exitShortPrice,
                exitFeesUsd,
                exitNetEdgePct,
                longLeg = ToLegDetail(legResult.LongFill),
                shortLeg = ToLegDetail(legResult.ShortFill)
            }), ct);

        _logger.LogInformation(
            "Carry trade closed. TradeId={TradeId}, Reason={Reason}, RealizedPnlUsd={RealizedPnlUsd}",
            trade.Id,
            reason,
            realizedPnlUsd);
    }

    private async Task SafeRecordLegAsync(
        Guid tradeId,
        string role,
        string side,
        decimal requestedQuantity,
        OrderFillResult? fill,
        CancellationToken ct)
    {
        if (fill is null || fill.FilledQuantity <= 0)
            return;

        try
        {
            await _repository.RecordLegAsync(
                new RecordCarryTradeLegRequest(
                    TradeId: tradeId,
                    Phase: CarryTradeLegPhase.Exit,
                    Connector: fill.ConnectorName,
                    Role: role,
                    Side: side,
                    ReduceOnly: true,
                    RequestedQuantity: requestedQuantity,
                    FilledQuantity: fill.FilledQuantity,
                    AverageFillPrice: fill.AverageFillPrice,
                    FeePaidUsd: fill.FeePaidUsd,
                    ExchangeOrderId: fill.ExchangeOrderId,
                    ClientOrderId: fill.ClientOrderId,
                    Status: fill.Status,
                    FilledAt: fill.FilledAt),
                ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to record exit leg for {TradeId} ({Role}).", tradeId, role);
        }
    }

    private async Task SafeRecordEventAsync(
        RecordCarryTradeEventRequest request,
        CancellationToken ct)
    {
        try
        {
            await _repository.RecordEventAsync(request, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to record carry trade event {EventType}.", request.EventType);
        }
    }

    private static object? ToLegDetail(OrderFillResult? fill)
        => fill is null
            ? null
            : new
            {
                connector = fill.ConnectorName,
                exchangeOrderId = fill.ExchangeOrderId,
                filledQuantity = fill.FilledQuantity,
                averageFillPrice = fill.AverageFillPrice,
                feePaidUsd = fill.FeePaidUsd,
                status = fill.Status
            };

    private void ResetConfirmState()
    {
        _confirmTradeId = null;
        _confirmCount = 0;
    }
}
