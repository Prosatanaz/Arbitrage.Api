using Arbitrage.Api.Application.Execution;
using Arbitrage.Api.Application.Execution.CarryTrades;
using Arbitrage.Api.Application.Execution.Credentials;
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
            await ClosePositionAsync(trade, "Manual", ct);
            ResetConfirmState();
            return;
        }

        var heldFor = DateTimeOffset.UtcNow - trade.OpenedAt;

        if (heldFor >= TimeSpan.FromMinutes(_executionOptions.MaxHoldMinutes))
        {
            await ClosePositionAsync(trade, "Timeout", ct);
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
                await ClosePositionAsync(trade, "TakeProfit", ct);
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
        CancellationToken ct)
    {
        var longCredentials = await _credentialService.GetSecretAsync(trade.LongConnector, ct);
        var shortCredentials = await _credentialService.GetSecretAsync(trade.ShortConnector, ct);

        if (longCredentials is null || shortCredentials is null)
        {
            _logger.LogCritical(
                "Carry trade monitor: cannot close trade {TradeId}, missing credentials. MANUAL INTERVENTION REQUIRED - position remains open.",
                trade.Id);
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

        if (!legResult.Success)
        {
            _logger.LogCritical(
                "Carry trade monitor: failed to close trade {TradeId}: {Reason}. MANUAL INTERVENTION REQUIRED - position may still be open.",
                trade.Id,
                legResult.FailureReason);
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
                CloseReason: reason),
            ct);

        _logger.LogInformation(
            "Carry trade closed. TradeId={TradeId}, Reason={Reason}, RealizedPnlUsd={RealizedPnlUsd}",
            trade.Id,
            reason,
            realizedPnlUsd);
    }

    private void ResetConfirmState()
    {
        _confirmTradeId = null;
        _confirmCount = 0;
    }
}
