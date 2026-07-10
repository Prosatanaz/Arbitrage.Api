using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Application.MarketData.Opportunities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Application.Execution.CarryTrades;

public sealed record CarryTradeEntryResult(bool Success, string Reason);

/// <summary>
/// Shared "open a real carry-trade position" logic used by both the automatic
/// <c>CarryTradeEntryWorker</c> and the manual open endpoint. Picks a qualifying validated
/// candidate and fires the two legs, but deliberately does NOT touch the execution gate -
/// callers own the arm/kill-switch attempt lifecycle so the gate stays the single safety
/// authority. One position at a time is enforced by the open-trade check plus the gate's
/// one-shot attempt.
/// </summary>
public sealed class CarryTradeEntryService
{
    private readonly ExchangeCredentialService _credentialService;
    private readonly ExchangeTradingClientRegistry _tradingClientRegistry;
    private readonly CarryTradeLegExecutor _legExecutor;
    private readonly ICarryTradeRepository _repository;
    private readonly ExecutionOptions _options;
    private readonly ILogger<CarryTradeEntryService> _logger;

    public CarryTradeEntryService(
        ExchangeCredentialService credentialService,
        ExchangeTradingClientRegistry tradingClientRegistry,
        CarryTradeLegExecutor legExecutor,
        ICarryTradeRepository repository,
        IOptions<ExecutionOptions> options,
        ILogger<CarryTradeEntryService> logger)
    {
        _credentialService = credentialService;
        _tradingClientRegistry = tradingClientRegistry;
        _legExecutor = legExecutor;
        _repository = repository;
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// Picks the highest-net-edge validated candidate that qualifies for entry, or null when
    /// nothing currently qualifies. Same selection rule the auto worker uses.
    /// </summary>
    public ValidatedDepthCandidate? SelectBestCandidate(LatestValidatedOpportunitySnapshot snapshot)
    {
        return snapshot.Items
            .Where(x => x.Status == DepthCandidateValidationStatus.Valid)
            .Where(x => x.NetEdgePct is not null && x.NetEdgePct.Value >= _options.MinEntryNetEdgePct)
            .Where(x => x.BuyQuote is not null && x.SellQuote is not null)
            .Where(x => IsEnabledConnector(x.Candidate.LongConnector) && IsEnabledConnector(x.Candidate.ShortConnector))
            .OrderByDescending(x => x.NetEdgePct)
            .FirstOrDefault();
    }

    /// <summary>
    /// Opens a real two-leg position and persists it. Returns success/failure with a reason;
    /// the caller is responsible for finishing the corresponding execution-gate attempt.
    /// </summary>
    public async Task<CarryTradeEntryResult> OpenPositionAsync(
        Guid attemptId,
        string tradingPair,
        string longConnector,
        string shortConnector,
        decimal armedNotionalUsd,
        decimal entryNetEdgePct,
        decimal referencePrice,
        decimal? grossSpreadPct,
        decimal? estimatedFeesPct,
        CancellationToken ct)
    {
        // Durable decision record: what the auto-worker decided to act on and why, captured
        // BEFORE firing any real order so the basis is logged even if execution then fails.
        await SafeRecordEventAsync(
            new RecordCarryTradeEventRequest(
                TradeId: null,
                AttemptId: attemptId,
                EventType: CarryTradeEventTypes.EntrySignal,
                Reason: $"Entry signal on {tradingPair}: long {longConnector} / short {shortConnector}, net edge {entryNetEdgePct:0.###}%.",
                Details: new
                {
                    tradingPair,
                    longConnector,
                    shortConnector,
                    armedNotionalUsd,
                    entryNetEdgePct,
                    grossSpreadPct,
                    estimatedFeesPct,
                    referencePrice,
                    minEntryNetEdgePct = _options.MinEntryNetEdgePct
                }),
            ct);

        var longCredentials = await _credentialService.GetSecretAsync(longConnector, ct);
        var shortCredentials = await _credentialService.GetSecretAsync(shortConnector, ct);

        if (longCredentials is null || shortCredentials is null)
            return await FailEntryAsync(attemptId, "Missing credentials for one or both connectors.", ct);

        if (!_tradingClientRegistry.TryGetClient(longConnector, out var longClient) ||
            !_tradingClientRegistry.TryGetClient(shortConnector, out var shortClient))
        {
            return await FailEntryAsync(attemptId, "Trading client not implemented for one or both connectors.", ct);
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
            return await FailEntryAsync(attemptId, "Rounded quantity is below exchange minimum.", ct);

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

            return await FailEntryAsync(attemptId, legResult.FailureReason ?? "Entry failed.", ct);
        }

        var entryFeesUsd = (legResult.LongFill?.FeePaidUsd ?? 0m) + (legResult.ShortFill?.FeePaidUsd ?? 0m);

        var trade = await _repository.InsertOpenAsync(
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
                EntryNetEdgePct: entryNetEdgePct,
                EntryGrossSpreadPct: grossSpreadPct,
                EntryEstimatedFeesPct: estimatedFeesPct,
                EntryReferencePrice: referencePrice),
            ct);

        // Per-leg exchange detail (order ids, exact fills, per-exchange fees).
        await SafeRecordLegAsync(trade.Id, CarryTradeLegPhase.Entry, "Long", "Buy", quantity, requestedReduceOnly: false, legResult.LongFill, ct);
        await SafeRecordLegAsync(trade.Id, CarryTradeLegPhase.Entry, "Short", "Sell", quantity, requestedReduceOnly: false, legResult.ShortFill, ct);

        await SafeRecordEventAsync(
            new RecordCarryTradeEventRequest(
                TradeId: trade.Id,
                AttemptId: attemptId,
                EventType: CarryTradeEventTypes.EntryFilled,
                Reason: $"Entry filled: matched qty {legResult.MatchedQuantity}, entry fees {entryFeesUsd:0.####} USDT.",
                Details: new
                {
                    matchedQuantity = legResult.MatchedQuantity,
                    entryLongPrice = legResult.LongFill!.AverageFillPrice,
                    entryShortPrice = legResult.ShortFill!.AverageFillPrice,
                    entryFeesUsd,
                    longLeg = ToLegDetail(legResult.LongFill),
                    shortLeg = ToLegDetail(legResult.ShortFill)
                }),
            ct);

        _logger.LogInformation(
            "Carry trade opened. AttemptId={AttemptId}, Pair={Pair}, Long={Long}, Short={Short}, Qty={Qty}",
            attemptId,
            tradingPair,
            longConnector,
            shortConnector,
            legResult.MatchedQuantity);

        return new CarryTradeEntryResult(true, "Carry trade opened.");
    }

    private async Task<CarryTradeEntryResult> FailEntryAsync(
        Guid attemptId,
        string reason,
        CancellationToken ct)
    {
        await SafeRecordEventAsync(
            new RecordCarryTradeEventRequest(
                TradeId: null,
                AttemptId: attemptId,
                EventType: CarryTradeEventTypes.EntryFailed,
                Reason: reason,
                Details: null),
            ct);

        return new CarryTradeEntryResult(false, reason);
    }

    private async Task SafeRecordLegAsync(
        Guid tradeId,
        CarryTradeLegPhase phase,
        string role,
        string side,
        decimal requestedQuantity,
        bool requestedReduceOnly,
        OrderFillResult? fill,
        CancellationToken ct)
    {
        if (fill is null)
            return;

        try
        {
            await _repository.RecordLegAsync(
                new RecordCarryTradeLegRequest(
                    TradeId: tradeId,
                    Phase: phase,
                    Connector: fill.ConnectorName,
                    Role: role,
                    Side: side,
                    ReduceOnly: requestedReduceOnly,
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
            // Logging must never break execution - the trade itself is already persisted.
            _logger.LogWarning(ex, "Failed to record carry trade leg for {TradeId} ({Role}).", tradeId, role);
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

    private static object ToLegDetail(OrderFillResult fill)
        => new
        {
            connector = fill.ConnectorName,
            exchangeOrderId = fill.ExchangeOrderId,
            filledQuantity = fill.FilledQuantity,
            averageFillPrice = fill.AverageFillPrice,
            feePaidUsd = fill.FeePaidUsd,
            status = fill.Status
        };

    public bool IsEnabledConnector(string connectorName)
    {
        return _options.EnabledConnectors.Contains(
            connectorName,
            StringComparer.OrdinalIgnoreCase);
    }
}
