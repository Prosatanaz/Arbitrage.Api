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
        CancellationToken ct)
    {
        var longCredentials = await _credentialService.GetSecretAsync(longConnector, ct);
        var shortCredentials = await _credentialService.GetSecretAsync(shortConnector, ct);

        if (longCredentials is null || shortCredentials is null)
            return new CarryTradeEntryResult(false, "Missing credentials for one or both connectors.");

        if (!_tradingClientRegistry.TryGetClient(longConnector, out var longClient) ||
            !_tradingClientRegistry.TryGetClient(shortConnector, out var shortClient))
        {
            return new CarryTradeEntryResult(false, "Trading client not implemented for one or both connectors.");
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
            return new CarryTradeEntryResult(false, "Rounded quantity is below exchange minimum.");

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

            return new CarryTradeEntryResult(false, legResult.FailureReason ?? "Entry failed.");
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

        _logger.LogInformation(
            "Carry trade opened. AttemptId={AttemptId}, Pair={Pair}, Long={Long}, Short={Short}, Qty={Qty}",
            attemptId,
            tradingPair,
            longConnector,
            shortConnector,
            legResult.MatchedQuantity);

        return new CarryTradeEntryResult(true, "Carry trade opened.");
    }

    public bool IsEnabledConnector(string connectorName)
    {
        return _options.EnabledConnectors.Contains(
            connectorName,
            StringComparer.OrdinalIgnoreCase);
    }
}
