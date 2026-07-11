using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Microsoft.Extensions.Logging;

namespace Arbitrage.Api.Application.Execution.CarryTrades;

/// <summary>
/// Reconciles a carry-trade close that reported failure against the exchanges' real position state.
///
/// A reduce-only close fired against an account that is already flat (the position was closed
/// out-of-band, or an earlier partial close finished the job) is REJECTED by the exchange, which the
/// leg executor can only report as "failed to fill". Without reconciliation the monitor then retries
/// and finally marks the trade Failed - a status that lies, because the position is genuinely gone.
///
/// This checks the actual positions on both legs; if both connectors positively confirm no open
/// position for the pair, the trade is marked Closed instead. To avoid mistaking an unverified
/// connector's empty stub for "flat", only connectors whose <see cref="IExchangeTradingClient.SupportsPositionReads"/>
/// is true are trusted - otherwise reconciliation declines and the caller keeps the safe retry/Failed path.
/// </summary>
public sealed class CarryTradeCloseReconciler
{
    private readonly ExchangeTradingClientRegistry _registry;
    private readonly ICarryTradeRepository _repository;
    private readonly ILogger<CarryTradeCloseReconciler> _logger;

    public CarryTradeCloseReconciler(
        ExchangeTradingClientRegistry registry,
        ICarryTradeRepository repository,
        ILogger<CarryTradeCloseReconciler> logger)
    {
        _registry = registry;
        _repository = repository;
        _logger = logger;
    }

    /// <summary>
    /// Returns true if the trade was reconciled to Closed (both legs positively confirmed flat), in
    /// which case the caller must stop retrying the close. Returns false if flatness could not be
    /// positively confirmed (unreadable connector, read error, or a position still exists) - the
    /// caller should then fall back to its bounded-retry / Failed handling.
    /// </summary>
    public async Task<bool> TryReconcileClosedAsync(
        CarryTrade trade,
        ExchangeApiCredentialSecret longCredentials,
        ExchangeApiCredentialSecret shortCredentials,
        string reason,
        decimal? exitNetEdgePct,
        LegExecutionResult legResult,
        CancellationToken ct)
    {
        if (!_registry.TryGetClient(trade.LongConnector, out var longClient) ||
            !_registry.TryGetClient(trade.ShortConnector, out var shortClient))
        {
            return false;
        }

        // Only trust connectors that actually read live positions - a stub's empty list must never
        // be read as "flat", or reconciliation could hide a real naked leg.
        if (!longClient.SupportsPositionReads || !shortClient.SupportsPositionReads)
            return false;

        IReadOnlyList<ExchangePositionSnapshot> longPositions;
        IReadOnlyList<ExchangePositionSnapshot> shortPositions;

        try
        {
            longPositions = await longClient.GetPositionsAsync(longCredentials, ct);
            shortPositions = await shortClient.GetPositionsAsync(shortCredentials, ct);
        }
        catch (Exception ex)
        {
            // A read failure is NOT evidence of flatness - decline and let the caller retry/fail.
            _logger.LogWarning(
                ex,
                "Close reconciliation: position read failed for trade {TradeId} ({Pair}); cannot confirm flat.",
                trade.Id,
                trade.TradingPair);
            return false;
        }

        if (HasOpenPositionForPair(longPositions, trade.TradingPair) ||
            HasOpenPositionForPair(shortPositions, trade.TradingPair))
        {
            // A real position remains - do not reconcile; the caller's retry/Failed path is correct.
            return false;
        }

        // Both legs confirmed flat. Compute realized PnL only if we actually captured a fill on both
        // exit legs; otherwise leave PnL/fees null (the close orders did not fill, so there is no
        // reliable exit price) - an honest "closed, PnL unknown".
        decimal? exitFeesUsd = null;
        decimal? realizedPnlUsd = null;

        var longFill = legResult.LongFill;
        var shortFill = legResult.ShortFill;

        if (longFill is not null && shortFill is not null &&
            longFill.FilledQuantity > 0 && shortFill.FilledQuantity > 0)
        {
            exitFeesUsd = longFill.FeePaidUsd + shortFill.FeePaidUsd;
            realizedPnlUsd = CarryTradePnlCalculator.Calculate(
                trade.EntryLongPrice,
                trade.EntryShortPrice,
                longFill.AverageFillPrice,
                shortFill.AverageFillPrice,
                trade.BaseQuantity,
                trade.EntryFeesUsd,
                exitFeesUsd.Value);
        }

        var closeReason = $"Reconciled: exchanges confirmed flat ({reason}).";

        var marked = await _repository.MarkReconciledClosedAsync(
            trade.Id,
            closeReason,
            realizedPnlUsd,
            exitFeesUsd,
            exitNetEdgePct,
            ct);

        if (!marked)
        {
            // Someone else changed the trade's status concurrently - treat as not-reconciled here.
            return false;
        }

        await SafeRecordEventAsync(new RecordCarryTradeEventRequest(
            TradeId: trade.Id,
            AttemptId: trade.AttemptId,
            EventType: CarryTradeEventTypes.CloseReconciled,
            Reason: $"Close orders did not fill, but {trade.LongConnector} and {trade.ShortConnector} both report no open {trade.TradingPair} position; marked Closed.",
            Details: new
            {
                reason,
                realizedPnlUsd,
                exitFeesUsd,
                pnlKnown = realizedPnlUsd is not null
            }), ct);

        _logger.LogInformation(
            "Carry trade reconciled to Closed (both exchanges flat). TradeId={TradeId}, Reason={Reason}, RealizedPnlUsd={RealizedPnlUsd}",
            trade.Id,
            reason,
            realizedPnlUsd);

        return true;
    }

    /// <summary>
    /// True if a live position with non-zero size exists for the given pair. Symbols are compared
    /// after stripping non-alphanumerics and upper-casing, so exchange-native forms ("TIAUSDT")
    /// match the canonical pair ("TIA-USDT").
    /// </summary>
    private static bool HasOpenPositionForPair(
        IReadOnlyList<ExchangePositionSnapshot> positions,
        string pair)
    {
        var target = Normalize(pair);

        foreach (var position in positions)
        {
            if (position.Size > 0 && Normalize(position.TradingPair) == target)
                return true;
        }

        return false;
    }

    private static string Normalize(string symbol)
        => new string((symbol ?? "").Where(char.IsLetterOrDigit).ToArray()).ToUpperInvariant();

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
}
