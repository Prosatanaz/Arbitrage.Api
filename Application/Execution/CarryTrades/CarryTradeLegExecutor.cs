using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;

namespace Arbitrage.Api.Application.Execution.CarryTrades;

public sealed record LegExecutionRequest(
    string TradingPair,
    string LongConnector,
    string ShortConnector,
    ExchangeApiCredentialSecret LongCredentials,
    ExchangeApiCredentialSecret ShortCredentials,
    string LongSide,
    string ShortSide,
    decimal Quantity,
    bool ReduceOnly,
    string ClientOrderId,
    int MaxLegDelayMs);

public sealed record LegExecutionResult(
    bool Success,
    decimal MatchedQuantity,
    OrderFillResult? LongFill,
    OrderFillResult? ShortFill,
    string? FailureReason);

/// <summary>
/// Shared open/close protocol for a two-leg carry trade: fires both legs in parallel to
/// minimize the naked-leg window, then reconciles actual fills - trimming the more-filled leg
/// down to match the less-filled one (never chasing), or unwinding entirely if one leg didn't
/// fill at all. Used identically by entry and exit.
/// </summary>
public sealed class CarryTradeLegExecutor
{
    private readonly ExchangeTradingClientRegistry _registry;
    private readonly ILogger<CarryTradeLegExecutor> _logger;

    public CarryTradeLegExecutor(
        ExchangeTradingClientRegistry registry,
        ILogger<CarryTradeLegExecutor> logger)
    {
        _registry = registry;
        _logger = logger;
    }

    public async Task<LegExecutionResult> ExecuteAsync(
        LegExecutionRequest request,
        CancellationToken ct)
    {
        if (!_registry.TryGetClient(request.LongConnector, out var longClient))
        {
            return new LegExecutionResult(
                false, 0m, null, null,
                $"Trading client not implemented for connector '{request.LongConnector}'.");
        }

        if (!_registry.TryGetClient(request.ShortConnector, out var shortClient))
        {
            return new LegExecutionResult(
                false, 0m, null, null,
                $"Trading client not implemented for connector '{request.ShortConnector}'.");
        }

        var longOrderRequest = new PlaceOrderRequest(
            request.TradingPair,
            request.LongSide,
            request.Quantity,
            request.ReduceOnly,
            $"{request.ClientOrderId}-L");

        var shortOrderRequest = new PlaceOrderRequest(
            request.TradingPair,
            request.ShortSide,
            request.Quantity,
            request.ReduceOnly,
            $"{request.ClientOrderId}-S");

        // Fire both legs in parallel - sequential firing only stretches the window where one
        // side is exposed with no offsetting hedge.
        var longTask = TryPlaceAsync(longClient, request.LongCredentials, longOrderRequest, request.MaxLegDelayMs, ct);
        var shortTask = TryPlaceAsync(shortClient, request.ShortCredentials, shortOrderRequest, request.MaxLegDelayMs, ct);

        await Task.WhenAll(longTask, shortTask);

        var (longFill, longError) = longTask.Result;
        var (shortFill, shortError) = shortTask.Result;

        var longFilledQty = longFill?.FilledQuantity ?? 0m;
        var shortFilledQty = shortFill?.FilledQuantity ?? 0m;

        // A leg that threw an exception (rather than cleanly reporting a 0 fill) is in an UNKNOWN
        // state: the order may have been placed and filled on the exchange even though we could not
        // read the result. Treating that as "unfilled" is what strands a naked leg. For any such
        // ambiguous leg we fire a reduce-only safety-close: if a position exists it is closed; if
        // the account is flat the reduce-only order is a harmless no-op.
        var longAmbiguous = longFilledQty <= 0 && longError is not null;
        var shortAmbiguous = shortFilledQty <= 0 && shortError is not null;

        if (longFilledQty <= 0 && shortFilledQty <= 0)
        {
            _logger.LogError(
                longError ?? shortError,
                "Both legs failed to fill for attempt {ClientOrderId} on {TradingPair}.",
                request.ClientOrderId,
                request.TradingPair);

            if (longAmbiguous)
                await SafetyCloseAmbiguousLegAsync(longClient, request.LongCredentials, request.TradingPair, OppositeSide(request.LongSide), request.Quantity, request.ClientOrderId, ct);
            if (shortAmbiguous)
                await SafetyCloseAmbiguousLegAsync(shortClient, request.ShortCredentials, request.TradingPair, OppositeSide(request.ShortSide), request.Quantity, request.ClientOrderId, ct);

            return new LegExecutionResult(false, 0m, longFill, shortFill, "Both legs failed to fill.");
        }

        if (longFilledQty <= 0)
        {
            _logger.LogWarning(
                longError,
                "Long leg failed to fill for attempt {ClientOrderId}; unwinding short leg.",
                request.ClientOrderId);

            await UnwindAsync(shortClient, request.ShortCredentials, request.TradingPair, OppositeSide(request.ShortSide), shortFilledQty, request.ClientOrderId, ct);

            if (longAmbiguous)
                await SafetyCloseAmbiguousLegAsync(longClient, request.LongCredentials, request.TradingPair, OppositeSide(request.LongSide), request.Quantity, request.ClientOrderId, ct);

            return new LegExecutionResult(false, 0m, longFill, shortFill, "Long leg failed to fill; short leg unwound.");
        }

        if (shortFilledQty <= 0)
        {
            _logger.LogWarning(
                shortError,
                "Short leg failed to fill for attempt {ClientOrderId}; unwinding long leg.",
                request.ClientOrderId);

            await UnwindAsync(longClient, request.LongCredentials, request.TradingPair, OppositeSide(request.LongSide), longFilledQty, request.ClientOrderId, ct);

            if (shortAmbiguous)
                await SafetyCloseAmbiguousLegAsync(shortClient, request.ShortCredentials, request.TradingPair, OppositeSide(request.ShortSide), request.Quantity, request.ClientOrderId, ct);

            return new LegExecutionResult(false, 0m, longFill, shortFill, "Short leg failed to fill; long leg unwound.");
        }

        // Both legs filled, but never chase a size gap - trim the more-filled leg down to match.
        var matchedQuantity = Math.Min(longFilledQty, shortFilledQty);

        if (longFilledQty > matchedQuantity)
        {
            await UnwindAsync(longClient, request.LongCredentials, request.TradingPair, OppositeSide(request.LongSide), longFilledQty - matchedQuantity, request.ClientOrderId, ct);
        }
        else if (shortFilledQty > matchedQuantity)
        {
            await UnwindAsync(shortClient, request.ShortCredentials, request.TradingPair, OppositeSide(request.ShortSide), shortFilledQty - matchedQuantity, request.ClientOrderId, ct);
        }

        return new LegExecutionResult(true, matchedQuantity, longFill, shortFill, null);
    }

    private async Task UnwindAsync(
        IExchangeTradingClient client,
        ExchangeApiCredentialSecret credentials,
        string tradingPair,
        string side,
        decimal quantity,
        string clientOrderId,
        CancellationToken ct)
    {
        if (quantity <= 0)
            return;

        try
        {
            await client.PlaceOrderAsync(
                credentials,
                new PlaceOrderRequest(tradingPair, side, quantity, true, $"{clientOrderId}-UNWIND"),
                ct);
        }
        catch (Exception ex)
        {
            _logger.LogCritical(
                ex,
                "Failed to unwind leg for attempt {ClientOrderId} on {ConnectorName}. MANUAL INTERVENTION REQUIRED - naked exposure of {Quantity} ({Side}) may remain open.",
                clientOrderId,
                client.ConnectorName,
                quantity,
                side);
        }
    }

    /// <summary>
    /// Fires a reduce-only close for a leg whose placement outcome is unknown (the client threw
    /// rather than reporting a clean fill). Reduce-only makes this safe when the account is flat -
    /// the exchange treats it as a no-op - while guaranteeing any position that actually opened is
    /// closed, so an ambiguous error can never leave a naked leg. Uses the full requested quantity;
    /// reduce-only caps the close at the real position size.
    /// </summary>
    private async Task SafetyCloseAmbiguousLegAsync(
        IExchangeTradingClient client,
        ExchangeApiCredentialSecret credentials,
        string tradingPair,
        string side,
        decimal quantity,
        string clientOrderId,
        CancellationToken ct)
    {
        if (quantity <= 0)
            return;

        _logger.LogWarning(
            "Ambiguous leg on {ConnectorName} for attempt {ClientOrderId} (order may have placed but could not be confirmed) - firing reduce-only safety close of up to {Quantity} ({Side}).",
            client.ConnectorName,
            clientOrderId,
            quantity,
            side);

        try
        {
            await client.PlaceOrderAsync(
                credentials,
                new PlaceOrderRequest(tradingPair, side, quantity, true, $"{clientOrderId}-SAFETY"),
                ct);
        }
        catch (Exception ex)
        {
            // A reduce-only reject usually just means the account was flat (nothing to close) - the
            // safe outcome. But we cannot fully distinguish that from a failed close of a real
            // position, so flag it for a human to verify against the exchange.
            _logger.LogError(
                ex,
                "Reduce-only safety close on {ConnectorName} for attempt {ClientOrderId} was rejected. Most likely the leg never opened (safe), but VERIFY there is no naked {Side} position of ~{Quantity} left open.",
                client.ConnectorName,
                clientOrderId,
                side,
                quantity);
        }
    }

    private static string OppositeSide(string side)
    {
        return side.Equals("Buy", StringComparison.OrdinalIgnoreCase) ? "Sell" : "Buy";
    }

    private static async Task<(OrderFillResult? Fill, Exception? Error)> TryPlaceAsync(
        IExchangeTradingClient client,
        ExchangeApiCredentialSecret credentials,
        PlaceOrderRequest request,
        int timeoutMs,
        CancellationToken ct)
    {
        using var timeoutCts = new CancellationTokenSource(timeoutMs);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);

        try
        {
            var fill = await client.PlaceOrderAsync(credentials, request, linkedCts.Token);

            return (fill, null);
        }
        catch (Exception ex)
        {
            return (null, ex);
        }
    }
}
