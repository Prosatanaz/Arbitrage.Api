using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Microsoft.AspNetCore.Mvc;

namespace Arbitrage.Api.Controllers;

/// <summary>
/// Read-only account view: live balances, open positions and open orders per exchange.
/// Never places or cancels orders - purely for monitoring / smoke-testing connectivity.
/// Balances are implemented for all connectors; positions/open-orders only return real data
/// for production-trusted clients (Bybit, HTX) and are empty elsewhere.
/// </summary>
[ApiController]
[Route("api/execution/accounts")]
public sealed class ExchangeAccountsController : ControllerBase
{
    private readonly ExchangeCredentialService _credentialService;
    private readonly ExchangeTradingClientRegistry _tradingClientRegistry;
    private readonly ILogger<ExchangeAccountsController> _logger;

    public ExchangeAccountsController(
        ExchangeCredentialService credentialService,
        ExchangeTradingClientRegistry tradingClientRegistry,
        ILogger<ExchangeAccountsController> logger)
    {
        _credentialService = credentialService;
        _tradingClientRegistry = tradingClientRegistry;
        _logger = logger;
    }

    [HttpGet]
    public async Task<IReadOnlyList<ExchangeAccountView>> List(CancellationToken ct)
    {
        var summaries = await _credentialService.GetSummariesAsync(ct);

        // Only hit exchanges that actually have enabled credentials - skip the rest so the page
        // does not fire pointless authenticated calls that will just fail.
        var candidates = summaries
            .Where(x => x is { IsConfigured: true, IsEnabled: true })
            .ToList();

        var views = await Task.WhenAll(
            candidates.Select(summary => BuildViewAsync(summary.ConnectorName, ct)));

        return views
            .OrderBy(x => x.ConnectorName)
            .ToList();
    }

    private async Task<ExchangeAccountView> BuildViewAsync(
        string connectorName,
        CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;

        if (!_tradingClientRegistry.TryGetClient(connectorName, out var client))
        {
            return ExchangeAccountView.Failed(
                connectorName,
                "Trading client is not implemented for this connector.",
                now);
        }

        ExchangeApiCredentialSecret? credentials;

        try
        {
            credentials = await _credentialService.GetSecretAsync(connectorName, ct);
        }
        catch (Exception ex)
        {
            return ExchangeAccountView.Failed(connectorName, $"Secret decrypt failed: {ex.Message}", now);
        }

        if (credentials is null)
            return ExchangeAccountView.Failed(connectorName, "API credentials are not configured.", now);

        var balancesTask = SafeFetchAsync(() => client.GetBalancesAsync(credentials, ct));
        var positionsTask = SafeFetchAsync(() => client.GetPositionsAsync(credentials, ct));
        var ordersTask = SafeFetchAsync(() => client.GetOpenOrdersAsync(credentials, ct));

        await Task.WhenAll(balancesTask, positionsTask, ordersTask);

        var (balances, balanceError) = balancesTask.Result;
        var (positions, positionError) = positionsTask.Result;
        var (orders, orderError) = ordersTask.Result;

        var error = balanceError ?? positionError ?? orderError;

        if (error is not null)
        {
            _logger.LogWarning(
                "Exchange account view partial failure for {Connector}: {Error}",
                connectorName,
                error);
        }

        return new ExchangeAccountView(
            ConnectorName: connectorName,
            Ok: error is null,
            Error: error,
            Balances: balances,
            Positions: positions,
            OpenOrders: orders,
            FetchedAt: now);
    }

    private static async Task<(IReadOnlyList<T> Items, string? Error)> SafeFetchAsync<T>(
        Func<Task<IReadOnlyList<T>>> fetch)
    {
        try
        {
            return (await fetch(), null);
        }
        catch (Exception ex)
        {
            return (Array.Empty<T>(), ex.Message);
        }
    }
}

public sealed record ExchangeAccountView(
    string ConnectorName,
    bool Ok,
    string? Error,
    IReadOnlyList<ExchangeBalanceSnapshot> Balances,
    IReadOnlyList<ExchangePositionSnapshot> Positions,
    IReadOnlyList<ExchangeOpenOrderSnapshot> OpenOrders,
    DateTimeOffset FetchedAt)
{
    public static ExchangeAccountView Failed(
        string connectorName,
        string error,
        DateTimeOffset fetchedAt)
        => new(
            ConnectorName: connectorName,
            Ok: false,
            Error: error,
            Balances: Array.Empty<ExchangeBalanceSnapshot>(),
            Positions: Array.Empty<ExchangePositionSnapshot>(),
            OpenOrders: Array.Empty<ExchangeOpenOrderSnapshot>(),
            FetchedAt: fetchedAt);
}
