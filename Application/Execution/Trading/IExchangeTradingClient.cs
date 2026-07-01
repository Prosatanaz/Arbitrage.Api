using Arbitrage.Api.Application.Execution.Credentials;

namespace Arbitrage.Api.Application.Execution.Trading;

public interface IExchangeTradingClient
{
    string ConnectorName { get; }

    Task<ExchangeConnectionCheckResult> CheckConnectionAsync(
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct);

    Task<IReadOnlyList<ExchangeBalanceSnapshot>> GetBalancesAsync(
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct);

    Task<IReadOnlyList<ExchangePositionSnapshot>> GetPositionsAsync(
        ExchangeApiCredentialSecret credentials,
        CancellationToken ct);

    Task<ExchangeSymbolRules?> GetSymbolRulesAsync(
        string tradingPair,
        CancellationToken ct);
}

public sealed record ExchangeConnectionCheckResult(
    string ConnectorName,
    bool IsConnected,
    string Status,
    string? Error,
    DateTimeOffset CheckedAt);