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

    Task<OrderFillResult> PlaceOrderAsync(
        ExchangeApiCredentialSecret credentials,
        PlaceOrderRequest request,
        CancellationToken ct);
}

public sealed record ExchangeConnectionCheckResult(
    string ConnectorName,
    bool IsConnected,
    string Status,
    string? Error,
    DateTimeOffset CheckedAt);

public sealed record PlaceOrderRequest(
    string TradingPair,
    string Side,
    decimal Quantity,
    bool ReduceOnly,
    string ClientOrderId);

public sealed record OrderFillResult(
    string ConnectorName,
    string ClientOrderId,
    string ExchangeOrderId,
    bool IsFilled,
    decimal FilledQuantity,
    decimal AverageFillPrice,
    decimal FeePaidUsd,
    string Status,
    DateTimeOffset FilledAt);