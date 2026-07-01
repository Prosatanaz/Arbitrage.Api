namespace Arbitrage.Api.Application.Execution.Trading;

public sealed record ExchangeBalanceSnapshot(
    string ConnectorName,
    string Asset,
    decimal WalletBalance,
    decimal AvailableBalance,
    decimal Equity,
    decimal UsdValue,
    DateTimeOffset ReceivedAt);

public sealed record ExchangePositionSnapshot(
    string ConnectorName,
    string TradingPair,
    decimal Size,
    decimal EntryPrice,
    decimal MarkPrice,
    decimal UnrealizedPnl,
    string Side,
    DateTimeOffset ReceivedAt);

public sealed record ExchangeSymbolRules(
    string ConnectorName,
    string TradingPair,
    string ExchangeSymbol,
    string Status,
    decimal TickSize,
    decimal QuantityStep,
    decimal MinQuantity,
    decimal MinNotional,
    decimal? MaxMarketQuantity,
    decimal? MaxLimitQuantity,
    DateTimeOffset ReceivedAt);