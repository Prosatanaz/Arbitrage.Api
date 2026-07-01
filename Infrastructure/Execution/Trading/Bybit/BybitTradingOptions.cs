namespace Arbitrage.Api.Infrastructure.Execution.Trading.Bybit;

public sealed class BybitTradingOptions
{
    public const string SectionName = "BybitTrading";

    public string BaseUrl { get; init; } = "https://api.bybit.com";

    public string AccountType { get; init; } = "UNIFIED";

    public int RecvWindowMs { get; init; } = 5000;

    public string Category { get; init; } = "linear";
}