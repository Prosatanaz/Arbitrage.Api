namespace Arbitrage.Api.Infrastructure.Execution.Trading.Mexc;

public sealed class MexcTradingOptions
{
    public const string SectionName = "MexcTrading";

    public string BaseUrl { get; init; } = "https://contract.mexc.com";
}
