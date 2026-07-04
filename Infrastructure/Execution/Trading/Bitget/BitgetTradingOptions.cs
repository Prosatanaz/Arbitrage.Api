namespace Arbitrage.Api.Infrastructure.Execution.Trading.Bitget;

public sealed class BitgetTradingOptions
{
    public const string SectionName = "BitgetTrading";

    public string BaseUrl { get; init; } = "https://api.bitget.com";

    public string ProductType { get; init; } = "USDT-FUTURES";

    public string MarginCoin { get; init; } = "USDT";

    public string MarginMode { get; init; } = "crossed";
}
