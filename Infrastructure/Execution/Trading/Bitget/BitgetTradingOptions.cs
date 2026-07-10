namespace Arbitrage.Api.Infrastructure.Execution.Trading.Bitget;

public sealed class BitgetTradingOptions
{
    public const string SectionName = "BitgetTrading";

    public string BaseUrl { get; init; } = "https://api.bitget.com";

    public string ProductType { get; init; } = "USDT-FUTURES";

    public string MarginCoin { get; init; } = "USDT";

    public string MarginMode { get; init; } = "crossed";

    /// <summary>
    /// Bitget account position mode: "hedge" (two-way, side + tradeSide open/close) or "one_way"
    /// (unilateral, side + reduceOnly). Must match the account's actual setting, otherwise
    /// place-order fails with code 40774. Bitget's default for new accounts is hedge/two-way.
    /// </summary>
    public string PositionMode { get; init; } = "hedge";
}
