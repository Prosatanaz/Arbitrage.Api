namespace Arbitrage.Api.Application.Execution.CarryTrades;

public static class CarryTradePnlCalculator
{
    /// <summary>
    /// Long leg gains as price rises (bought low, sold high); short leg gains as price falls
    /// (sold high, bought back low) - the two terms have opposite sign by construction.
    /// </summary>
    public static decimal Calculate(
        decimal entryLongPrice,
        decimal entryShortPrice,
        decimal exitLongPrice,
        decimal exitShortPrice,
        decimal quantity,
        decimal entryFeesUsd,
        decimal exitFeesUsd)
    {
        return (exitLongPrice - entryLongPrice) * quantity
            - (exitShortPrice - entryShortPrice) * quantity
            - entryFeesUsd
            - exitFeesUsd;
    }
}
