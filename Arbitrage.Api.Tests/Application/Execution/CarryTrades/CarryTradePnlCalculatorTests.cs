using Arbitrage.Api.Application.Execution.CarryTrades;
using Xunit;

namespace Arbitrage.Api.Tests.Application.Execution.CarryTrades;

public class CarryTradePnlCalculatorTests
{
    [Fact]
    public void Calculate_SpreadConverges_ReturnsPositivePnl()
    {
        // Entry: long=100.00, short=100.30 (0.30% edge). Exit: long=101.00, short=101.05 (0.05% edge).
        // Long leg: +1.00/unit. Short leg: -0.75/unit. Net: +0.25/unit before fees.
        var result = CarryTradePnlCalculator.Calculate(
            entryLongPrice: 100.00m,
            entryShortPrice: 100.30m,
            exitLongPrice: 101.00m,
            exitShortPrice: 101.05m,
            quantity: 10m,
            entryFeesUsd: 0m,
            exitFeesUsd: 0m);

        Assert.Equal(2.5m, result);
    }

    [Fact]
    public void Calculate_SpreadWidens_ReturnsNegativePnl()
    {
        // Entry edge 0.30%, exit edge widens to 1.49% - the basis moved against the position.
        var result = CarryTradePnlCalculator.Calculate(
            entryLongPrice: 100.00m,
            entryShortPrice: 100.30m,
            exitLongPrice: 101.00m,
            exitShortPrice: 102.50m,
            quantity: 10m,
            entryFeesUsd: 0m,
            exitFeesUsd: 0m);

        Assert.Equal(-12m, result);
    }

    [Fact]
    public void Calculate_SubtractsEntryAndExitFees()
    {
        var result = CarryTradePnlCalculator.Calculate(
            entryLongPrice: 100.00m,
            entryShortPrice: 100.30m,
            exitLongPrice: 100.00m,
            exitShortPrice: 100.30m,
            quantity: 10m,
            entryFeesUsd: 1.5m,
            exitFeesUsd: 2.5m);

        Assert.Equal(-4m, result);
    }
}
