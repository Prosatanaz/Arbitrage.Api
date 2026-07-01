using Arbitrage.Api.Application.Execution.Trading;
using Xunit;

namespace Arbitrage.Api.Tests.Application.Execution.Trading;

public class OrderSizeRoundingTests
{
    [Fact]
    public void RoundDownToStep_ExactMultiple_ReturnsSameValue()
    {
        var result = OrderSizeRounding.RoundDownToStep(1.20m, 0.10m);

        Assert.Equal(1.20m, result);
    }

    [Fact]
    public void RoundDownToStep_WithRemainder_RoundsDownToStep()
    {
        var result = OrderSizeRounding.RoundDownToStep(1.27m, 0.10m);

        Assert.Equal(1.20m, result);
    }

    [Fact]
    public void RoundDownToStep_StepIsZero_ReturnsOriginalValue()
    {
        var result = OrderSizeRounding.RoundDownToStep(1.27m, 0m);

        Assert.Equal(1.27m, result);
    }

    [Fact]
    public void RoundDownToStep_StepIsNegative_ReturnsOriginalValue()
    {
        var result = OrderSizeRounding.RoundDownToStep(1.27m, -0.10m);

        Assert.Equal(1.27m, result);
    }

    [Fact]
    public void RoundDownToStep_ValueIsZero_ReturnsZero()
    {
        var result = OrderSizeRounding.RoundDownToStep(0m, 0.10m);

        Assert.Equal(0m, result);
    }

    [Fact]
    public void RoundDownToStep_ValueSmallerThanStep_ReturnsZero()
    {
        var result = OrderSizeRounding.RoundDownToStep(0.05m, 0.10m);

        Assert.Equal(0m, result);
    }
}
