namespace Arbitrage.Api.Application.Execution.Trading;

public static class OrderSizeRounding
{
    public static decimal RoundDownToStep(decimal value, decimal step)
    {
        if (step <= 0)
            return value;

        return Math.Floor(value / step) * step;
    }
}
