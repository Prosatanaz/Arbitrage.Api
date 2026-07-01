using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Application.MarketData.Depth;

public static class OrderBookWalker
{
    public static DepthExecutionQuote Walk(
        IReadOnlyList<OrderBookDepthLevel> levels,
        decimal requestedBaseAmount,
        bool isBuy)
    {
        if (requestedBaseAmount <= 0 || levels.Count == 0)
        {
            return new DepthExecutionQuote(
                RequestedBaseAmount: requestedBaseAmount,
                FilledBaseAmount: 0,
                AveragePrice: 0,
                BestPrice: 0,
                WorstPrice: 0,
                QuoteAmount: 0,
                SlippagePct: 0,
                LevelsUsed: 0,
                IsFullyFillable: false);
        }

        var remainingBase = requestedBaseAmount;
        var filledBase = 0m;
        var quoteAmount = 0m;
        var levelsUsed = 0;
        var worstPrice = 0m;

        var bestPrice = levels[0].Price;

        foreach (var level in levels)
        {
            if (remainingBase <= 0)
                break;

            if (level.Price <= 0 || level.Amount <= 0)
                continue;

            var takeBase = Math.Min(remainingBase, level.Amount);

            filledBase += takeBase;
            quoteAmount += takeBase * level.Price;
            remainingBase -= takeBase;
            worstPrice = level.Price;
            levelsUsed++;
        }

        var averagePrice = filledBase > 0
            ? quoteAmount / filledBase
            : 0m;

        var isFullyFillable = filledBase >= requestedBaseAmount;

        var slippagePct = CalculateSlippagePct(
            bestPrice,
            averagePrice,
            isBuy);

        return new DepthExecutionQuote(
            RequestedBaseAmount: requestedBaseAmount,
            FilledBaseAmount: filledBase,
            AveragePrice: averagePrice,
            BestPrice: bestPrice,
            WorstPrice: worstPrice,
            QuoteAmount: quoteAmount,
            SlippagePct: slippagePct,
            LevelsUsed: levelsUsed,
            IsFullyFillable: isFullyFillable);
    }

    private static decimal CalculateSlippagePct(
        decimal bestPrice,
        decimal averagePrice,
        bool isBuy)
    {
        if (bestPrice <= 0 || averagePrice <= 0)
            return 0m;

        var value = isBuy
            ? (averagePrice - bestPrice) / bestPrice * 100m
            : (bestPrice - averagePrice) / bestPrice * 100m;

        return NormalizeTiny(value);
    }

    private static decimal NormalizeTiny(decimal value)
    {
        return Math.Abs(value) < 0.0000000001m
            ? 0m
            : value;
    }
}
