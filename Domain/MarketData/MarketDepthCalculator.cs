using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Application.MarketData;

public static class MarketDepthCalculator
{
    public static ExecutionQuote CalculateExecutionQuote(
        OrderBookSnapshot snapshot,
        TradeSide side,
        decimal requestedBaseAmount)
    {
        if (requestedBaseAmount <= 0)
            throw new ArgumentOutOfRangeException(nameof(requestedBaseAmount));

        var levels = GetLevels(snapshot, side);

        if (levels.Count == 0)
            throw new InvalidOperationException($"Order book has no levels for side {side}.");

        var bestPrice = levels[0].Price;

        decimal remaining = requestedBaseAmount;
        decimal filledBaseAmount = 0m;
        decimal quoteAmount = 0m;
        decimal worstPrice = bestPrice;
        int levelsUsed = 0;

        foreach (var level in levels)
        {
            if (remaining <= 0)
                break;

            var takenBaseAmount = Math.Min(remaining, level.Amount);

            filledBaseAmount += takenBaseAmount;
            quoteAmount += takenBaseAmount * level.Price;
            remaining -= takenBaseAmount;

            worstPrice = level.Price;
            levelsUsed++;
        }

        if (filledBaseAmount <= 0)
            throw new InvalidOperationException("Could not fill any amount from order book.");

        var averagePrice = quoteAmount / filledBaseAmount;
        var isFullyFillable = filledBaseAmount == requestedBaseAmount;

        var slippagePct = CalculateSlippagePct(
            side,
            bestPrice,
            averagePrice);

        return new ExecutionQuote(
            Side: side,
            RequestedBaseAmount: requestedBaseAmount,
            FilledBaseAmount: filledBaseAmount,
            AveragePrice: averagePrice,
            BestPrice: bestPrice,
            WorstPrice: worstPrice,
            QuoteAmount: quoteAmount,
            SlippagePct: slippagePct,
            LevelsUsed: levelsUsed,
            IsFullyFillable: isFullyFillable);
    }
    public static decimal CalculateBaseAmountFromQuoteNotional(
    OrderBookSnapshot snapshot,
    TradeSide side,
    decimal quoteNotional)
    {
        if (quoteNotional <= 0)
            throw new ArgumentOutOfRangeException(
                nameof(quoteNotional),
                "Quote notional must be greater than zero.");

        var levels = side switch
        {
            TradeSide.Buy => snapshot.Asks,
            TradeSide.Sell => snapshot.Bids,
            _ => throw new ArgumentOutOfRangeException(nameof(side))
        };

        if (levels.Count == 0)
            throw new InvalidOperationException($"Order book has no levels for side {side}.");

        var bestPrice = levels[0].Price;

        if (bestPrice <= 0)
            throw new InvalidOperationException("Best price must be greater than zero.");

        return quoteNotional / bestPrice;
    }
    public static DepthAvailability CalculateAvailableDepthWithinSlippage(
        OrderBookSnapshot snapshot,
        TradeSide side,
        decimal maxSlippagePct)
    {
        if (maxSlippagePct < 0)
            throw new ArgumentOutOfRangeException(nameof(maxSlippagePct));

        var levels = GetLevels(snapshot, side);

        if (levels.Count == 0)
            throw new InvalidOperationException($"Order book has no levels for side {side}.");

        var bestPrice = levels[0].Price;

        var priceLimit = side switch
        {
            TradeSide.Buy => bestPrice * (1 + maxSlippagePct / 100m),
            TradeSide.Sell => bestPrice * (1 - maxSlippagePct / 100m),
            _ => throw new ArgumentOutOfRangeException(nameof(side))
        };

        decimal maxBaseAmount = 0m;
        decimal quoteAmount = 0m;
        int levelsUsed = 0;

        foreach (var level in levels)
        {
            var isAcceptable = side switch
            {
                TradeSide.Buy => level.Price <= priceLimit,
                TradeSide.Sell => level.Price >= priceLimit,
                _ => throw new ArgumentOutOfRangeException(nameof(side))
            };

            if (!isAcceptable)
                break;

            maxBaseAmount += level.Amount;
            quoteAmount += level.Amount * level.Price;
            levelsUsed++;
        }

        return new DepthAvailability(
            Side: side,
            MaxBaseAmount: maxBaseAmount,
            QuoteAmount: quoteAmount,
            BestPrice: bestPrice,
            PriceLimit: priceLimit,
            MaxSlippagePct: maxSlippagePct,
            LevelsUsed: levelsUsed);
    }

    private static IReadOnlyList<OrderBookLevel> GetLevels(
        OrderBookSnapshot snapshot,
        TradeSide side)
    {
        return side switch
        {
            TradeSide.Buy => snapshot.Asks,
            TradeSide.Sell => snapshot.Bids,
            _ => throw new ArgumentOutOfRangeException(nameof(side))
        };
    }

    private static decimal CalculateSlippagePct(
        TradeSide side,
        decimal bestPrice,
        decimal averagePrice)
    {
        if (bestPrice <= 0)
            throw new ArgumentOutOfRangeException(nameof(bestPrice));

        return side switch
        {
            TradeSide.Buy => (averagePrice - bestPrice) / bestPrice * 100m,
            TradeSide.Sell => (bestPrice - averagePrice) / bestPrice * 100m,
            _ => throw new ArgumentOutOfRangeException(nameof(side))
        };
    }
}