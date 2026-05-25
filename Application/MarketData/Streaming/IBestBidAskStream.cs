namespace Arbitrage.Api.Application.MarketData.Streaming;

public interface IBestBidAskStream
{
    string ConnectorName { get; }

    Task StartAsync(
        IReadOnlyList<string> tradingPairs,
        CancellationToken ct);
}