namespace Arbitrage.Api.Application.MarketData.Universe;

public interface ITradingPairDiscoveryClient
{
    string ConnectorName { get; }

    Task<IReadOnlyList<DiscoveredTradingPair>> GetTradingPairsAsync(
        CancellationToken ct);
}