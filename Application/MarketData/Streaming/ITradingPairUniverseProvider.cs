namespace Arbitrage.Api.Application.MarketData.Streaming;

public interface ITradingPairUniverseProvider
{
    Task<IReadOnlyList<string>> GetTradingPairsAsync(
        CancellationToken ct);

    Task<IReadOnlyList<string>> GetTradingPairsForConnectorAsync(
        string connectorName,
        CancellationToken ct);
}