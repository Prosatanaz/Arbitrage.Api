namespace Arbitrage.Api.Application.MarketData.Streaming;

public interface ITradingPairUniverseProvider
{
    Task<IReadOnlyList<string>> GetTradingPairsAsync(
        CancellationToken ct);

    Task<IReadOnlyList<string>> GetTradingPairsForConnectorAsync(
        string connectorName,
        CancellationToken ct);

    /// <summary>
    /// Same set as <see cref="GetTradingPairsForConnectorAsync"/>, but ordered by
    /// arbitrage relevance: pairs supported by more exchanges come first (more
    /// cross-exchange counterparties = more spread opportunities). Used by streams
    /// that must cap how many symbols they subscribe to, so the most useful ones
    /// are kept.
    /// </summary>
    Task<IReadOnlyList<string>> GetRankedTradingPairsForConnectorAsync(
        string connectorName,
        CancellationToken ct);
}