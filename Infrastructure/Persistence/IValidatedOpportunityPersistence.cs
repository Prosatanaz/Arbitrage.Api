using Arbitrage.Api.Application.MarketData.Depth;

namespace Arbitrage.Api.Application.Persistence;

public interface IValidatedOpportunityPersistence
{
    Task SaveAsync(
        DateTimeOffset validatedAt,
        IReadOnlyList<ValidatedDepthCandidate> items,
        CancellationToken ct);
}