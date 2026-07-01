using Arbitrage.Api.Application.MarketData.Depth;

namespace Arbitrage.Api.Application.Persistence;

public sealed class NoopValidatedOpportunityPersistence : IValidatedOpportunityPersistence
{
    public Task SaveAsync(
        DateTimeOffset validatedAt,
        IReadOnlyList<ValidatedDepthCandidate> items,
        CancellationToken ct)
    {
        return Task.CompletedTask;
    }
}