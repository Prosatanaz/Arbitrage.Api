using Arbitrage.Api.Application.MarketData.Depth;

namespace Arbitrage.Api.Application.MarketData.Opportunities;

public sealed record LatestValidatedOpportunitySnapshot(
    DateTimeOffset? UpdatedAt,
    EvaluateDepthCandidatesResponse? Evaluation,
    IReadOnlyList<ValidatedDepthCandidate> Items);

public sealed class LatestValidatedOpportunityStore
{
    private readonly object _lock = new();

    private DateTimeOffset? _updatedAt;

    private EvaluateDepthCandidatesResponse? _evaluation;

    private IReadOnlyList<ValidatedDepthCandidate> _items =
        Array.Empty<ValidatedDepthCandidate>();

    public void Set(
        EvaluateDepthCandidatesResponse evaluation,
        IReadOnlyList<ValidatedDepthCandidate> items)
    {
        lock (_lock)
        {
            _updatedAt = DateTimeOffset.UtcNow;
            _evaluation = evaluation;
            _items = items.ToList();
        }
    }

    public LatestValidatedOpportunitySnapshot Get()
    {
        lock (_lock)
        {
            return new LatestValidatedOpportunitySnapshot(
                UpdatedAt: _updatedAt,
                Evaluation: _evaluation,
                Items: _items.ToList());
        }
    }
}