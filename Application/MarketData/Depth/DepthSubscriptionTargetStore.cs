namespace Arbitrage.Api.Application.MarketData.Depth;

public sealed class DepthSubscriptionTargetStore
{
    private readonly object _lock = new();

    private IReadOnlySet<DepthSubscriptionKey> _targets =
        new HashSet<DepthSubscriptionKey>();

    private DateTimeOffset? _updatedAt;

    public void ReplaceAll(IEnumerable<DepthSubscriptionKey> targets)
    {
        lock (_lock)
        {
            _targets = targets
                .Distinct()
                .ToHashSet();

            _updatedAt = DateTimeOffset.UtcNow;
        }
    }

    public IReadOnlySet<DepthSubscriptionKey> GetAll()
    {
        lock (_lock)
        {
            return _targets.ToHashSet();
        }
    }

    public IReadOnlyList<string> GetPairsForConnector(string connectorName)
    {
        lock (_lock)
        {
            return _targets
                .Where(x => x.ConnectorName.Equals(
                    connectorName,
                    StringComparison.OrdinalIgnoreCase))
                .Select(x => x.TradingPair)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x)
                .ToList();
        }
    }

    public DateTimeOffset? GetUpdatedAt()
    {
        lock (_lock)
        {
            return _updatedAt;
        }
    }
}