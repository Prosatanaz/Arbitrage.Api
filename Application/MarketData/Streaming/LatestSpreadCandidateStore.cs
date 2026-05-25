using Arbitrage.Api.Domain.MarketData;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Application.MarketData.Streaming;

public sealed record TrackedSpreadCandidate(
    SpreadCandidate Candidate,
    DateTimeOffset FirstSeenAt,
    DateTimeOffset LastSeenAt);

public sealed class LatestSpreadCandidateStore
{
    private readonly object _lock = new();
    private readonly SpreadDetectorOptions _options;

    private readonly Dictionary<CandidateKey, TrackedSpreadCandidate> _items = new();

    private DateTimeOffset? _updatedAt;

    public LatestSpreadCandidateStore(
        IOptions<SpreadDetectorOptions> options)
    {
        _options = options.Value;
    }

    public void Set(IReadOnlyList<SpreadCandidate> latestCandidates)
    {
        var now = DateTimeOffset.UtcNow;

        lock (_lock)
        {
            foreach (var candidate in latestCandidates)
            {
                var key = CandidateKey.From(candidate);

                if (_items.TryGetValue(key, out var existing))
                {
                    _items[key] = existing with
                    {
                        Candidate = candidate,
                        LastSeenAt = now
                    };
                }
                else
                {
                    _items[key] = new TrackedSpreadCandidate(
                        Candidate: candidate,
                        FirstSeenAt: now,
                        LastSeenAt: now);
                }
            }

            RemoveExpiredCandidates(now);

            _updatedAt = now;
        }
    }

    public (DateTimeOffset? UpdatedAt, IReadOnlyList<SpreadCandidate> Items) Get()
    {
        lock (_lock)
        {
            return (
                _updatedAt,
                _items.Values
                    .OrderByDescending(x => x.Candidate.GrossSpreadPct)
                    .Select(x => x.Candidate)
                    .ToList());
        }
    }

    public (DateTimeOffset? UpdatedAt, IReadOnlyList<TrackedSpreadCandidate> Items) GetTracked()
    {
        lock (_lock)
        {
            return (
                _updatedAt,
                _items.Values
                    .OrderByDescending(x => x.Candidate.GrossSpreadPct)
                    .ToList());
        }
    }

    public int Count()
    {
        lock (_lock)
        {
            return _items.Count;
        }
    }

    private void RemoveExpiredCandidates(DateTimeOffset now)
    {
        var ttl = TimeSpan.FromMilliseconds(_options.CandidateTtlMs);

        var expiredKeys = _items
            .Where(x => now - x.Value.LastSeenAt > ttl)
            .Select(x => x.Key)
            .ToList();

        foreach (var expiredKey in expiredKeys)
        {
            _items.Remove(expiredKey);
        }
    }

    private sealed record CandidateKey(
        string TradingPair,
        string LongConnector,
        string ShortConnector)
    {
        public static CandidateKey From(SpreadCandidate candidate)
        {
            return new CandidateKey(
                TradingPair: candidate.TradingPair,
                LongConnector: candidate.LongConnector,
                ShortConnector: candidate.ShortConnector);
        }
    }
}