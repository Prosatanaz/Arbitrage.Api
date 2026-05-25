using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Application.MarketData.Streaming;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.HostedServices;

public sealed class CandidateDepthSubscriptionWorker : BackgroundService
{
    private readonly LatestSpreadCandidateStore _candidateStore;
    private readonly DepthSubscriptionTargetStore _targetStore;
    private readonly DepthTrackingOptions _options;
    private readonly ILogger<CandidateDepthSubscriptionWorker> _logger;

    public CandidateDepthSubscriptionWorker(
        LatestSpreadCandidateStore candidateStore,
        DepthSubscriptionTargetStore targetStore,
        IOptions<DepthTrackingOptions> options,
        ILogger<CandidateDepthSubscriptionWorker> logger)
    {
        _candidateStore = candidateStore;
        _targetStore = targetStore;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Candidate depth subscription worker is disabled.");
            return;
        }

        _logger.LogInformation(
            "Candidate depth subscription worker started. IntervalMs={IntervalMs}, CandidateMaxAgeMs={CandidateMaxAgeMs}, MaxCandidatesToTrack={MaxCandidatesToTrack}",
            _options.IntervalMs,
            _options.CandidateMaxAgeMs,
            _options.MaxCandidatesToTrack);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var targets = BuildTargets();

                _targetStore.ReplaceAll(targets);

                if (targets.Count > 0)
                {
                    _logger.LogInformation(
                        "Depth subscription targets updated. Targets={Targets}, Connectors={Connectors}",
                        targets.Count,
                        string.Join(", ", targets
                            .Select(x => x.ConnectorName)
                            .Distinct(StringComparer.OrdinalIgnoreCase)));
                }
                else
                {
                    _logger.LogDebug("Depth subscription targets are empty.");
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to update depth subscription targets.");
            }

            await Task.Delay(_options.IntervalMs, stoppingToken);
        }
    }

    private IReadOnlyList<DepthSubscriptionKey> BuildTargets()
    {
        var now = DateTimeOffset.UtcNow;
        var maxAge = TimeSpan.FromMilliseconds(_options.CandidateMaxAgeMs);

        var (_, trackedCandidates) = _candidateStore.GetTracked();

        var selectedCandidates = trackedCandidates
            .Where(x => now - x.LastSeenAt <= maxAge)
            .OrderByDescending(x => x.Candidate.GrossSpreadPct)
            .Take(_options.MaxCandidatesToTrack)
            .ToList();

        var rawTargets = new List<DepthSubscriptionKey>();

        foreach (var trackedCandidate in selectedCandidates)
        {
            var candidate = trackedCandidate.Candidate;

            rawTargets.Add(new DepthSubscriptionKey(
                candidate.LongConnector,
                candidate.TradingPair));

            rawTargets.Add(new DepthSubscriptionKey(
                candidate.ShortConnector,
                candidate.TradingPair));
        }

        return rawTargets
            .GroupBy(x => x.ConnectorName, StringComparer.OrdinalIgnoreCase)
            .SelectMany(group => group
                .Distinct()
                .Take(_options.MaxPairsPerConnector))
            .Distinct()
            .ToList();
    }
}