using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Application.MarketData.Opportunities;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.HostedServices;

public sealed class ValidatedOpportunityWorker : BackgroundService
{
    private readonly DepthCandidateEvaluator _evaluator;
    private readonly LatestValidatedOpportunityStore _store;
    private readonly ValidatedOpportunityOptions _options;
    private readonly ILogger<ValidatedOpportunityWorker> _logger;

    public ValidatedOpportunityWorker(
        DepthCandidateEvaluator evaluator,
        LatestValidatedOpportunityStore store,
        IOptions<ValidatedOpportunityOptions> options,
        ILogger<ValidatedOpportunityWorker> logger)
    {
        _evaluator = evaluator;
        _store = store;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Validated opportunity worker is disabled.");
            return;
        }

        _logger.LogInformation(
            "Validated opportunity worker started. IntervalMs={IntervalMs}, MaxCandidates={MaxCandidates}, NotionalUsd={NotionalUsd}",
            _options.IntervalMs,
            _options.MaxCandidates,
            _options.NotionalUsd);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var request = BuildRequest();

                var evaluation = _evaluator.EvaluateCurrent(request);

                var items = FilterItems(evaluation.Items)
                    .OrderByDescending(x => x.NetEdgePct ?? decimal.MinValue)
                    .ToList();

                _store.Set(
                    evaluation: evaluation,
                    items: items);

                if (items.Count > 0)
                {
                    var best = items.First();

                    _logger.LogInformation(
                        "Validated opportunities updated. Items={Items}, Valid={Valid}, NetBelowMin={NetBelowMin}, MissingDepth={MissingDepth}, Best={BestPair} {BestStatus} {BestNetEdgePct}%",
                        items.Count,
                        evaluation.ValidCount,
                        evaluation.NetEdgeBelowMinimumCount,
                        evaluation.MissingDepthCount,
                        best.Candidate.TradingPair,
                        best.Status,
                        best.NetEdgePct);
                }
                else
                {
                    _logger.LogDebug(
                        "Validated opportunities are empty. CandidatesChecked={CandidatesChecked}, MissingDepth={MissingDepth}",
                        evaluation.CandidatesChecked,
                        evaluation.MissingDepthCount);
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
                    "Validated opportunity update failed.");
            }

            await Task.Delay(_options.IntervalMs, stoppingToken);
        }
    }

    private EvaluateDepthCandidatesRequest BuildRequest()
    {
        return new EvaluateDepthCandidatesRequest
        {
            MaxCandidates = _options.MaxCandidates,
            NotionalUsd = _options.NotionalUsd,
            MinNetEdgePct = _options.MinNetEdgePct,
            MaxDepthAgeMs = _options.MaxDepthAgeMs,
            MinCandidateAgeMs = _options.MinCandidateAgeMs,
            DefaultTakerFeePct = _options.DefaultTakerFeePct,
            CloseFeeBufferPct = _options.CloseFeeBufferPct,
            SafetyBufferPct = _options.SafetyBufferPct,
            TakerFeesPct = _options.TakerFeesPct
        };
    }

    private IEnumerable<ValidatedDepthCandidate> FilterItems(
        IReadOnlyList<ValidatedDepthCandidate> items)
    {
        foreach (var item in items)
        {
            if (item.Status == DepthCandidateValidationStatus.Valid)
            {
                yield return item;
                continue;
            }

            if (_options.IncludeNetEdgeBelowMinimum &&
                item.Status == DepthCandidateValidationStatus.NetEdgeBelowMinimum)
            {
                yield return item;
                continue;
            }

            if (_options.IncludeNotFullyFillable &&
                item.Status == DepthCandidateValidationStatus.NotFullyFillable)
            {
                yield return item;
                continue;
            }

            if (_options.IncludeMissingDepth &&
                item.Status is DepthCandidateValidationStatus.MissingBuyDepth
                    or DepthCandidateValidationStatus.MissingSellDepth
                    or DepthCandidateValidationStatus.StaleBuyDepth
                    or DepthCandidateValidationStatus.StaleSellDepth)
            {
                yield return item;
            }
        }
    }
}