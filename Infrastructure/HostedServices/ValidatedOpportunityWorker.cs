using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Application.MarketData.Opportunities;
using Arbitrage.Api.Application.Persistence;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.HostedServices;

public sealed class ValidatedOpportunityWorker : BackgroundService
{
    private readonly DepthCandidateEvaluator _depthCandidateEvaluator;
    private readonly LatestValidatedOpportunityStore _store;
    private readonly IValidatedOpportunityPersistence _persistence;
    private readonly ValidatedOpportunityOptions _options;
    private readonly ILogger<ValidatedOpportunityWorker> _logger;

    public ValidatedOpportunityWorker(
        DepthCandidateEvaluator depthCandidateEvaluator,
        LatestValidatedOpportunityStore store,
        IValidatedOpportunityPersistence persistence,
        IOptions<ValidatedOpportunityOptions> options,
        ILogger<ValidatedOpportunityWorker> logger)
    {
        _depthCandidateEvaluator = depthCandidateEvaluator;
        _store = store;
        _persistence = persistence;
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
                var request = new EvaluateDepthCandidatesRequest
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

                var evaluation = _depthCandidateEvaluator.EvaluateCurrent(request);

                var items = evaluation.Items
                    .Take(_options.MaxCandidates)
                    .ToList();

                _store.Set(
                    evaluation,
                    items);

                await _persistence.SaveAsync(
                    evaluation.EvaluatedAt,
                    items,
                    stoppingToken);

                if (items.Count > 0)
                {
                    var best = items
                        .OrderByDescending(x => x.NetEdgePct ?? decimal.MinValue)
                        .First();

                    _logger.LogInformation(
                        "Validated opportunities updated. Items={Items}, Valid={Valid}, NetBelowMin={NetBelowMin}, MissingDepth={MissingDepth}, NotFillable={NotFillable}, Best={BestPair} {BestStatus} {BestNetEdgePct}%",
                        items.Count,
                        evaluation.ValidCount,
                        evaluation.NetEdgeBelowMinimumCount,
                        evaluation.MissingDepthCount,
                        evaluation.NotFillableCount,
                        best.Candidate.TradingPair,
                        best.Status,
                        best.NetEdgePct);
                }
                else
                {
                    _logger.LogDebug(
                        "No validated opportunities produced. CandidatesAvailable={CandidatesAvailable}, CandidatesMatured={CandidatesMatured}, CandidatesTargeted={CandidatesTargeted}, CandidatesChecked={CandidatesChecked}",
                        evaluation.CandidatesAvailable,
                        evaluation.CandidatesMatured,
                        evaluation.CandidatesTargeted,
                        evaluation.CandidatesChecked);
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
                    "Validated opportunity iteration failed.");
            }

            await Task.Delay(_options.IntervalMs, stoppingToken);
        }
    }
}