using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Application.MarketData.Opportunities;
using Microsoft.AspNetCore.Mvc;

namespace Arbitrage.Api.Controllers;

[ApiController]
[Route("api/opportunities")]
public sealed class OpportunitiesController : ControllerBase
{
    private readonly LatestValidatedOpportunityStore _store;

    public OpportunitiesController(
        LatestValidatedOpportunityStore store)
    {
        _store = store;
    }

    [HttpGet("latest")]
    public object Latest()
    {
        var snapshot = _store.Get();

        return new
        {
            snapshot.UpdatedAt,
            HasData = snapshot.UpdatedAt is not null,
            Evaluation = snapshot.Evaluation is null
                ? null
                : new
                {
                    snapshot.Evaluation.EvaluatedAt,
                    snapshot.Evaluation.CandidatesAvailable,
                    snapshot.Evaluation.CandidatesMatured,
                    snapshot.Evaluation.CandidatesTargeted,
                    snapshot.Evaluation.CandidatesSkippedAsNotTargeted,
                    snapshot.Evaluation.CandidatesChecked,
                    snapshot.Evaluation.ValidCount,
                    snapshot.Evaluation.NetEdgeBelowMinimumCount,
                    snapshot.Evaluation.MissingDepthCount,
                    snapshot.Evaluation.NotFillableCount
                },
            Count = snapshot.Items.Count,
            ValidCount = snapshot.Items.Count(x =>
                x.Status == DepthCandidateValidationStatus.Valid),
            PositiveValidCount = snapshot.Items.Count(x =>
                x.Status == DepthCandidateValidationStatus.Valid &&
                x.NetEdgePct > 0),
            Items = snapshot.Items
        };
    }

    [HttpGet("latest/valid")]
    public object LatestValid()
    {
        var snapshot = _store.Get();

        var items = snapshot.Items
            .Where(x =>
                x.Status == DepthCandidateValidationStatus.Valid &&
                x.NetEdgePct > 0)
            .OrderByDescending(x => x.NetEdgePct)
            .ToList();

        return new
        {
            snapshot.UpdatedAt,
            HasData = snapshot.UpdatedAt is not null,
            Count = items.Count,
            Items = items
        };
    }

    [HttpGet("latest/summary")]
    public object LatestSummary()
    {
        var snapshot = _store.Get();

        var items = snapshot.Items
            .Where(x =>
                x.Status == DepthCandidateValidationStatus.Valid &&
                x.NetEdgePct > 0 &&
                x.BuyQuote is not null &&
                x.SellQuote is not null &&
                x.GrossSpreadPct is not null &&
                x.EstimatedFeesPct is not null)
            .OrderByDescending(x => x.NetEdgePct)
            .Select(x =>
            {
                var buyQuote = x.BuyQuote!;
                var sellQuote = x.SellQuote!;

                var netEdgePct = x.NetEdgePct!.Value;
                var estimatedProfitUsd = x.NotionalUsd * netEdgePct / 100m;

                return new ValidatedOpportunitySummary(
                    TradingPair: x.Candidate.TradingPair,
                    Direction: $"Long {x.Candidate.LongConnector} / Short {x.Candidate.ShortConnector}",
                    LongConnector: x.Candidate.LongConnector,
                    ShortConnector: x.Candidate.ShortConnector,
                    NotionalUsd: x.NotionalUsd,
                    BuyAveragePrice: buyQuote.AveragePrice,
                    SellAveragePrice: sellQuote.AveragePrice,
                    GrossSpreadPct: x.GrossSpreadPct!.Value,
                    EstimatedFeesPct: x.EstimatedFeesPct!.Value,
                    NetEdgePct: netEdgePct,
                    BuySlippagePct: buyQuote.SlippagePct,
                    SellSlippagePct: sellQuote.SlippagePct,
                    BuyLevelsUsed: buyQuote.LevelsUsed,
                    SellLevelsUsed: sellQuote.LevelsUsed,
                    RequestedBaseAmount: x.RequestedBaseAmount,
                    BuyQuoteAmount: buyQuote.QuoteAmount,
                    SellQuoteAmount: sellQuote.QuoteAmount,
                    EstimatedProfitUsd: estimatedProfitUsd,
                    DetectedAt: x.Candidate.DetectedAt,
                    ValidatedAt: snapshot.UpdatedAt);
            })
            .ToList();

        return new
        {
            snapshot.UpdatedAt,
            HasData = snapshot.UpdatedAt is not null,
            Count = items.Count,
            Items = items
        };
    }
}