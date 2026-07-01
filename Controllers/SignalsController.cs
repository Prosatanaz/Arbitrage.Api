using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Application.MarketData.Opportunities;
using Arbitrage.Api.Application.SignalQuality;
using Microsoft.AspNetCore.Mvc;

namespace Arbitrage.Api.Controllers;

[ApiController]
[Route("api/signals")]
public sealed class SignalsController : ControllerBase
{
    private readonly LatestValidatedOpportunityStore _opportunityStore;
    private readonly SignalQualityService _signalQualityService;

    public SignalsController(
        LatestValidatedOpportunityStore opportunityStore,
        SignalQualityService signalQualityService)
    {
        _opportunityStore = opportunityStore;
        _signalQualityService = signalQualityService;
    }

    [HttpGet("active")]
    public object Active(
    [FromQuery] int limit = 100)
    {
        var snapshot = _opportunityStore.Get();
        var now = DateTimeOffset.UtcNow;

        var safeLimit = Math.Clamp(limit, 1, 500);

        var items = snapshot.Items
            .Select(item => BuildSignalItem(
                item,
                now,
                snapshot.UpdatedAt))
            .Where(x =>
                x.ValidationStatus == DepthCandidateValidationStatus.Valid.ToString())
            .Where(x =>
                x.Decision == SignalDecision.Alert.ToString() ||
                x.Decision == SignalDecision.Candidate.ToString())
            .OrderBy(x => x.DecisionRank)
            .ThenByDescending(x => x.NetEdgePct ?? decimal.MinValue)
            .ThenByDescending(x => x.GrossSpreadPct ?? decimal.MinValue)
            .Take(safeLimit)
            .Select(x => new
            {
                x.TradingPair,
                x.Direction,
                x.LongConnector,
                x.ShortConnector,

                x.Decision,
                x.DecisionReason,
                x.ValidationStatus,

                x.NotionalUsd,
                x.EstimatedProfitUsd,

                x.GrossSpreadPct,
                x.EstimatedFeesPct,
                x.NetEdgePct,

                x.BuyAveragePrice,
                x.SellAveragePrice,
                x.BuySlippagePct,
                x.SellSlippagePct,
                x.BuyLevelsUsed,
                x.SellLevelsUsed,
                x.IsBuyFullyFillable,
                x.IsSellFullyFillable,

                x.RequestedBaseAmount,
                x.BuyQuoteAmount,
                x.SellQuoteAmount,

                x.DetectedAt,
                x.ValidatedAt,
                x.SignalAgeMs
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

    [HttpGet("active/alerts")]
    public object Alerts(
        [FromQuery] int limit = 100)
    {
        var snapshot = _opportunityStore.Get();
        var now = DateTimeOffset.UtcNow;

        var safeLimit = Math.Clamp(limit, 1, 500);

        var items = snapshot.Items
            .Select(item => BuildSignalItem(
                item,
                now,
                snapshot.UpdatedAt))
            .Where(x =>
                x.ValidationStatus == DepthCandidateValidationStatus.Valid.ToString() &&
                x.Decision == SignalDecision.Alert.ToString())
            .OrderByDescending(x => x.NetEdgePct ?? decimal.MinValue)
            .ThenByDescending(x => x.GrossSpreadPct ?? decimal.MinValue)
            .Take(safeLimit)
            .Select(x => new
            {
                x.TradingPair,
                x.Direction,
                x.LongConnector,
                x.ShortConnector,

                x.Decision,
                x.DecisionReason,

                x.NotionalUsd,
                x.EstimatedProfitUsd,

                x.GrossSpreadPct,
                x.EstimatedFeesPct,
                x.NetEdgePct,

                x.BuyAveragePrice,
                x.SellAveragePrice,
                x.BuySlippagePct,
                x.SellSlippagePct,
                x.BuyLevelsUsed,
                x.SellLevelsUsed,

                x.DetectedAt,
                x.ValidatedAt,
                x.SignalAgeMs
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

    [HttpGet("diagnostics")]
    public object Diagnostics(
    [FromQuery] bool includeIgnored = true,
    [FromQuery] bool includeBlocked = true,
    [FromQuery] bool includeInvalid = true,
    [FromQuery] int limit = 200)
    {
        var snapshot = _opportunityStore.Get();
        var now = DateTimeOffset.UtcNow;

        var safeLimit = Math.Clamp(limit, 1, 1000);

        var items = snapshot.Items
            .Select(item => BuildSignalItem(
                item,
                now,
                snapshot.UpdatedAt))
            .Where(x =>
                includeInvalid ||
                x.ValidationStatus == DepthCandidateValidationStatus.Valid.ToString())
            .Where(x =>
                x.Decision == SignalDecision.Alert.ToString() ||
                x.Decision == SignalDecision.Candidate.ToString() ||
                includeIgnored && x.Decision == SignalDecision.Ignored.ToString() ||
                includeBlocked && x.Decision == SignalDecision.Blocked.ToString())
            .OrderBy(x => x.DecisionRank)
            .ThenByDescending(x => x.NetEdgePct ?? decimal.MinValue)
            .ThenByDescending(x => x.GrossSpreadPct ?? decimal.MinValue)
            .Take(safeLimit)
            .Select(x => new
            {
                x.TradingPair,
                x.Direction,
                x.LongConnector,
                x.ShortConnector,

                x.Decision,
                x.DecisionReason,
                x.ValidationStatus,
                x.ValidationReason,

                x.NotionalUsd,
                x.EstimatedProfitUsd,

                x.GrossSpreadPct,
                x.EstimatedFeesPct,
                x.NetEdgePct,

                x.BuyAveragePrice,
                x.SellAveragePrice,
                x.BuySlippagePct,
                x.SellSlippagePct,
                x.BuyLevelsUsed,
                x.SellLevelsUsed,
                x.IsBuyFullyFillable,
                x.IsSellFullyFillable,

                x.RequestedBaseAmount,
                x.BuyQuoteAmount,
                x.SellQuoteAmount,

                x.DetectedAt,
                x.ValidatedAt,
                x.SignalAgeMs
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
    [HttpGet("active/summary")]
    public object Summary()
    {
        var snapshot = _opportunityStore.Get();
        var now = DateTimeOffset.UtcNow;

        var items = snapshot.Items
            .Select(item => BuildSignalItem(
                item,
                now,
                snapshot.UpdatedAt))
            .ToList();

        return new
        {
            snapshot.UpdatedAt,
            HasData = snapshot.UpdatedAt is not null,

            Total = items.Count,

            Valid = items.Count(x =>
                x.ValidationStatus == DepthCandidateValidationStatus.Valid.ToString()),

            Alerts = items.Count(x =>
                x.ValidationStatus == DepthCandidateValidationStatus.Valid.ToString() &&
                x.Decision == SignalDecision.Alert.ToString()),

            Candidates = items.Count(x =>
                x.ValidationStatus == DepthCandidateValidationStatus.Valid.ToString() &&
                x.Decision == SignalDecision.Candidate.ToString()),

            Ignored = items.Count(x =>
                x.ValidationStatus == DepthCandidateValidationStatus.Valid.ToString() &&
                x.Decision == SignalDecision.Ignored.ToString()),

            Blocked = items.Count(x =>
                x.ValidationStatus == DepthCandidateValidationStatus.Valid.ToString() &&
                x.Decision == SignalDecision.Blocked.ToString()),

            Invalid = items.Count(x =>
                x.ValidationStatus != DepthCandidateValidationStatus.Valid.ToString()),

            BestAlert = items
                .Where(x =>
                    x.ValidationStatus == DepthCandidateValidationStatus.Valid.ToString() &&
                    x.Decision == SignalDecision.Alert.ToString())
                .OrderByDescending(x => x.NetEdgePct ?? decimal.MinValue)
                .Select(x => new
                {
                    x.TradingPair,
                    x.Direction,
                    x.NetEdgePct,
                    x.EstimatedProfitUsd,
                    x.DecisionReason
                })
                .FirstOrDefault(),

            BestCandidate = items
                .Where(x =>
                    x.ValidationStatus == DepthCandidateValidationStatus.Valid.ToString() &&
                    x.Decision == SignalDecision.Candidate.ToString())
                .OrderByDescending(x => x.NetEdgePct ?? decimal.MinValue)
                .Select(x => new
                {
                    x.TradingPair,
                    x.Direction,
                    x.NetEdgePct,
                    x.EstimatedProfitUsd,
                    x.DecisionReason
                })
                .FirstOrDefault()
        };
    }

    private SignalItem BuildSignalItem(
        ValidatedDepthCandidate item,
        DateTimeOffset now,
        DateTimeOffset? validatedAt)
    {
        var buyQuote = item.BuyQuote;
        var sellQuote = item.SellQuote;

        var qualityResult = item.Status == DepthCandidateValidationStatus.Valid
            ? _signalQualityService.Evaluate(new SignalQualityInput(
                TradingPair: item.Candidate.TradingPair,
                LongConnector: item.Candidate.LongConnector,
                ShortConnector: item.Candidate.ShortConnector,
                NetEdgePct: item.NetEdgePct,
                BuySlippagePct: buyQuote?.SlippagePct,
                SellSlippagePct: sellQuote?.SlippagePct,
                EpisodeSamples: null,
                EpisodeDuration: null))
            : new SignalQualityResult(
                SignalDecision.Ignored,
                "Validation status is not valid.");

        var estimatedProfitUsd = item.NetEdgePct is null
            ? (decimal?)null
            : item.NotionalUsd * item.NetEdgePct.Value / 100m;

        return new SignalItem(
            TradingPair: item.Candidate.TradingPair,
            Direction: $"Long {item.Candidate.LongConnector} / Short {item.Candidate.ShortConnector}",
            LongConnector: item.Candidate.LongConnector,
            ShortConnector: item.Candidate.ShortConnector,

            Decision: qualityResult.Decision.ToString(),
            DecisionRank: GetDecisionRank(qualityResult.Decision),
            DecisionReason: qualityResult.Reason,

            ValidationStatus: item.Status.ToString(),
            ValidationReason: item.Reason,

            NotionalUsd: item.NotionalUsd,
            EstimatedProfitUsd: estimatedProfitUsd,

            GrossSpreadPct: item.GrossSpreadPct,
            EstimatedFeesPct: item.EstimatedFeesPct,
            NetEdgePct: item.NetEdgePct,

            BuyAveragePrice: buyQuote?.AveragePrice,
            SellAveragePrice: sellQuote?.AveragePrice,
            BuySlippagePct: buyQuote?.SlippagePct,
            SellSlippagePct: sellQuote?.SlippagePct,
            BuyLevelsUsed: buyQuote?.LevelsUsed,
            SellLevelsUsed: sellQuote?.LevelsUsed,
            IsBuyFullyFillable: buyQuote?.IsFullyFillable,
            IsSellFullyFillable: sellQuote?.IsFullyFillable,

            RequestedBaseAmount: item.RequestedBaseAmount,
            BuyQuoteAmount: buyQuote?.QuoteAmount,
            SellQuoteAmount: sellQuote?.QuoteAmount,

            DetectedAt: item.Candidate.DetectedAt,
            ValidatedAt: validatedAt,
            SignalAgeMs: (now - item.Candidate.DetectedAt).TotalMilliseconds);
    }

    private static int GetDecisionRank(SignalDecision decision)
    {
        return decision switch
        {
            SignalDecision.Alert => 0,
            SignalDecision.Candidate => 1,
            SignalDecision.Blocked => 2,
            SignalDecision.Ignored => 3,
            _ => 99
        };
    }

    private sealed record SignalItem(
        string TradingPair,
        string Direction,
        string LongConnector,
        string ShortConnector,

        string Decision,
        int DecisionRank,
        string DecisionReason,

        string ValidationStatus,
        string? ValidationReason,

        decimal NotionalUsd,
        decimal? EstimatedProfitUsd,

        decimal? GrossSpreadPct,
        decimal? EstimatedFeesPct,
        decimal? NetEdgePct,

        decimal? BuyAveragePrice,
        decimal? SellAveragePrice,
        decimal? BuySlippagePct,
        decimal? SellSlippagePct,
        int? BuyLevelsUsed,
        int? SellLevelsUsed,
        bool? IsBuyFullyFillable,
        bool? IsSellFullyFillable,

        decimal RequestedBaseAmount,
        decimal? BuyQuoteAmount,
        decimal? SellQuoteAmount,

        DateTimeOffset DetectedAt,
        DateTimeOffset? ValidatedAt,
        double SignalAgeMs);
}