using Arbitrage.Api.Application.Instruments;
using Arbitrage.Api.Application.SignalQuality;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Controllers;

[ApiController]
[Route("api/signal-quality")]
public sealed class SignalQualityController : ControllerBase
{
    private readonly SignalQualityOptions _signalQualityOptions;
    private readonly InstrumentFilterOptions _instrumentFilterOptions;
    private readonly SignalQualityService _signalQualityService;
    private readonly InstrumentFilterService _instrumentFilterService;

    public SignalQualityController(
        IOptions<SignalQualityOptions> signalQualityOptions,
        IOptions<InstrumentFilterOptions> instrumentFilterOptions,
        SignalQualityService signalQualityService,
        InstrumentFilterService instrumentFilterService)
    {
        _signalQualityOptions = signalQualityOptions.Value;
        _instrumentFilterOptions = instrumentFilterOptions.Value;
        _signalQualityService = signalQualityService;
        _instrumentFilterService = instrumentFilterService;
    }

    [HttpGet("config")]
    public object Config()
    {
        return new
        {
            SignalQuality = new
            {
                _signalQualityOptions.Enabled,
                _signalQualityOptions.MinCandidateNetEdgePct,
                _signalQualityOptions.MinAlertNetEdgePct,
                _signalQualityOptions.MinEpisodeSamples,
                _signalQualityOptions.MinEpisodeDurationSeconds,
                _signalQualityOptions.EpisodeMaxGapSeconds,
                _signalQualityOptions.MaxBuySlippagePct,
                _signalQualityOptions.MaxSellSlippagePct,
                _signalQualityOptions.BlockedConnectors,
                _signalQualityOptions.AllowedTradingPairs,
                _signalQualityOptions.BlockedTradingPairs
            },
            InstrumentFilter = new
            {
                _instrumentFilterOptions.Enabled,
                _instrumentFilterOptions.RequiredQuoteAsset,
                _instrumentFilterOptions.RequireAlphanumericBaseAsset,
                _instrumentFilterOptions.BlockedBaseAssets,
                _instrumentFilterOptions.BlockedBaseAssetSuffixes,
                _instrumentFilterOptions.BlockedTradingPairs,
                _instrumentFilterOptions.AllowedBaseAssets,
                _instrumentFilterOptions.AllowedTradingPairs
            }
        };
    }

    [HttpGet("check")]
    public object Check(
        [FromQuery] string tradingPair,
        [FromQuery] string longConnector,
        [FromQuery] string shortConnector,
        [FromQuery] decimal? netEdgePct,
        [FromQuery] decimal? buySlippagePct = null,
        [FromQuery] decimal? sellSlippagePct = null,
        [FromQuery] int? episodeSamples = null,
        [FromQuery] int? episodeDurationSeconds = null)
    {
        var instrumentResult = _instrumentFilterService.EvaluateTradingPair(tradingPair);

        var signalResult = _signalQualityService.Evaluate(new SignalQualityInput(
            TradingPair: tradingPair,
            LongConnector: longConnector,
            ShortConnector: shortConnector,
            NetEdgePct: netEdgePct,
            BuySlippagePct: buySlippagePct,
            SellSlippagePct: sellSlippagePct,
            EpisodeSamples: episodeSamples,
            EpisodeDuration: episodeDurationSeconds is null
                ? null
                : TimeSpan.FromSeconds(episodeDurationSeconds.Value)));

        return new
        {
            TradingPair = InstrumentFilterService.NormalizeTradingPair(tradingPair),
            instrumentResult.BaseAsset,
            instrumentResult.QuoteAsset,
            InstrumentDecision = instrumentResult.Decision.ToString(),
            InstrumentReason = instrumentResult.Reason,
            LongConnector = longConnector,
            ShortConnector = shortConnector,
            NetEdgePct = netEdgePct,
            SignalDecision = signalResult.Decision.ToString(),
            SignalReason = signalResult.Reason
        };
    }
}