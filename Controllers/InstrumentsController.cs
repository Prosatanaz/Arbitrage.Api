using Arbitrage.Api.Application.Instruments;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Controllers;

[ApiController]
[Route("api/instruments")]
public sealed class InstrumentsController : ControllerBase
{
    private readonly InstrumentFilterOptions _options;
    private readonly InstrumentFilterService _instrumentFilter;

    public InstrumentsController(
        IOptions<InstrumentFilterOptions> options,
        InstrumentFilterService instrumentFilter)
    {
        _options = options.Value;
        _instrumentFilter = instrumentFilter;
    }

    [HttpGet("filter/config")]
    public object Config()
    {
        return new
        {
            _options.Enabled,
            _options.RequiredQuoteAsset,
            _options.RequireAlphanumericBaseAsset,
            _options.BlockedBaseAssets,
            _options.BlockedBaseAssetSuffixes,
            _options.BlockedTradingPairs,
            _options.AllowedBaseAssets,
            _options.AllowedTradingPairs
        };
    }

    [HttpGet("filter/check")]
    public object Check([FromQuery] string tradingPair)
    {
        var result = _instrumentFilter.EvaluateTradingPair(tradingPair);

        return new
        {
            result.TradingPair,
            result.BaseAsset,
            result.QuoteAsset,
            Decision = result.Decision.ToString(),
            result.Reason
        };
    }
}