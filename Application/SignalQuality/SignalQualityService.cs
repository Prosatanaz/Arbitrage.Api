using Arbitrage.Api.Application.Instruments;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Application.SignalQuality;

public sealed class SignalQualityService
{
    private readonly SignalQualityOptions _options;
    private readonly InstrumentFilterService _instrumentFilter;

    public SignalQualityService(
        IOptions<SignalQualityOptions> options,
        InstrumentFilterService instrumentFilter)
    {
        _options = options.Value;
        _instrumentFilter = instrumentFilter;
    }

    public SignalQualityResult Evaluate(
        SignalQualityInput input)
    {
        if (!_options.Enabled)
        {
            return new SignalQualityResult(
                SignalDecision.Ignored,
                "Signal quality evaluation is disabled.");
        }

        if (string.IsNullOrWhiteSpace(input.TradingPair))
        {
            return new SignalQualityResult(
                SignalDecision.Blocked,
                "Trading pair is empty.");
        }

        var instrumentFilterResult = _instrumentFilter.EvaluateTradingPair(input.TradingPair);

        if (instrumentFilterResult.Decision == InstrumentFilterDecision.Blocked)
        {
            return new SignalQualityResult(
                SignalDecision.Blocked,
                instrumentFilterResult.Reason);
        }

        if (IsBlockedTradingPair(input.TradingPair))
        {
            return new SignalQualityResult(
                SignalDecision.Blocked,
                "Trading pair is blocked by signal quality.");
        }

        if (IsBlockedConnector(input.LongConnector))
        {
            return new SignalQualityResult(
                SignalDecision.Blocked,
                "Long connector is blocked.");
        }

        if (IsBlockedConnector(input.ShortConnector))
        {
            return new SignalQualityResult(
                SignalDecision.Blocked,
                "Short connector is blocked.");
        }

        if (_options.AllowedTradingPairs.Length > 0 &&
            !IsAllowedTradingPair(input.TradingPair))
        {
            return new SignalQualityResult(
                SignalDecision.Ignored,
                "Trading pair is not in signal allowlist.");
        }

        if (input.NetEdgePct is null)
        {
            return new SignalQualityResult(
                SignalDecision.Ignored,
                "Net edge is missing.");
        }

        if (input.NetEdgePct.Value < _options.MinCandidateNetEdgePct)
        {
            return new SignalQualityResult(
                SignalDecision.Ignored,
                "Net edge is below candidate threshold.");
        }

        if (input.BuySlippagePct is not null &&
            input.BuySlippagePct.Value > _options.MaxBuySlippagePct)
        {
            return new SignalQualityResult(
                SignalDecision.Ignored,
                "Buy slippage is too high.");
        }

        if (input.SellSlippagePct is not null &&
            input.SellSlippagePct.Value > _options.MaxSellSlippagePct)
        {
            return new SignalQualityResult(
                SignalDecision.Ignored,
                "Sell slippage is too high.");
        }

        if (input.EpisodeSamples is not null &&
            input.EpisodeSamples.Value < _options.MinEpisodeSamples)
        {
            return new SignalQualityResult(
                SignalDecision.Ignored,
                "Episode does not have enough samples.");
        }

        if (input.EpisodeDuration is not null &&
            input.EpisodeDuration.Value < TimeSpan.FromSeconds(_options.MinEpisodeDurationSeconds))
        {
            return new SignalQualityResult(
                SignalDecision.Ignored,
                "Episode duration is too short.");
        }

        if (input.NetEdgePct.Value >= _options.MinAlertNetEdgePct)
        {
            return new SignalQualityResult(
                SignalDecision.Alert,
                "Signal passed alert threshold.");
        }

        return new SignalQualityResult(
            SignalDecision.Candidate,
            "Signal passed candidate threshold.");
    }

    public static string ExtractBaseAsset(string tradingPair)
    {
        return InstrumentFilterService.ExtractBaseAsset(tradingPair);
    }

    private bool IsBlockedConnector(string connectorName)
    {
        return _options.BlockedConnectors.Contains(
            connectorName,
            StringComparer.OrdinalIgnoreCase);
    }

    private bool IsBlockedTradingPair(string tradingPair)
    {
        return _options.BlockedTradingPairs.Contains(
            tradingPair,
            StringComparer.OrdinalIgnoreCase);
    }

    private bool IsAllowedTradingPair(string tradingPair)
    {
        return _options.AllowedTradingPairs.Contains(
            tradingPair,
            StringComparer.OrdinalIgnoreCase);
    }
}