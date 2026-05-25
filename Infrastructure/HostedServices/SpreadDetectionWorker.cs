using Arbitrage.Api.Application.MarketData.Streaming;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.HostedServices;

public sealed class SpreadDetectionWorker : BackgroundService
{
    private readonly ITradingPairUniverseProvider _universeProvider;
    private readonly SpreadDetector _spreadDetector;
    private readonly LatestSpreadCandidateStore _store;
    private readonly SpreadDetectorOptions _options;
    private readonly ILogger<SpreadDetectionWorker> _logger;

    public SpreadDetectionWorker(
        ITradingPairUniverseProvider universeProvider,
        SpreadDetector spreadDetector,
        LatestSpreadCandidateStore store,
        IOptions<SpreadDetectorOptions> options,
        ILogger<SpreadDetectionWorker> logger)
    {
        _universeProvider = universeProvider;
        _spreadDetector = spreadDetector;
        _store = store;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Spread detector is disabled.");
            return;
        }

        IReadOnlyList<string> tradingPairs;

        try
        {
            tradingPairs = await _universeProvider.GetTradingPairsAsync(stoppingToken);
        }
        catch (FileNotFoundException ex)
        {
            _logger.LogError(
                ex,
                "Trading pair universe file was not found. Spread detector will not start.");

            return;
        }

        if (tradingPairs.Count == 0)
        {
            _logger.LogWarning("Trading pair universe is empty. Spread detector will not start.");
            return;
        }

        _logger.LogInformation(
            "Spread detector started. Pairs={Pairs}, IntervalMs={IntervalMs}, MinGrossSpreadPct={MinGrossSpreadPct}, MaxSnapshotAgeMs={MaxSnapshotAgeMs}",
            tradingPairs.Count,
            _options.IntervalMs,
            _options.MinGrossSpreadPct,
            _options.MaxSnapshotAgeMs);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var candidates = _spreadDetector
                    .Detect(
                        tradingPairs,
                        TimeSpan.FromMilliseconds(_options.MaxSnapshotAgeMs),
                        _options.MinGrossSpreadPct)
                    .Take(_options.MaxCandidatesToKeep)
                    .ToList();

                _store.Set(candidates);

                if (candidates.Count > 0)
                {
                    var best = candidates[0];

                    _logger.LogInformation(
                        "Spread candidates detected. CurrentTickCount={CurrentTickCount}, StoreCount={StoreCount}, Best={BestPair} {BestSpreadPct}%",
                        candidates.Count,
                        _store.Count(),
                        best.TradingPair,
                        best.GrossSpreadPct);
                }
                else
                {
                    _logger.LogDebug(
                        "No spread candidates detected. StoreCount={StoreCount}",
                        _store.Count());
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
                    "Spread detection iteration failed.");
            }

            await Task.Delay(_options.IntervalMs, stoppingToken);
        }
    }
}