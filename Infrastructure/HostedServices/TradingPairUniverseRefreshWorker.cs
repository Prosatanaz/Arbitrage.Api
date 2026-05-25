using Arbitrage.Api.Application.MarketData.Universe;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.HostedServices;

public sealed class TradingPairUniverseRefreshWorker : BackgroundService
{
    private readonly TradingPairUniverseRefreshService _refreshService;
    private readonly TradingPairUniverseOptions _options;
    private readonly ILogger<TradingPairUniverseRefreshWorker> _logger;

    public TradingPairUniverseRefreshWorker(
        TradingPairUniverseRefreshService refreshService,
        IOptions<TradingPairUniverseOptions> options,
        ILogger<TradingPairUniverseRefreshWorker> logger)
    {
        _refreshService = refreshService;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Trading pair universe refresh worker is disabled.");
            return;
        }

        _logger.LogInformation(
            "Trading pair universe periodic refresh worker started. RefreshIntervalMinutes={RefreshIntervalMinutes}",
            _options.RefreshIntervalMinutes);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(
                    TimeSpan.FromMinutes(_options.RefreshIntervalMinutes),
                    stoppingToken);

                await _refreshService.RefreshAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Trading pair universe periodic refresh failed.");
            }
        }
    }
}