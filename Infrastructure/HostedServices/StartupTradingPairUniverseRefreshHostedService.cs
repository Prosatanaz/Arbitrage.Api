using Arbitrage.Api.Application.MarketData.Universe;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.HostedServices;

public sealed class StartupTradingPairUniverseRefreshHostedService : IHostedService
{
    private readonly TradingPairUniverseRefreshService _refreshService;
    private readonly TradingPairUniverseOptions _options;
    private readonly IWebHostEnvironment _environment;
    private readonly ILogger<StartupTradingPairUniverseRefreshHostedService> _logger;

    public StartupTradingPairUniverseRefreshHostedService(
        TradingPairUniverseRefreshService refreshService,
        IOptions<TradingPairUniverseOptions> options,
        IWebHostEnvironment environment,
        ILogger<StartupTradingPairUniverseRefreshHostedService> logger)
    {
        _refreshService = refreshService;
        _options = options.Value;
        _environment = environment;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Trading pair universe startup refresh is disabled because universe updater is disabled.");
            return;
        }

        if (!_options.RefreshOnStartup)
        {
            _logger.LogInformation("Trading pair universe startup refresh is disabled.");
            return;
        }

        _logger.LogInformation("Refreshing trading pair universe before market data workers start...");

        try
        {
            var universe = await _refreshService.RefreshAsync(cancellationToken);

            _logger.LogInformation(
                "Trading pair universe startup refresh completed. Pairs={Pairs}",
                universe.Pairs.Count);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            var fallbackPath = ResolveOutputPath();

            if (File.Exists(fallbackPath))
            {
                _logger.LogWarning(
                    ex,
                    "Trading pair universe startup refresh failed. Existing file will be used. Path={Path}",
                    fallbackPath);

                return;
            }

            _logger.LogError(
                ex,
                "Trading pair universe startup refresh failed and no fallback file exists. Path={Path}",
                fallbackPath);

            throw;
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    private string ResolveOutputPath()
    {
        return Path.IsPathRooted(_options.OutputPath)
            ? _options.OutputPath
            : Path.Combine(_environment.ContentRootPath, _options.OutputPath);
    }
}