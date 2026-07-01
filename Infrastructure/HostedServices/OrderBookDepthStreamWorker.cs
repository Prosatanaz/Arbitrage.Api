using Arbitrage.Api.Application.MarketData.Depth;

namespace Arbitrage.Api.Infrastructure.HostedServices;

public sealed class OrderBookDepthStreamWorker : BackgroundService
{
    private readonly IEnumerable<IOrderBookDepthStream> _streams;
    private readonly DepthTrackingOptions _options;
    private readonly ILogger<OrderBookDepthStreamWorker> _logger;

    public OrderBookDepthStreamWorker(
        IEnumerable<IOrderBookDepthStream> streams,
        Microsoft.Extensions.Options.IOptions<DepthTrackingOptions> options,
        ILogger<OrderBookDepthStreamWorker> logger)
    {
        _streams = streams;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Order book depth streams are disabled.");
            return;
        }

        var enabledConnectors = _options.EnabledConnectors
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var streams = _streams
            .Where(x => enabledConnectors.Contains(x.ConnectorName))
            .ToList();

        if (streams.Count == 0)
        {
            _logger.LogWarning("No enabled order book depth streams were registered.");
            return;
        }

        _logger.LogInformation(
            "Starting order book depth streams. Streams={Count}",
            streams.Count);

        var tasks = streams
            .Select(x => x.StartAsync(stoppingToken))
            .ToList();

        await Task.WhenAll(tasks);
    }
}