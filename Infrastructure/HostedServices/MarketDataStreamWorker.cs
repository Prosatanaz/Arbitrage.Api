using Arbitrage.Api.Application.MarketData.Streaming;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.HostedServices;

public sealed class MarketDataStreamWorker : BackgroundService
{
    private readonly IEnumerable<IBestBidAskStream> _streams;
    private readonly ITradingPairUniverseProvider _universeProvider;
    private readonly MarketDataStreamOptions _options;
    private readonly ILogger<MarketDataStreamWorker> _logger;

    public MarketDataStreamWorker(
        IEnumerable<IBestBidAskStream> streams,
        ITradingPairUniverseProvider universeProvider,
        IOptions<MarketDataStreamOptions> options,
        ILogger<MarketDataStreamWorker> logger)
    {
        _streams = streams;
        _universeProvider = universeProvider;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Market data streams are disabled.");
            return;
        }

        var enabledConnectors = _options.EnabledConnectors
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var streams = _streams
            .Where(x => enabledConnectors.Contains(x.ConnectorName))
            .ToList();

        if (streams.Count == 0)
        {
            _logger.LogWarning("No enabled market data streams were registered.");
            return;
        }

        var tasks = new List<Task>();

        foreach (var stream in streams)
        {
            IReadOnlyList<string> connectorPairs;

            try
            {
                connectorPairs =
                    await _universeProvider.GetTradingPairsForConnectorAsync(
                        stream.ConnectorName,
                        stoppingToken);
            }
            catch (FileNotFoundException ex)
            {
                _logger.LogError(
                    ex,
                    "Trading pair universe file was not found. Market data streams will not start.");

                return;
            }

            if (connectorPairs.Count == 0)
            {
                _logger.LogWarning(
                    "No trading pairs found for connector {Connector}. Stream will not start.",
                    stream.ConnectorName);

                continue;
            }

            _logger.LogInformation(
                "Starting market data stream. Connector={Connector}, Pairs={Pairs}",
                stream.ConnectorName,
                connectorPairs.Count);

            tasks.Add(stream.StartAsync(connectorPairs, stoppingToken));
        }

        if (tasks.Count == 0)
        {
            _logger.LogWarning("No market data stream tasks were started.");
            return;
        }

        await Task.WhenAll(tasks);
    }
}