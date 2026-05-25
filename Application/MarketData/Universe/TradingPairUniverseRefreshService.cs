using System.Text.Json;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Application.MarketData.Universe;

public sealed class TradingPairUniverseRefreshService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly IEnumerable<ITradingPairDiscoveryClient> _clients;
    private readonly TradingPairUniverseOptions _options;
    private readonly IHostEnvironment _environment;
    private readonly ILogger<TradingPairUniverseRefreshService> _logger;

    public TradingPairUniverseRefreshService(
        IEnumerable<ITradingPairDiscoveryClient> clients,
        IOptions<TradingPairUniverseOptions> options,
        IHostEnvironment environment,
        ILogger<TradingPairUniverseRefreshService> logger)
    {
        _clients = clients;
        _options = options.Value;
        _environment = environment;
        _logger = logger;
    }

    public async Task<TradingPairUniverseFile> RefreshAsync(
        CancellationToken ct)
    {
        var enabledConnectors = _options.EnabledConnectors
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var clients = _clients
            .Where(x => enabledConnectors.Contains(x.ConnectorName))
            .ToList();

        if (clients.Count == 0)
        {
            throw new InvalidOperationException(
                "No trading pair discovery clients are enabled.");
        }

        var allPairs = new List<DiscoveredTradingPair>();
        var successCount = 0;

        foreach (var client in clients)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var pairs = await client.GetTradingPairsAsync(ct);

                if (pairs.Count == 0)
                {
                    _logger.LogWarning(
                        "Trading pair discovery returned zero pairs. Connector={Connector}",
                        client.ConnectorName);

                    continue;
                }

                allPairs.AddRange(pairs);
                successCount++;

                _logger.LogInformation(
                    "Discovered trading pairs. Connector={Connector}, Count={Count}",
                    client.ConnectorName,
                    pairs.Count);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Trading pair discovery failed. Connector={Connector}",
                    client.ConnectorName);
            }
        }

        if (successCount == 0 || allPairs.Count == 0)
        {
            throw new InvalidOperationException(
                "Trading pair discovery failed for all enabled connectors. Existing universe file will not be overwritten.");
        }

        var universe = new TradingPairUniverseFile(
            UpdatedAt: DateTimeOffset.UtcNow,
            Pairs: allPairs
                .GroupBy(x => x.TradingPair, StringComparer.OrdinalIgnoreCase)
                .Select(group => new TradingPairUniverseFileItem(
                    TradingPair: group.Key.ToUpperInvariant(),
                    SupportedConnectors: group
                        .Select(x => x.ConnectorName)
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .OrderBy(x => x)
                        .ToList()))
                .OrderBy(x => x.TradingPair)
                .ToList());

        await WriteUniverseFileAsync(universe, ct);

        _logger.LogInformation(
            "Trading pair universe file refreshed. Pairs={Pairs}, Path={Path}",
            universe.Pairs.Count,
            ResolveOutputPath());

        return universe;
    }

    private async Task WriteUniverseFileAsync(
        TradingPairUniverseFile universe,
        CancellationToken ct)
    {
        var outputPath = ResolveOutputPath();
        var directory = Path.GetDirectoryName(outputPath);

        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var tempPath = outputPath + ".tmp";

        var json = JsonSerializer.Serialize(
            universe,
            JsonOptions);

        await File.WriteAllTextAsync(
            tempPath,
            json,
            ct);

        File.Move(
            tempPath,
            outputPath,
            overwrite: true);
    }

    private string ResolveOutputPath()
    {
        return Path.IsPathRooted(_options.OutputPath)
            ? _options.OutputPath
            : Path.Combine(
                _environment.ContentRootPath,
                _options.OutputPath);
    }
}