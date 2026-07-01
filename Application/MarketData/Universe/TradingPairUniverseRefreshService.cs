using System.Text.Json;
using Arbitrage.Api.Application.Instruments;
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
    private readonly InstrumentFilterService _instrumentFilter;
    private readonly ILogger<TradingPairUniverseRefreshService> _logger;

    public TradingPairUniverseRefreshService(
        IEnumerable<ITradingPairDiscoveryClient> clients,
        IOptions<TradingPairUniverseOptions> options,
        IHostEnvironment environment,
        InstrumentFilterService instrumentFilter,
        ILogger<TradingPairUniverseRefreshService> logger)
    {
        _clients = clients;
        _options = options.Value;
        _environment = environment;
        _instrumentFilter = instrumentFilter;
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
        var succeededConnectors = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var failedConnectors = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var zeroPairConnectors = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var client in clients)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var pairs = await client.GetTradingPairsAsync(ct);

                if (pairs.Count == 0)
                {
                    zeroPairConnectors.Add(client.ConnectorName);

                    _logger.LogWarning(
                        "Trading pair discovery returned zero pairs. Connector={Connector}",
                        client.ConnectorName);

                    continue;
                }

                allPairs.AddRange(pairs);
                succeededConnectors.Add(client.ConnectorName);

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
                failedConnectors.Add(client.ConnectorName);

                _logger.LogWarning(
                    ex,
                    "Trading pair discovery failed. Connector={Connector}",
                    client.ConnectorName);
            }
        }

        if (succeededConnectors.Count == 0 || allPairs.Count == 0)
        {
            throw new InvalidOperationException(
                "Trading pair discovery failed for all enabled connectors. Existing universe file will not be overwritten.");
        }

        if (_options.RequireAllEnabledConnectors)
        {
            var missingConnectors = enabledConnectors
                .Except(succeededConnectors, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (missingConnectors.Count > 0)
            {
                throw new InvalidOperationException(
                    $"Trading pair discovery did not succeed for all enabled connectors. Missing={string.Join(", ", missingConnectors)}");
            }
        }

        if (_options.TreatZeroPairsAsFailure && zeroPairConnectors.Count > 0)
        {
            throw new InvalidOperationException(
                $"Trading pair discovery returned zero pairs for connectors. Connectors={string.Join(", ", zeroPairConnectors)}");
        }

        var filteredPairs = new List<DiscoveredTradingPair>();
        var blockedByReason = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var pair in allPairs)
        {
            var result = _instrumentFilter.EvaluateTradingPair(pair.TradingPair);

            if (result.Decision == InstrumentFilterDecision.Allowed)
            {
                filteredPairs.Add(new DiscoveredTradingPair(
                    result.TradingPair,
                    pair.ConnectorName));

                continue;
            }

            blockedByReason.TryGetValue(result.Reason, out var count);
            blockedByReason[result.Reason] = count + 1;
        }

        if (filteredPairs.Count == 0)
        {
            throw new InvalidOperationException(
                "Instrument filter removed all discovered trading pairs. Existing universe file will not be overwritten.");
        }

        foreach (var item in blockedByReason.OrderByDescending(x => x.Value))
        {
            _logger.LogInformation(
                "Trading pair universe filter blocked pairs. Reason={Reason}, Count={Count}",
                item.Key,
                item.Value);
        }

        var universe = new TradingPairUniverseFile(
            UpdatedAt: DateTimeOffset.UtcNow,
            Pairs: filteredPairs
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
            "Trading pair universe file refreshed. RawPairs={RawPairs}, FilteredPairs={FilteredPairs}, OutputPairs={OutputPairs}, SucceededConnectors={SucceededConnectors}, Path={Path}",
            allPairs.Count,
            filteredPairs.Count,
            universe.Pairs.Count,
            string.Join(", ", succeededConnectors.OrderBy(x => x)),
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