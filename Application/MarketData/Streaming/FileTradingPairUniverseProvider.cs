using System.Text.Json;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Application.MarketData.Streaming;

public sealed class FileTradingPairUniverseProvider : ITradingPairUniverseProvider
{
    private readonly MarketDataStreamOptions _options;
    private readonly ILogger<FileTradingPairUniverseProvider> _logger;
    private readonly IHostEnvironment _environment;

    private readonly object _lock = new();

    private IReadOnlyList<TradingPairUniverseItem>? _cachedItems;
    private DateTimeOffset? _cachedFileWriteTime;

    public FileTradingPairUniverseProvider(
        IOptions<MarketDataStreamOptions> options,
        ILogger<FileTradingPairUniverseProvider> logger,
        IHostEnvironment environment)
    {
        _options = options.Value;
        _logger = logger;
        _environment = environment;
    }

    public async Task<IReadOnlyList<string>> GetTradingPairsAsync(
        CancellationToken ct)
    {
        var items = await GetItemsAsync(ct);

        return items
            .Select(x => x.TradingPair)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();
    }

    public async Task<IReadOnlyList<string>> GetTradingPairsForConnectorAsync(
        string connectorName,
        CancellationToken ct)
    {
        var items = await GetItemsAsync(ct);

        return items
            .Where(x =>
                x.SupportedConnectors.Count == 0 ||
                x.SupportedConnectors.Contains(
                    connectorName,
                    StringComparer.OrdinalIgnoreCase))
            .Select(x => x.TradingPair)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();
    }

    private async Task<IReadOnlyList<TradingPairUniverseItem>> GetItemsAsync(
        CancellationToken ct)
    {
        var path = ResolvePath();

        if (!File.Exists(path))
        {
            throw new FileNotFoundException(
                $"Discovered pairs file was not found: {path}");
        }

        var currentWriteTime = File.GetLastWriteTimeUtc(path);

        lock (_lock)
        {
            if (_cachedItems is not null &&
                _cachedFileWriteTime == currentWriteTime)
            {
                return _cachedItems;
            }
        }

        await using var stream = File.OpenRead(path);

        using var document = await JsonDocument.ParseAsync(
            stream,
            cancellationToken: ct);

        var items = ReadUniverseItems(document.RootElement)
            .Where(x => !string.IsNullOrWhiteSpace(x.TradingPair))
            .GroupBy(x => x.TradingPair, StringComparer.OrdinalIgnoreCase)
            .Select(group => MergeItems(group.Key, group))
            .OrderBy(x => x.TradingPair)
            .ToList();

        if (items.Count == 0)
        {
            throw new InvalidOperationException(
                $"Trading pair universe file contains no readable pairs. Path={path}");
        }

        lock (_lock)
        {
            _cachedItems = items;
            _cachedFileWriteTime = currentWriteTime;
        }

        _logger.LogInformation(
            "Loaded trading pair universe. Count={Count}, Path={Path}, LastWriteTimeUtc={LastWriteTimeUtc}",
            items.Count,
            path,
            currentWriteTime);

        return items;
    }

    private IEnumerable<TradingPairUniverseItem> ReadUniverseItems(
        JsonElement root)
    {
        if (root.ValueKind == JsonValueKind.Object)
        {
            if (!TryGetPropertyIgnoreCase(root, "pairs", out var pairsElement))
            {
                _logger.LogWarning(
                    "Trading pair universe root object does not contain 'pairs' property. Raw={Raw}",
                    root.ToString());

                yield break;
            }

            foreach (var item in ReadPairsArray(pairsElement))
            {
                yield return item;
            }

            yield break;
        }

        if (root.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in ReadPairsArray(root))
            {
                yield return item;
            }

            yield break;
        }

        _logger.LogWarning(
            "Unsupported trading pair universe JSON root kind. Kind={Kind}",
            root.ValueKind);
    }

    private IEnumerable<TradingPairUniverseItem> ReadPairsArray(
        JsonElement pairsElement)
    {
        if (pairsElement.ValueKind != JsonValueKind.Array)
        {
            _logger.LogWarning(
                "Trading pair universe 'pairs' value is not an array. Kind={Kind}",
                pairsElement.ValueKind);

            yield break;
        }

        foreach (var element in pairsElement.EnumerateArray())
        {
            if (element.ValueKind == JsonValueKind.String)
            {
                var tradingPair = element.GetString();

                if (!string.IsNullOrWhiteSpace(tradingPair))
                {
                    yield return new TradingPairUniverseItem(
                        TradingPair: tradingPair.ToUpperInvariant(),
                        SupportedConnectors: Array.Empty<string>());
                }

                continue;
            }

            if (element.ValueKind == JsonValueKind.Object)
            {
                var item = ReadObjectItem(element);

                if (item is not null)
                {
                    yield return item;
                }

                continue;
            }

            _logger.LogDebug(
                "Skipping unsupported trading pair universe item. Kind={Kind}",
                element.ValueKind);
        }
    }

    private TradingPairUniverseItem? ReadObjectItem(
        JsonElement element)
    {
        if (!TryGetPropertyIgnoreCase(element, "tradingPair", out var tradingPairElement))
        {
            _logger.LogDebug(
                "Skipping universe item without tradingPair property. Raw={Raw}",
                element.ToString());

            return null;
        }

        var tradingPair = tradingPairElement.GetString();

        if (string.IsNullOrWhiteSpace(tradingPair))
        {
            return null;
        }

        var supportedConnectors = ReadSupportedConnectors(element);

        return new TradingPairUniverseItem(
            TradingPair: tradingPair.ToUpperInvariant(),
            SupportedConnectors: supportedConnectors);
    }

    private static IReadOnlyList<string> ReadSupportedConnectors(
        JsonElement element)
    {
        if (!TryGetPropertyIgnoreCase(
                element,
                "supportedConnectors",
                out var connectorsElement))
        {
            return Array.Empty<string>();
        }

        if (connectorsElement.ValueKind != JsonValueKind.Array)
        {
            return Array.Empty<string>();
        }

        return connectorsElement
            .EnumerateArray()
            .Where(x => x.ValueKind == JsonValueKind.String)
            .Select(x => x.GetString())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();
    }

    private static bool TryGetPropertyIgnoreCase(
        JsonElement element,
        string propertyName,
        out JsonElement value)
    {
        if (element.TryGetProperty(propertyName, out value))
        {
            return true;
        }

        foreach (var property in element.EnumerateObject())
        {
            if (property.Name.Equals(
                    propertyName,
                    StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }

    private static TradingPairUniverseItem MergeItems(
        string tradingPair,
        IEnumerable<TradingPairUniverseItem> items)
    {
        var supportedConnectors = items
            .SelectMany(x => x.SupportedConnectors)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        return new TradingPairUniverseItem(
            TradingPair: tradingPair.ToUpperInvariant(),
            SupportedConnectors: supportedConnectors);
    }

    private string ResolvePath()
    {
        return Path.IsPathRooted(_options.DiscoveredPairsPath)
            ? _options.DiscoveredPairsPath
            : Path.Combine(
                _environment.ContentRootPath,
                _options.DiscoveredPairsPath);
    }

    private sealed record TradingPairUniverseItem(
        string TradingPair,
        IReadOnlyList<string> SupportedConnectors);
}