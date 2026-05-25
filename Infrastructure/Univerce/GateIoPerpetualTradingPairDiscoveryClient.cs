using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Universe;
using Arbitrage.Api.Infrastructure.MarketData.Streams.GateIo;

namespace Arbitrage.Api.Infrastructure.MarketData.Universe;

public sealed class GateIoPerpetualTradingPairDiscoveryClient
    : ITradingPairDiscoveryClient
{
    private const string Url =
        "https://api.gateio.ws/api/v4/futures/usdt/contracts";

    private readonly HttpClient _httpClient;
    private readonly ILogger<GateIoPerpetualTradingPairDiscoveryClient> _logger;

    public string ConnectorName => "gate_io_perpetual";

    public GateIoPerpetualTradingPairDiscoveryClient(
        HttpClient httpClient,
        ILogger<GateIoPerpetualTradingPairDiscoveryClient> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
    }

    public async Task<IReadOnlyList<DiscoveredTradingPair>> GetTradingPairsAsync(
        CancellationToken ct)
    {
        using var response = await _httpClient.GetAsync(Url, ct);

        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync(ct);

        using var document = await JsonDocument.ParseAsync(
            stream,
            cancellationToken: ct);

        if (document.RootElement.ValueKind != JsonValueKind.Array)
        {
            _logger.LogWarning(
                "Unexpected Gate.io contracts response root kind. Kind={Kind}",
                document.RootElement.ValueKind);

            return [];
        }

        var result = new List<DiscoveredTradingPair>();
        var total = 0;
        var skippedWithoutName = 0;
        var skippedNotUsdt = 0;
        var skippedDelisting = 0;

        foreach (var contractElement in document.RootElement.EnumerateArray())
        {
            total++;

            if (!TryGetStringProperty(
                    contractElement,
                    "name",
                    out var contractName))
            {
                skippedWithoutName++;
                continue;
            }

            if (!contractName.EndsWith(
                    "_USDT",
                    StringComparison.OrdinalIgnoreCase))
            {
                skippedNotUsdt++;
                continue;
            }

            if (IsDelisting(contractElement))
            {
                skippedDelisting++;
                continue;
            }

            result.Add(new DiscoveredTradingPair(
                TradingPair: GateIoSymbolMapper.FromGateIoContract(contractName),
                ConnectorName: ConnectorName));
        }

        var pairs = result
            .Distinct()
            .OrderBy(x => x.TradingPair)
            .ToList();

        _logger.LogInformation(
            "Gate.io contracts parsed. Total={Total}, Pairs={Pairs}, SkippedWithoutName={SkippedWithoutName}, SkippedNotUsdt={SkippedNotUsdt}, SkippedDelisting={SkippedDelisting}",
            total,
            pairs.Count,
            skippedWithoutName,
            skippedNotUsdt,
            skippedDelisting);

        if (pairs.Count == 0)
        {
            LogSampleContracts(document.RootElement);
        }

        return pairs;
    }

    private void LogSampleContracts(JsonElement root)
    {
        var samples = root
            .EnumerateArray()
            .Take(3)
            .Select(x => x.ToString())
            .ToList();

        _logger.LogWarning(
            "Gate.io discovery returned zero pairs. SampleContracts={SampleContracts}",
            string.Join(" | ", samples));
    }

    private static bool IsDelisting(JsonElement element)
    {
        if (TryGetBoolProperty(element, "in_delisting", out var inDelisting))
            return inDelisting;

        if (TryGetBoolProperty(element, "inDelisting", out inDelisting))
            return inDelisting;

        // Some APIs encode booleans as strings.
        if (TryGetStringProperty(element, "in_delisting", out var inDelistingText) &&
            bool.TryParse(inDelistingText, out inDelisting))
        {
            return inDelisting;
        }

        if (TryGetStringProperty(element, "inDelisting", out inDelistingText) &&
            bool.TryParse(inDelistingText, out inDelisting))
        {
            return inDelisting;
        }

        // If field is absent, do not reject the contract.
        return false;
    }

    private static bool TryGetStringProperty(
        JsonElement element,
        string propertyName,
        out string value)
    {
        value = "";

        if (!TryGetPropertyIgnoreCase(
                element,
                propertyName,
                out var property))
        {
            return false;
        }

        if (property.ValueKind == JsonValueKind.String)
        {
            value = property.GetString() ?? "";
            return !string.IsNullOrWhiteSpace(value);
        }

        return false;
    }

    private static bool TryGetBoolProperty(
        JsonElement element,
        string propertyName,
        out bool value)
    {
        value = false;

        if (!TryGetPropertyIgnoreCase(
                element,
                propertyName,
                out var property))
        {
            return false;
        }

        if (property.ValueKind == JsonValueKind.True)
        {
            value = true;
            return true;
        }

        if (property.ValueKind == JsonValueKind.False)
        {
            value = false;
            return true;
        }

        return false;
    }

    private static bool TryGetPropertyIgnoreCase(
        JsonElement element,
        string propertyName,
        out JsonElement value)
    {
        if (element.TryGetProperty(propertyName, out value))
            return true;

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
}