using System.Globalization;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Universe;
using Arbitrage.Api.Infrastructure.MarketData.Streams.Mexc;

namespace Arbitrage.Api.Infrastructure.MarketData.Universe;

public sealed class MexcPerpetualTradingPairDiscoveryClient
    : ITradingPairDiscoveryClient
{
    private const string Url =
        "https://api.mexc.com/api/v1/contract/detail/country";

    private readonly HttpClient _httpClient;
    private readonly MexcContractMetadataStore _metadataStore;
    private readonly ILogger<MexcPerpetualTradingPairDiscoveryClient> _logger;

    public string ConnectorName => "mexc_perpetual";

    public MexcPerpetualTradingPairDiscoveryClient(
        HttpClient httpClient,
        MexcContractMetadataStore metadataStore,
        ILogger<MexcPerpetualTradingPairDiscoveryClient> logger)
    {
        _httpClient = httpClient;
        _metadataStore = metadataStore;
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

        if (!TryGetPropertyIgnoreCase(
                document.RootElement,
                "data",
                out var dataElement))
        {
            _logger.LogWarning("MEXC contract detail response does not contain data property.");
            return [];
        }

        var contracts = ReadContracts(dataElement);

        var result = new List<DiscoveredTradingPair>();
        var metadataItems = new List<MexcContractMetadata>();

        var skippedNotPerpetual = 0;
        var skippedNotUsdt = 0;
        var skippedDisabled = 0;
        var skippedWithoutSymbol = 0;
        var skippedWithoutContractSize = 0;

        foreach (var contract in contracts)
        {
            if (!TryGetStringProperty(contract, "symbol", out var symbol))
            {
                skippedWithoutSymbol++;
                continue;
            }

            if (!symbol.EndsWith("_USDT", StringComparison.OrdinalIgnoreCase))
            {
                skippedNotUsdt++;
                continue;
            }

            if (TryGetIntProperty(contract, "futureType", out var futureType) &&
                futureType != 1)
            {
                skippedNotPerpetual++;
                continue;
            }

            if (TryGetStringProperty(contract, "quoteCoin", out var quoteCoin) &&
                !string.Equals(quoteCoin, "USDT", StringComparison.OrdinalIgnoreCase))
            {
                skippedNotUsdt++;
                continue;
            }

            if (TryGetStringProperty(contract, "settleCoin", out var settleCoin) &&
                !string.Equals(settleCoin, "USDT", StringComparison.OrdinalIgnoreCase))
            {
                skippedNotUsdt++;
                continue;
            }

            if (TryGetIntProperty(contract, "state", out var state) &&
                state != 0)
            {
                skippedDisabled++;
                continue;
            }

            if (TryGetBoolProperty(contract, "apiAllowed", out var apiAllowed) &&
                !apiAllowed)
            {
                skippedDisabled++;
                continue;
            }

            if (TryGetBoolProperty(contract, "isHidden", out var isHidden) &&
                isHidden)
            {
                skippedDisabled++;
                continue;
            }

            if (!TryGetDecimalProperty(contract, "contractSize", out var contractSize) ||
                contractSize <= 0)
            {
                skippedWithoutContractSize++;
                continue;
            }

            var tradingPair = MexcSymbolMapper.FromMexcSymbol(symbol);

            result.Add(new DiscoveredTradingPair(
                TradingPair: tradingPair,
                ConnectorName: ConnectorName));

            metadataItems.Add(new MexcContractMetadata(
                Symbol: symbol.ToUpperInvariant(),
                TradingPair: tradingPair,
                ContractSize: contractSize));
        }

        _metadataStore.SetMany(metadataItems);

        var pairs = result
            .Distinct()
            .OrderBy(x => x.TradingPair)
            .ToList();

        _logger.LogInformation(
            "MEXC contracts parsed. Contracts={Contracts}, Pairs={Pairs}, Metadata={Metadata}, SkippedNotPerpetual={SkippedNotPerpetual}, SkippedNotUsdt={SkippedNotUsdt}, SkippedDisabled={SkippedDisabled}, SkippedWithoutSymbol={SkippedWithoutSymbol}, SkippedWithoutContractSize={SkippedWithoutContractSize}",
            contracts.Count,
            pairs.Count,
            metadataItems.Count,
            skippedNotPerpetual,
            skippedNotUsdt,
            skippedDisabled,
            skippedWithoutSymbol,
            skippedWithoutContractSize);

        return pairs;
    }

    private static IReadOnlyList<JsonElement> ReadContracts(
        JsonElement dataElement)
    {
        if (dataElement.ValueKind == JsonValueKind.Array)
        {
            return dataElement.EnumerateArray().ToList();
        }

        if (dataElement.ValueKind == JsonValueKind.Object)
        {
            return [dataElement];
        }

        return [];
    }

    private static bool TryGetPropertyIgnoreCase(
        JsonElement element,
        string propertyName,
        out JsonElement value)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            value = default;
            return false;
        }

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

    private static bool TryGetStringProperty(
        JsonElement element,
        string propertyName,
        out string value)
    {
        value = "";

        if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
            return false;

        if (property.ValueKind != JsonValueKind.String)
            return false;

        value = property.GetString() ?? "";
        return !string.IsNullOrWhiteSpace(value);
    }

    private static bool TryGetIntProperty(
        JsonElement element,
        string propertyName,
        out int value)
    {
        value = 0;

        if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
            return false;

        if (property.ValueKind == JsonValueKind.Number)
            return property.TryGetInt32(out value);

        if (property.ValueKind == JsonValueKind.String)
        {
            return int.TryParse(
                property.GetString(),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out value);
        }

        return false;
    }

    private static bool TryGetBoolProperty(
        JsonElement element,
        string propertyName,
        out bool value)
    {
        value = false;

        if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
            return false;

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

        if (property.ValueKind == JsonValueKind.String)
        {
            return bool.TryParse(property.GetString(), out value);
        }

        return false;
    }

    private static bool TryGetDecimalProperty(
        JsonElement element,
        string propertyName,
        out decimal value)
    {
        value = 0;

        if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
            return false;

        if (property.ValueKind == JsonValueKind.Number)
            return property.TryGetDecimal(out value);

        if (property.ValueKind == JsonValueKind.String)
        {
            return decimal.TryParse(
                property.GetString(),
                NumberStyles.Number,
                CultureInfo.InvariantCulture,
                out value);
        }

        return false;
    }
}