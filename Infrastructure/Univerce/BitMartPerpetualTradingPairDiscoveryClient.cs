using System.Globalization;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Universe;
using Arbitrage.Api.Infrastructure.MarketData.Streams.BitMart;

namespace Arbitrage.Api.Infrastructure.MarketData.Universe;

public sealed class BitMartPerpetualTradingPairDiscoveryClient
    : ITradingPairDiscoveryClient
{
    private const string Url =
        "https://api-cloud-v2.bitmart.com/contract/public/details";

    private readonly HttpClient _httpClient;
    private readonly BitMartContractMetadataStore _metadataStore;
    private readonly ILogger<BitMartPerpetualTradingPairDiscoveryClient> _logger;

    public string ConnectorName => "bitmart_perpetual";

    public BitMartPerpetualTradingPairDiscoveryClient(
        HttpClient httpClient,
        BitMartContractMetadataStore metadataStore,
        ILogger<BitMartPerpetualTradingPairDiscoveryClient> logger)
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

        if (!TryGetPropertyIgnoreCase(document.RootElement, "data", out var dataElement) ||
            !TryGetPropertyIgnoreCase(dataElement, "symbols", out var symbolsElement) ||
            symbolsElement.ValueKind != JsonValueKind.Array)
        {
            _logger.LogWarning("BitMart contract details response does not contain data.symbols array.");
            return [];
        }

        var result = new List<DiscoveredTradingPair>();
        var metadataItems = new List<BitMartContractMetadata>();

        var total = 0;
        var skippedNotPerpetual = 0;
        var skippedNotUsdt = 0;
        var skippedDisabled = 0;
        var skippedMissingContractSize = 0;

        foreach (var symbolElement in symbolsElement.EnumerateArray())
        {
            total++;

            if (!TryGetStringProperty(symbolElement, "symbol", out var symbol))
                continue;

            if (TryGetIntProperty(symbolElement, "product_type", out var productType) &&
                productType != 1)
            {
                skippedNotPerpetual++;
                continue;
            }

            if (!TryGetStringProperty(symbolElement, "base_currency", out var baseCurrency))
                baseCurrency = symbol.EndsWith("USDT", StringComparison.OrdinalIgnoreCase)
                    ? symbol[..^4]
                    : "";

            if (!TryGetStringProperty(symbolElement, "quote_currency", out var quoteCurrency))
                quoteCurrency = "USDT";

            if (!string.Equals(quoteCurrency, "USDT", StringComparison.OrdinalIgnoreCase))
            {
                skippedNotUsdt++;
                continue;
            }

            if (!symbol.EndsWith("USDT", StringComparison.OrdinalIgnoreCase))
            {
                skippedNotUsdt++;
                continue;
            }

            if (TryGetStringProperty(symbolElement, "status", out var status) &&
                IsDisabledStatus(status))
            {
                skippedDisabled++;
                continue;
            }

            if (!TryGetDecimalProperty(symbolElement, "contract_size", out var contractSize) ||
                contractSize <= 0)
            {
                skippedMissingContractSize++;
                continue;
            }

            var tradingPair = !string.IsNullOrWhiteSpace(baseCurrency)
                ? BitMartSymbolMapper.FromAssets(baseCurrency, quoteCurrency)
                : BitMartSymbolMapper.FromBitMartSymbol(symbol);

            result.Add(new DiscoveredTradingPair(
                TradingPair: tradingPair,
                ConnectorName: ConnectorName));

            metadataItems.Add(new BitMartContractMetadata(
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
            "BitMart contracts parsed. Total={Total}, Pairs={Pairs}, Metadata={Metadata}, SkippedNotPerpetual={SkippedNotPerpetual}, SkippedNotUsdt={SkippedNotUsdt}, SkippedDisabled={SkippedDisabled}, SkippedMissingContractSize={SkippedMissingContractSize}",
            total,
            pairs.Count,
            metadataItems.Count,
            skippedNotPerpetual,
            skippedNotUsdt,
            skippedDisabled,
            skippedMissingContractSize);

        return pairs;
    }

    private static bool IsDisabledStatus(string status)
    {
        return status.Equals("Delisted", StringComparison.OrdinalIgnoreCase) ||
               status.Equals("Offline", StringComparison.OrdinalIgnoreCase) ||
               status.Equals("Suspend", StringComparison.OrdinalIgnoreCase) ||
               status.Equals("Suspended", StringComparison.OrdinalIgnoreCase);
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