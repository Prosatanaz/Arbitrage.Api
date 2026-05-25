using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Universe;
using Arbitrage.Api.Infrastructure.MarketData.Streams.KuCoin;

namespace Arbitrage.Api.Infrastructure.MarketData.Universe;

public sealed class KuCoinPerpetualTradingPairDiscoveryClient
    : ITradingPairDiscoveryClient
{
    private const string Url =
        "https://api-futures.kucoin.com/api/v1/contracts/active";

    private readonly HttpClient _httpClient;
    private readonly ILogger<KuCoinPerpetualTradingPairDiscoveryClient> _logger;

    public string ConnectorName => "kucoin_perpetual";

    public KuCoinPerpetualTradingPairDiscoveryClient(
        HttpClient httpClient,
        ILogger<KuCoinPerpetualTradingPairDiscoveryClient> logger)
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

        if (!TryGetPropertyIgnoreCase(
                document.RootElement,
                "data",
                out var dataElement))
        {
            _logger.LogWarning("KuCoin contracts response does not contain data property.");
            return [];
        }

        var contracts = dataElement.ValueKind == JsonValueKind.Array
            ? dataElement.EnumerateArray().ToList()
            : dataElement.ValueKind == JsonValueKind.Object
                ? [dataElement]
                : [];

        var result = new List<DiscoveredTradingPair>();

        foreach (var contract in contracts)
        {
            if (!TryGetStringProperty(contract, "symbol", out var symbol))
                continue;

            if (!TryGetStringProperty(contract, "quoteCurrency", out var quoteCurrency))
                continue;

            if (!TryGetStringProperty(contract, "settleCurrency", out var settleCurrency))
                continue;

            if (!TryGetStringProperty(contract, "status", out var status))
                status = "Open";

            if (!string.Equals(quoteCurrency, "USDT", StringComparison.OrdinalIgnoreCase))
                continue;

            if (!string.Equals(settleCurrency, "USDT", StringComparison.OrdinalIgnoreCase))
                continue;

            if (!string.Equals(status, "Open", StringComparison.OrdinalIgnoreCase))
                continue;

            var tradingPair = TryGetStringProperty(contract, "baseCurrency", out var baseCurrency)
                ? KuCoinSymbolMapper.FromKuCoinAssets(baseCurrency, quoteCurrency)
                : KuCoinSymbolMapper.FromKuCoinSymbol(symbol);

            result.Add(new DiscoveredTradingPair(
                TradingPair: tradingPair,
                ConnectorName: ConnectorName));
        }

        var pairs = result
            .Distinct()
            .OrderBy(x => x.TradingPair)
            .ToList();

        _logger.LogInformation(
            "KuCoin contracts parsed. Contracts={Contracts}, Pairs={Pairs}",
            contracts.Count,
            pairs.Count);

        return pairs;
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