using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Universe;

namespace Arbitrage.Api.Infrastructure.Univerce;

public sealed class BinancePerpetualTradingPairDiscoveryClient
    : ITradingPairDiscoveryClient
{
    private const string Url = "https://fapi.binance.com/fapi/v1/exchangeInfo";

    private readonly HttpClient _httpClient;

    public string ConnectorName => "binance_perpetual";

    public BinancePerpetualTradingPairDiscoveryClient(
        HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public async Task<IReadOnlyList<DiscoveredTradingPair>> GetTradingPairsAsync(
        CancellationToken ct)
    {
        using var response = await _httpClient.GetAsync(Url, ct);

        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync(ct);

        var dto = await JsonSerializer.DeserializeAsync<BinanceExchangeInfoResponse>(
            stream,
            cancellationToken: ct);

        if (dto?.Symbols is null)
            return [];

        return dto.Symbols
            .Where(x =>
                string.Equals(x.Status, "TRADING", StringComparison.OrdinalIgnoreCase) &&
                string.Equals(x.ContractType, "PERPETUAL", StringComparison.OrdinalIgnoreCase) &&
                string.Equals(x.QuoteAsset, "USDT", StringComparison.OrdinalIgnoreCase))
            .Select(x => new DiscoveredTradingPair(
                TradingPair: ToTradingPair(x.BaseAsset, x.QuoteAsset),
                ConnectorName: ConnectorName))
            .Distinct()
            .OrderBy(x => x.TradingPair)
            .ToList();
    }

    private static string ToTradingPair(
        string baseAsset,
        string quoteAsset)
    {
        return $"{baseAsset.ToUpperInvariant()}-{quoteAsset.ToUpperInvariant()}";
    }

    private sealed class BinanceExchangeInfoResponse
    {
        [JsonPropertyName("symbols")]
        public List<BinanceSymbol> Symbols { get; init; } = new();
    }

    private sealed class BinanceSymbol
    {
        [JsonPropertyName("status")]
        public string Status { get; init; } = "";

        [JsonPropertyName("contractType")]
        public string ContractType { get; init; } = "";

        [JsonPropertyName("baseAsset")]
        public string BaseAsset { get; init; } = "";

        [JsonPropertyName("quoteAsset")]
        public string QuoteAsset { get; init; } = "";
    }
}