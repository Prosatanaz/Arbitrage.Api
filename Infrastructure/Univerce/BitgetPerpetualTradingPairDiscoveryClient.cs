using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Universe;
using Arbitrage.Api.Infrastructure.MarketData.Streams.Bitget;

namespace Arbitrage.Api.Infrastructure.Univerce;

public sealed class BitgetPerpetualTradingPairDiscoveryClient
    : ITradingPairDiscoveryClient
{
    private const string Url =
        "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES";

    private readonly HttpClient _httpClient;

    public string ConnectorName => "bitget_perpetual";

    public BitgetPerpetualTradingPairDiscoveryClient(
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

        var dto = await JsonSerializer.DeserializeAsync<BitgetContractsResponse>(
            stream,
            cancellationToken: ct);

        if (dto?.Data is null)
            return [];

        return dto.Data
            .Where(x =>
                string.Equals(x.SymbolStatus, "normal", StringComparison.OrdinalIgnoreCase) &&
                string.Equals(x.SymbolType, "perpetual", StringComparison.OrdinalIgnoreCase) &&
                string.Equals(x.QuoteCoin, "USDT", StringComparison.OrdinalIgnoreCase))
            .Select(x => new DiscoveredTradingPair(
                TradingPair: BitgetSymbolMapper.FromBitgetSymbol(x.Symbol),
                ConnectorName: ConnectorName))
            .Distinct()
            .OrderBy(x => x.TradingPair)
            .ToList();
    }

    private sealed class BitgetContractsResponse
    {
        [JsonPropertyName("code")]
        public string Code { get; init; } = "";

        [JsonPropertyName("msg")]
        public string Message { get; init; } = "";

        [JsonPropertyName("data")]
        public List<BitgetContract> Data { get; init; } = new();
    }

    private sealed class BitgetContract
    {
        [JsonPropertyName("symbol")]
        public string Symbol { get; init; } = "";

        [JsonPropertyName("baseCoin")]
        public string BaseCoin { get; init; } = "";

        [JsonPropertyName("quoteCoin")]
        public string QuoteCoin { get; init; } = "";

        [JsonPropertyName("symbolType")]
        public string SymbolType { get; init; } = "";

        [JsonPropertyName("symbolStatus")]
        public string SymbolStatus { get; init; } = "";
    }
}