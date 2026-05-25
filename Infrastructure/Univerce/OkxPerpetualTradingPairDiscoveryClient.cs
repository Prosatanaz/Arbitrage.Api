using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Universe;
using Arbitrage.Api.Infrastructure.MarketData.Streams.Okx;

namespace Arbitrage.Api.Infrastructure.MarketData.Universe;

public sealed class OkxPerpetualTradingPairDiscoveryClient
    : ITradingPairDiscoveryClient
{
    private const string Url =
        "https://www.okx.com/api/v5/public/instruments?instType=SWAP";

    private readonly HttpClient _httpClient;

    public string ConnectorName => "okx_perpetual";

    public OkxPerpetualTradingPairDiscoveryClient(
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

        var dto = await JsonSerializer.DeserializeAsync<OkxInstrumentsResponse>(
            stream,
            cancellationToken: ct);

        if (dto?.Data is null)
            return [];

        return dto.Data
            .Where(x =>
                string.Equals(x.State, "live", StringComparison.OrdinalIgnoreCase) &&
                string.Equals(x.SettleCurrency, "USDT", StringComparison.OrdinalIgnoreCase) &&
                x.InstrumentId.EndsWith("-SWAP", StringComparison.OrdinalIgnoreCase))
            .Select(x => new DiscoveredTradingPair(
                TradingPair: OkxSymbolMapper.FromOkxInstrumentId(x.InstrumentId),
                ConnectorName: ConnectorName))
            .Distinct()
            .OrderBy(x => x.TradingPair)
            .ToList();
    }

    private sealed class OkxInstrumentsResponse
    {
        [JsonPropertyName("data")]
        public List<OkxInstrument> Data { get; init; } = new();
    }

    private sealed class OkxInstrument
    {
        [JsonPropertyName("instId")]
        public string InstrumentId { get; init; } = "";

        [JsonPropertyName("state")]
        public string State { get; init; } = "";

        [JsonPropertyName("settleCcy")]
        public string SettleCurrency { get; init; } = "";
    }
}