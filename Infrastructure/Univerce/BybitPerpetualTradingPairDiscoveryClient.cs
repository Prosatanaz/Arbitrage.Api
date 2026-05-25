using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Universe;
using Arbitrage.Api.Infrastructure.MarketData.Streams.Bybit;

namespace Arbitrage.Api.Infrastructure.Univerce;

public sealed class BybitPerpetualTradingPairDiscoveryClient
    : ITradingPairDiscoveryClient
{
    private const string BaseUrl =
        "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000";

    private readonly HttpClient _httpClient;

    public string ConnectorName => "bybit_perpetual";

    public BybitPerpetualTradingPairDiscoveryClient(
        HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public async Task<IReadOnlyList<DiscoveredTradingPair>> GetTradingPairsAsync(
        CancellationToken ct)
    {
        var result = new List<DiscoveredTradingPair>();
        string? cursor = null;

        do
        {
            var url = string.IsNullOrWhiteSpace(cursor)
                ? BaseUrl
                : $"{BaseUrl}&cursor={Uri.EscapeDataString(cursor)}";

            using var response = await _httpClient.GetAsync(url, ct);

            response.EnsureSuccessStatusCode();

            await using var stream = await response.Content.ReadAsStreamAsync(ct);

            var dto = await JsonSerializer.DeserializeAsync<BybitInstrumentsResponse>(
                stream,
                cancellationToken: ct);

            var instruments = dto?.Result?.List ?? [];

            result.AddRange(instruments
                .Where(x =>
                    string.Equals(x.Status, "Trading", StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(x.QuoteCoin, "USDT", StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(x.ContractType, "LinearPerpetual", StringComparison.OrdinalIgnoreCase))
                .Select(x => new DiscoveredTradingPair(
                    TradingPair: BybitSymbolMapper.FromBybitSymbol(x.Symbol),
                    ConnectorName: ConnectorName)));

            cursor = dto?.Result?.NextPageCursor;

        } while (!string.IsNullOrWhiteSpace(cursor));

        return result
            .Distinct()
            .OrderBy(x => x.TradingPair)
            .ToList();
    }

    private sealed class BybitInstrumentsResponse
    {
        [JsonPropertyName("result")]
        public BybitInstrumentsResult? Result { get; init; }
    }

    private sealed class BybitInstrumentsResult
    {
        [JsonPropertyName("list")]
        public List<BybitInstrument> List { get; init; } = new();

        [JsonPropertyName("nextPageCursor")]
        public string? NextPageCursor { get; init; }
    }

    private sealed class BybitInstrument
    {
        [JsonPropertyName("symbol")]
        public string Symbol { get; init; } = "";

        [JsonPropertyName("status")]
        public string Status { get; init; } = "";

        [JsonPropertyName("quoteCoin")]
        public string QuoteCoin { get; init; } = "";

        [JsonPropertyName("contractType")]
        public string ContractType { get; init; } = "";
    }
}