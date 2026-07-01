using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Universe;
using Arbitrage.Api.Infrastructure.MarketData.Streams.BingX;

namespace Arbitrage.Api.Infrastructure.Univerce;

public sealed class BingXPerpetualTradingPairDiscoveryClient : ITradingPairDiscoveryClient
{
    private const string Connector = "bingx_perpetual";
    private const string RequestUri = "/openApi/swap/v2/quote/contracts";

    private readonly HttpClient _httpClient;
    private readonly ILogger<BingXPerpetualTradingPairDiscoveryClient> _logger;

    public string ConnectorName => Connector;

    public BingXPerpetualTradingPairDiscoveryClient(
        HttpClient httpClient,
        ILogger<BingXPerpetualTradingPairDiscoveryClient> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
    }

    public async Task<IReadOnlyList<DiscoveredTradingPair>> GetTradingPairsAsync(
        CancellationToken ct)
    {
        var response = await _httpClient.GetFromJsonAsync<BingXContractsResponse>(
            RequestUri,
            cancellationToken: ct);

        if (response is null)
        {
            _logger.LogWarning("BingX contracts response is null.");
            return [];
        }

        if (response.Code != 0)
        {
            _logger.LogWarning(
                "BingX contracts returned non-success code. Code={Code}, Message={Message}",
                response.Code,
                response.Message);

            return [];
        }

        var result = response.Data
            .Where(IsTradableUsdtPerpetual)
            .Select(x => new DiscoveredTradingPair(
                TradingPair: BingXSymbolMapper.FromBingXSymbol(x.Symbol),
                ConnectorName: Connector))
            .Distinct()
            .OrderBy(x => x.TradingPair)
            .ToList();

        _logger.LogInformation(
            "BingX perpetual trading pairs discovered. Count={Count}",
            result.Count);

        return result;
    }

    private static bool IsTradableUsdtPerpetual(BingXContractInfo item)
    {
        if (string.IsNullOrWhiteSpace(item.Symbol))
            return false;

        if (!item.Symbol.EndsWith("-USDT", StringComparison.OrdinalIgnoreCase))
            return false;

        // BingX contract responses commonly expose status as integer-like value.
        // 1 is online/trading in many BingX market metadata responses.
        // If status is absent, keep the pair and let runtime market data validate it.
        if (item.Status is not null && item.Status != 1)
            return false;

        return true;
    }

    private sealed class BingXContractsResponse
    {
        [JsonPropertyName("code")]
        public int Code { get; init; }

        [JsonPropertyName("msg")]
        public string? Message { get; init; }

        [JsonPropertyName("data")]
        public List<BingXContractInfo> Data { get; init; } = [];
    }

    private sealed class BingXContractInfo
    {
        [JsonPropertyName("symbol")]
        public string Symbol { get; init; } = "";

        [JsonPropertyName("status")]
        public int? Status { get; init; }
    }
}