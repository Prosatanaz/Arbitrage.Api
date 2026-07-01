using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Universe;
using Arbitrage.Api.Infrastructure.MarketData.Streams.Htx;

namespace Arbitrage.Api.Infrastructure.Univerce;

public sealed class HtxPerpetualTradingPairDiscoveryClient : ITradingPairDiscoveryClient
{
    private const string Connector = "htx_perpetual";
    private const string RequestUri = "/linear-swap-api/v1/swap_contract_info?business_type=all";

    private readonly HttpClient _httpClient;
    private readonly HtxContractMetadataStore _metadataStore;
    private readonly ILogger<HtxPerpetualTradingPairDiscoveryClient> _logger;

    public string ConnectorName => Connector;

    public HtxPerpetualTradingPairDiscoveryClient(
        HttpClient httpClient,
        HtxContractMetadataStore metadataStore,
        ILogger<HtxPerpetualTradingPairDiscoveryClient> logger)
    {
        _httpClient = httpClient;
        _metadataStore = metadataStore;
        _logger = logger;
    }

    public async Task<IReadOnlyList<DiscoveredTradingPair>> GetTradingPairsAsync(
        CancellationToken ct)
    {
        var response = await _httpClient.GetFromJsonAsync<HtxContractInfoResponse>(
            RequestUri,
            cancellationToken: ct);

        if (response is null)
        {
            _logger.LogWarning("HTX contract info response is null.");
            return [];
        }

        if (!string.Equals(response.Status, "ok", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning(
                "HTX contract info returned non-ok status. Status={Status}, ErrorCode={ErrorCode}, ErrorMessage={ErrorMessage}",
                response.Status,
                response.ErrorCode,
                response.ErrorMessage);

            return [];
        }

        var pairs = new List<DiscoveredTradingPair>();
        var metadata = new List<HtxContractMetadata>();

        var skippedNotUsdt = 0;
        var skippedDisabled = 0;
        var skippedWithoutContractSize = 0;
        var skippedWithoutContractCode = 0;

        foreach (var item in response.Data)
        {
            if (string.IsNullOrWhiteSpace(item.ContractCode))
            {
                skippedWithoutContractCode++;
                continue;
            }

            if (!item.ContractCode.EndsWith("-USDT", StringComparison.OrdinalIgnoreCase))
            {
                skippedNotUsdt++;
                continue;
            }

            if (item.ContractStatus is not null && item.ContractStatus != 1)
            {
                skippedDisabled++;
                continue;
            }

            if (!TryReadDecimal(item.ContractSize, out var contractSize) ||
                contractSize <= 0)
            {
                skippedWithoutContractSize++;
                continue;
            }

            var tradingPair = HtxSymbolMapper.FromHtxContractCode(item.ContractCode);

            pairs.Add(new DiscoveredTradingPair(
                TradingPair: tradingPair,
                ConnectorName: Connector));

            metadata.Add(new HtxContractMetadata(
                ContractCode: item.ContractCode.Trim().ToUpperInvariant(),
                TradingPair: tradingPair,
                ContractSize: contractSize));
        }

        _metadataStore.SetMany(metadata);

        var result = pairs
            .Distinct()
            .OrderBy(x => x.TradingPair)
            .ToList();

        _logger.LogInformation(
            "HTX contracts parsed. Total={Total}, Pairs={Pairs}, Metadata={Metadata}, SkippedNotUsdt={SkippedNotUsdt}, SkippedDisabled={SkippedDisabled}, SkippedWithoutContractSize={SkippedWithoutContractSize}, SkippedWithoutContractCode={SkippedWithoutContractCode}",
            response.Data.Count,
            result.Count,
            metadata.Count,
            skippedNotUsdt,
            skippedDisabled,
            skippedWithoutContractSize,
            skippedWithoutContractCode);

        return result;
    }

    private static bool TryReadDecimal(
        JsonElement element,
        out decimal value)
    {
        value = 0;

        if (element.ValueKind == JsonValueKind.Number)
            return element.TryGetDecimal(out value);

        if (element.ValueKind == JsonValueKind.String)
        {
            return decimal.TryParse(
                element.GetString(),
                NumberStyles.Number,
                CultureInfo.InvariantCulture,
                out value);
        }

        return false;
    }

    private sealed class HtxContractInfoResponse
    {
        [JsonPropertyName("status")]
        public string Status { get; init; } = "";

        [JsonPropertyName("err_code")]
        public int? ErrorCode { get; init; }

        [JsonPropertyName("err_msg")]
        public string? ErrorMessage { get; init; }

        [JsonPropertyName("data")]
        public List<HtxContractInfo> Data { get; init; } = [];
    }

    private sealed class HtxContractInfo
    {
        [JsonPropertyName("contract_code")]
        public string ContractCode { get; init; } = "";

        [JsonPropertyName("contract_status")]
        public int? ContractStatus { get; init; }

        [JsonPropertyName("contract_size")]
        public JsonElement ContractSize { get; init; }
    }
}