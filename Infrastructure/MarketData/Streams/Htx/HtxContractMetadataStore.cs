namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Htx;

public sealed record HtxContractMetadata(
    string ContractCode,
    string TradingPair,
    decimal ContractSize);

public sealed class HtxContractMetadataStore
{
    private readonly object _lock = new();

    private readonly Dictionary<string, HtxContractMetadata> _byContractCode =
        new(StringComparer.OrdinalIgnoreCase);

    public void SetMany(IReadOnlyList<HtxContractMetadata> items)
    {
        lock (_lock)
        {
            foreach (var item in items)
            {
                if (string.IsNullOrWhiteSpace(item.ContractCode))
                    continue;

                _byContractCode[item.ContractCode] = item;
            }
        }
    }

    public bool TryGetByContractCode(
        string contractCode,
        out HtxContractMetadata metadata)
    {
        lock (_lock)
        {
            return _byContractCode.TryGetValue(contractCode, out metadata!);
        }
    }
}