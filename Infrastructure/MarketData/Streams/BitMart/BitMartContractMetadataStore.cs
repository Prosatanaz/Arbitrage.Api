namespace Arbitrage.Api.Infrastructure.MarketData.Streams.BitMart;

public sealed record BitMartContractMetadata(
    string Symbol,
    string TradingPair,
    decimal ContractSize);

public sealed class BitMartContractMetadataStore
{
    private readonly object _lock = new();

    private readonly Dictionary<string, BitMartContractMetadata> _bySymbol =
        new(StringComparer.OrdinalIgnoreCase);

    public void SetMany(IReadOnlyList<BitMartContractMetadata> items)
    {
        lock (_lock)
        {
            foreach (var item in items)
            {
                if (string.IsNullOrWhiteSpace(item.Symbol))
                    continue;

                _bySymbol[item.Symbol] = item;
            }
        }
    }

    public bool TryGetBySymbol(
        string symbol,
        out BitMartContractMetadata metadata)
    {
        lock (_lock)
        {
            return _bySymbol.TryGetValue(symbol, out metadata!);
        }
    }
}