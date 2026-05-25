namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Mexc;

public sealed record MexcContractMetadata(
    string Symbol,
    string TradingPair,
    decimal ContractSize);

public sealed class MexcContractMetadataStore
{
    private readonly object _lock = new();

    private readonly Dictionary<string, MexcContractMetadata> _bySymbol =
        new(StringComparer.OrdinalIgnoreCase);

    public void SetMany(IReadOnlyList<MexcContractMetadata> items)
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
        out MexcContractMetadata metadata)
    {
        lock (_lock)
        {
            return _bySymbol.TryGetValue(symbol, out metadata!);
        }
    }
}