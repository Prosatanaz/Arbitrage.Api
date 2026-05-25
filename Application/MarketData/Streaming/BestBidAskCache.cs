using System.Collections.Concurrent;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Application.MarketData.Streaming;

public sealed class BestBidAskCache
{
    private readonly ConcurrentDictionary<BestBidAskKey, BestBidAskSnapshot> _items = new();

    public void Set(BestBidAskSnapshot snapshot)
    {
        var key = new BestBidAskKey(
            snapshot.ConnectorName,
            snapshot.TradingPair);

        _items[key] = snapshot;
    }

    public bool TryGet(
        string connectorName,
        string tradingPair,
        out BestBidAskSnapshot? snapshot)
    {
        return _items.TryGetValue(
            new BestBidAskKey(connectorName, tradingPair),
            out snapshot);
    }

    public IReadOnlyList<BestBidAskSnapshot> GetByTradingPair(
        string tradingPair)
    {
        return _items
            .Where(x => x.Key.TradingPair.Equals(
                tradingPair,
                StringComparison.OrdinalIgnoreCase))
            .Select(x => x.Value)
            .ToList();
    }

    public IReadOnlyDictionary<BestBidAskKey, BestBidAskSnapshot> GetAll()
    {
        return _items;
    }
}