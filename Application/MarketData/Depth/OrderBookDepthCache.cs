using System.Collections.Concurrent;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Application.MarketData.Depth;

public sealed class OrderBookDepthCache
{
    private readonly ConcurrentDictionary<DepthSubscriptionKey, OrderBookDepthSnapshot> _items = new();

    public void Set(OrderBookDepthSnapshot snapshot)
    {
        var key = new DepthSubscriptionKey(
            snapshot.ConnectorName,
            snapshot.TradingPair);

        _items[key] = snapshot;
    }

    public bool TryGet(
        string connectorName,
        string tradingPair,
        out OrderBookDepthSnapshot? snapshot)
    {
        return _items.TryGetValue(
            new DepthSubscriptionKey(connectorName, tradingPair),
            out snapshot);
    }

    public void Remove(
        string connectorName,
        string tradingPair)
    {
        _items.TryRemove(
            new DepthSubscriptionKey(connectorName, tradingPair),
            out _);
    }

    public IReadOnlyDictionary<DepthSubscriptionKey, OrderBookDepthSnapshot> GetAll()
    {
        return _items;
    }

    public IReadOnlyList<OrderBookDepthSnapshot> GetByTradingPair(
        string tradingPair)
    {
        return _items
            .Where(x => x.Key.TradingPair.Equals(
                tradingPair,
                StringComparison.OrdinalIgnoreCase))
            .Select(x => x.Value)
            .ToList();
    }
}