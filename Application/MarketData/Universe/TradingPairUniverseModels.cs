using System.Text.Json.Serialization;

namespace Arbitrage.Api.Application.MarketData.Universe;

public sealed record DiscoveredTradingPair(
    string TradingPair,
    string ConnectorName);

public sealed record TradingPairUniverseFile(
    DateTimeOffset UpdatedAt,
    IReadOnlyList<TradingPairUniverseFileItem> Pairs);

public sealed record TradingPairUniverseFileItem(
    string TradingPair,
    IReadOnlyList<string> SupportedConnectors);