namespace Arbitrage.Api.Application.MarketData.Depth;

public interface IOrderBookDepthStream
{
    string ConnectorName { get; }

    Task StartAsync(CancellationToken ct);
}