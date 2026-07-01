namespace Arbitrage.Api.Application.Execution.Trading;

public sealed class ExchangeTradingClientRegistry
{
    private readonly IReadOnlyDictionary<string, IExchangeTradingClient> _clients;

    public ExchangeTradingClientRegistry(
        IEnumerable<IExchangeTradingClient> clients)
    {
        _clients = clients.ToDictionary(
            x => x.ConnectorName,
            StringComparer.OrdinalIgnoreCase);
    }

    public bool TryGetClient(
        string connectorName,
        out IExchangeTradingClient client)
    {
        return _clients.TryGetValue(
            connectorName,
            out client!);
    }
}