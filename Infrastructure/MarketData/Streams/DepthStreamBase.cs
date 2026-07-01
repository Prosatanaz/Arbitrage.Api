using System.Net.WebSockets;
using Arbitrage.Api.Application.MarketData.Depth;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams;

public abstract class DepthStreamBase : WsStreamBase, IOrderBookDepthStream
{
    private readonly DepthSubscriptionTargetStore _targetStore;
    private readonly OrderBookDepthCache _depthCache;
    private readonly ILogger _logger;

    private readonly HashSet<string> _subscribed = new(StringComparer.OrdinalIgnoreCase);

    protected DepthStreamBase(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        ILogger logger)
        : base(logger)
    {
        _targetStore = targetStore;
        _depthCache = depthCache;
        _logger = logger;
    }

    public abstract string ConnectorName { get; }

    protected sealed override string ConnectorDisplayName => ConnectorName;

    public Task StartAsync(CancellationToken ct) => RunReconnectLoopAsync(ct);

    protected abstract string MapTradingPairToExchangeSymbol(string tradingPair);

    protected abstract string MapExchangeSymbolToTradingPair(string exchangeSymbol);

    protected virtual Task<Uri> ResolveWebSocketUrlAsync(CancellationToken ct) =>
        Task.FromResult(new Uri(WsUrl));

    protected virtual string WsUrl =>
        throw new NotSupportedException(
            $"{GetType().Name} must override either WsUrl or ResolveWebSocketUrlAsync.");

    protected abstract Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbolsToAdd,
        CancellationToken ct);

    protected abstract Task UnsubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbolsToRemove,
        CancellationToken ct);

    protected abstract Task ProcessMessageAsync(
        ClientWebSocket socket,
        string json,
        CancellationToken ct);

    protected virtual Task? RunPingLoopAsync(ClientWebSocket socket, CancellationToken ct) => null;

    protected virtual void OnConnectionReset()
    {
    }

    protected int SubscribedSymbolCount => _subscribed.Count;

    protected bool IsSubscribed(string exchangeSymbol) =>
        _subscribed.Contains(exchangeSymbol);

    protected virtual void OnConnecting()
    {
        _logger.LogInformation("Connecting to {Connector} depth stream.", ConnectorName);
    }

    protected sealed override async Task RunConnectionAsync(CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _subscribed.Clear();
        OnConnectionReset();

        OnConnecting();

        var url = await ResolveWebSocketUrlAsync(ct);

        await socket.ConnectAsync(url, ct);

        _logger.LogInformation("Connected to {Connector} depth stream.", ConnectorName);

        var tasks = new List<Task>
        {
            ReceiveLoopAsync(socket, ct),
            SubscriptionLoopAsync(socket, ct)
        };

        var pingTask = RunPingLoopAsync(socket, ct);

        if (pingTask is not null)
            tasks.Add(pingTask);

        await Task.WhenAny(tasks);
    }

    private async Task SubscriptionLoopAsync(ClientWebSocket socket, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            var desired = _targetStore
                .GetPairsForConnector(ConnectorName)
                .Select(MapTradingPairToExchangeSymbol)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var (toAdd, toRemove) = DepthSubscriptionDiff.Compute(desired, _subscribed);

            if (toAdd.Count > 0)
            {
                foreach (var symbol in toAdd)
                    _subscribed.Add(symbol);

                await SubscribeAsync(socket, toAdd, ct);
            }

            if (toRemove.Count > 0)
            {
                foreach (var symbol in toRemove)
                    _subscribed.Remove(symbol);

                await UnsubscribeAsync(socket, toRemove, ct);

                foreach (var symbol in toRemove)
                {
                    _depthCache.Remove(
                        ConnectorName,
                        MapExchangeSymbolToTradingPair(symbol));
                }
            }

            await Task.Delay(1000, ct);
        }
    }

    private async Task ReceiveLoopAsync(ClientWebSocket socket, CancellationToken ct)
    {
        var buffer = new byte[ReceiveBufferSize];

        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            var message = await ReceiveTextAsync(socket, buffer, ct);

            if (message is null)
                break;

            await ProcessMessageAsync(socket, message, ct);
        }
    }
}
