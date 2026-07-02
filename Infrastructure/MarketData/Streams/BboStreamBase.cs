using System.Net.WebSockets;
using Arbitrage.Api.Application.MarketData.Streaming;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams;

public abstract class BboStreamBase : WsStreamBase, IBestBidAskStream
{
    private readonly ILogger _logger;

    protected BboStreamBase(ILogger logger) : base(logger)
    {
        _logger = logger;
    }

    public abstract string ConnectorName { get; }

    protected sealed override string ConnectorDisplayName => ConnectorName;

    internal HashSet<string> AllowedSymbols { get; set; } =
        new(StringComparer.OrdinalIgnoreCase);

    public virtual Task StartAsync(
        IReadOnlyList<string> tradingPairs,
        CancellationToken ct)
    {
        AllowedSymbols = tradingPairs
            .Select(MapTradingPairToExchangeSymbol)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return RunReconnectLoopAsync(ct);
    }

    protected abstract string MapTradingPairToExchangeSymbol(string tradingPair);

    protected virtual Task<Uri> ResolveWebSocketUrlAsync(CancellationToken ct) =>
        Task.FromResult(new Uri(WsUrl));

    protected virtual string WsUrl =>
        throw new NotSupportedException(
            $"{GetType().Name} must override either WsUrl or ResolveWebSocketUrlAsync.");

    protected abstract Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbols,
        CancellationToken ct);

    protected abstract Task ProcessMessageAsync(
        ClientWebSocket socket,
        string json,
        CancellationToken ct);

    protected virtual Task? RunPingLoopAsync(ClientWebSocket socket, CancellationToken ct) => null;

    // When true, SubscribeAsync runs concurrently with the receive/ping loops
    // instead of being awaited before they start - lets ticks for
    // already-subscribed symbols flow in while later batches are still being
    // sent, instead of holding the connection idle until subscribing finishes.
    protected virtual bool SubscribeConcurrently => false;

    protected virtual void OnConnecting()
    {
        _logger.LogInformation("Connecting to {Connector} BBO stream.", ConnectorName);
    }

    protected sealed override async Task RunConnectionAsync(CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        OnConnecting();

        var url = await ResolveWebSocketUrlAsync(ct);

        await socket.ConnectAsync(url, ct);

        _logger.LogInformation(
            "Connected to {Connector} BBO stream. AllowedSymbols={Count}",
            ConnectorName,
            AllowedSymbols.Count);

        if (!SubscribeConcurrently)
            await SubscribeAsync(socket, AllowedSymbols.ToList(), ct);

        var tasks = new List<Task> { ReceiveLoopAsync(socket, ct) };

        var pingTask = RunPingLoopAsync(socket, ct);

        if (pingTask is not null)
            tasks.Add(pingTask);

        if (SubscribeConcurrently)
        {
            var subscriptionTask = SubscribeAsync(socket, AllowedSymbols.ToList(), ct);

            while (!ct.IsCancellationRequested)
            {
                var allTasks = subscriptionTask is null
                    ? tasks
                    : tasks.Append(subscriptionTask).ToList();

                var completed = await Task.WhenAny(allTasks);

                if (completed == subscriptionTask)
                {
                    await subscriptionTask;
                    subscriptionTask = null;
                    continue;
                }

                await completed;
                return;
            }

            return;
        }

        await Task.WhenAny(tasks);
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
