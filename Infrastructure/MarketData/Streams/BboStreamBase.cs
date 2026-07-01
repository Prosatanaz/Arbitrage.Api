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

    public Task StartAsync(
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

        await SubscribeAsync(socket, AllowedSymbols.ToList(), ct);

        var tasks = new List<Task> { ReceiveLoopAsync(socket, ct) };

        var pingTask = RunPingLoopAsync(socket, ct);

        if (pingTask is not null)
            tasks.Add(pingTask);

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
