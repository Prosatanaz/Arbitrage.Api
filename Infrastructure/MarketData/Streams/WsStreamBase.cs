using System.Net.WebSockets;
using System.Text;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams;

public abstract class WsStreamBase
{
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly ILogger _logger;

    protected WsStreamBase(ILogger logger)
    {
        _logger = logger;
    }

    protected abstract string ConnectorDisplayName { get; }

    protected virtual int ReceiveBufferSize => 1024 * 256;

    protected async Task RunReconnectLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await RunConnectionAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "{Connector} stream crashed. Reconnecting in 5 seconds...",
                    ConnectorDisplayName);

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    protected abstract Task RunConnectionAsync(CancellationToken ct);

    protected virtual bool TryDecompress(
        byte[] buffer,
        int count,
        WebSocketMessageType messageType,
        out string text)
    {
        text = Encoding.UTF8.GetString(buffer, 0, count);
        return true;
    }

    protected async Task<string?> ReceiveTextAsync(
        ClientWebSocket socket,
        byte[] buffer,
        CancellationToken ct)
    {
        using var memory = new MemoryStream();

        WebSocketMessageType? messageType = null;

        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, ct);

            if (result.MessageType == WebSocketMessageType.Close)
                return null;

            messageType ??= result.MessageType;

            memory.Write(buffer, 0, result.Count);

            if (result.EndOfMessage)
                break;
        }

        // GetBuffer() avoids the extra array copy ToArray() would incur; the
        // buffer's capacity can exceed Length, so callers must respect count.
        var bytes = memory.GetBuffer();
        var count = (int)memory.Length;

        return TryDecompress(bytes, count, messageType!.Value, out var text)
            ? text
            : Encoding.UTF8.GetString(bytes, 0, count);
    }

    protected async Task SendTextAsync(
        ClientWebSocket socket,
        string payload,
        CancellationToken ct)
    {
        await _sendLock.WaitAsync(ct);

        try
        {
            if (socket.State != WebSocketState.Open)
                return;

            var bytes = Encoding.UTF8.GetBytes(payload);

            await socket.SendAsync(
                bytes,
                WebSocketMessageType.Text,
                endOfMessage: true,
                cancellationToken: ct);
        }
        finally
        {
            _sendLock.Release();
        }
    }
}
