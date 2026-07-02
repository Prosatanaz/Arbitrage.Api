using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Mexc;

public sealed class MexcBboStream : BboStreamBase
{
    private const string Connector = "mexc_perpetual";
    private const string Channel = "push.tickers";

    private readonly BestBidAskCache _cache;
    private readonly ILogger<MexcBboStream> _logger;

    private int _successfulUpdatesLogged;

    public MexcBboStream(
        BestBidAskCache cache,
        ILogger<MexcBboStream> logger)
        : base(logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://contract.mexc.com/edge";

    protected override int ReceiveBufferSize => 1024 * 512;

    protected override bool SubscribeConcurrently => true;

    protected override void OnConnecting()
    {
        _successfulUpdatesLogged = 0;
        base.OnConnecting();
    }

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        MexcSymbolMapper.ToMexcSymbol(tradingPair);

    protected override async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbols,
        CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(new
        {
            method = "sub.tickers",
            param = new { },
            gzip = false
        });

        await SendTextAsync(socket, payload, ct);

        _logger.LogInformation("MEXC BBO subscription completed.");
    }

    protected override Task? RunPingLoopAsync(ClientWebSocket socket, CancellationToken ct) =>
        PingLoopAsync(socket, ct);

    private async Task PingLoopAsync(ClientWebSocket socket, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            await Task.Delay(TimeSpan.FromSeconds(15), ct);

            if (socket.State != WebSocketState.Open)
                break;

            var payload = JsonSerializer.Serialize(new
            {
                method = "ping"
            });

            await SendTextAsync(socket, payload, ct);
        }
    }

    protected override async Task ProcessMessageAsync(
        ClientWebSocket socket,
        string json,
        CancellationToken ct)
    {
        try
        {
            ProcessMessage(json);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to parse MEXC BBO message. Raw={Raw}",
                json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to process MEXC BBO message. Raw={Raw}",
                json);
        }

        await Task.CompletedTask;
    }

    private void ProcessMessage(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (!TryGetStringProperty(root, "channel", out var channel))
            return;

        if (string.Equals(channel, "pong", StringComparison.OrdinalIgnoreCase))
            return;

        if (!string.Equals(channel, Channel, StringComparison.OrdinalIgnoreCase))
            return;

        if (!TryGetPropertyIgnoreCase(root, "data", out var dataElement))
            return;

        if (dataElement.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in dataElement.EnumerateArray())
            {
                if (item.ValueKind == JsonValueKind.Object)
                    ProcessTicker(item);
            }

            return;
        }

        if (dataElement.ValueKind == JsonValueKind.Object)
            ProcessTicker(dataElement);
    }

    private void ProcessTicker(JsonElement item)
    {
        if (!TryGetStringProperty(item, "symbol", out var symbol))
            return;

        if (!AllowedSymbols.Contains(symbol))
            return;

        if (!TryGetDecimalProperty(item, "maxBidPrice", out var bidPrice))
            return;

        if (!TryGetDecimalProperty(item, "minAskPrice", out var askPrice))
            return;

        if (bidPrice <= 0 || askPrice <= 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = TryGetLongProperty(item, "timestamp", out var timestamp) &&
                                timestamp > 0
            ? ConvertTimestamp(timestamp)
            : receivedAt;

        var snapshot = new BestBidAskSnapshot(
            ConnectorName: Connector,
            TradingPair: MexcSymbolMapper.FromMexcSymbol(symbol),
            BestBidPrice: bidPrice,
            BestBidAmount: 0,
            BestAskPrice: askPrice,
            BestAskAmount: 0,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _cache.Set(snapshot);

        if (Interlocked.CompareExchange(ref _successfulUpdatesLogged, 1, 0) == 0)
        {
            _logger.LogInformation(
                "MEXC BBO first update received. Symbol={Symbol}, Bid={Bid}, Ask={Ask}",
                symbol,
                bidPrice,
                askPrice);
        }
    }

    private static DateTimeOffset ConvertTimestamp(long timestamp)
    {
        if (timestamp > 10_000_000_000)
            return DateTimeOffset.FromUnixTimeMilliseconds(timestamp);

        return DateTimeOffset.FromUnixTimeSeconds(timestamp);
    }

    private static bool TryGetPropertyIgnoreCase(
        JsonElement element,
        string propertyName,
        out JsonElement value)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            value = default;
            return false;
        }

        if (element.TryGetProperty(propertyName, out value))
            return true;

        foreach (var property in element.EnumerateObject())
        {
            if (property.Name.Equals(
                    propertyName,
                    StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }

    private static bool TryGetStringProperty(
        JsonElement element,
        string propertyName,
        out string value)
    {
        value = "";

        if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
            return false;

        if (property.ValueKind != JsonValueKind.String)
            return false;

        value = property.GetString() ?? "";
        return !string.IsNullOrWhiteSpace(value);
    }

    private static bool TryGetDecimalProperty(
        JsonElement element,
        string propertyName,
        out decimal value)
    {
        value = 0;

        if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
            return false;

        if (property.ValueKind == JsonValueKind.Number)
            return property.TryGetDecimal(out value);

        if (property.ValueKind == JsonValueKind.String)
        {
            return decimal.TryParse(
                property.GetString(),
                NumberStyles.Number,
                CultureInfo.InvariantCulture,
                out value);
        }

        return false;
    }

    private static bool TryGetLongProperty(
        JsonElement element,
        string propertyName,
        out long value)
    {
        value = 0;

        if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
            return false;

        if (property.ValueKind == JsonValueKind.Number)
            return property.TryGetInt64(out value);

        if (property.ValueKind == JsonValueKind.String)
        {
            return long.TryParse(
                property.GetString(),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out value);
        }

        return false;
    }
}
