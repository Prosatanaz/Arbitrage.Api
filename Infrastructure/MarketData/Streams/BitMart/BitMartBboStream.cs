using System.Globalization;
using System.Net.WebSockets;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.BitMart;

public sealed class BitMartBboStream : BboStreamBase
{
    private const string Connector = "bitmart_perpetual";
    private const string Channel = "futures/bookticker";

    private readonly BestBidAskCache _cache;
    private readonly ILogger<BitMartBboStream> _logger;

    private int _successfulUpdatesLogged;

    public BitMartBboStream(
        BestBidAskCache cache,
        ILogger<BitMartBboStream> logger)
        : base(logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://openapi-ws-v2.bitmart.com/api?protocol=1.1";

    protected override int ReceiveBufferSize => 1024 * 256;

    protected override bool SubscribeConcurrently => true;

    protected override void OnConnecting()
    {
        _successfulUpdatesLogged = 0;
        base.OnConnecting();
    }

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        BitMartSymbolMapper.ToBitMartSymbol(tradingPair);

    protected override async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbols,
        CancellationToken ct)
    {
        const int batchSize = 20;

        foreach (var batch in symbols.Chunk(batchSize))
        {
            var payload = JsonSerializer.Serialize(new
            {
                action = "subscribe",
                args = batch
                    .Select(symbol => $"{Channel}:{symbol}")
                    .ToList()
            });

            await SendTextAsync(socket, payload, ct);

            _logger.LogInformation(
                "BitMart BBO subscribe requested. BatchSize={BatchSize}, Total={Total}",
                batch.Length,
                AllowedSymbols.Count);

            await Task.Delay(1000, ct);
        }

        _logger.LogInformation(
            "BitMart BBO subscription completed. Total={Total}",
            AllowedSymbols.Count);
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

            await SendTextAsync(socket, "{\"action\":\"ping\"}", ct);
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
                "Failed to parse BitMart BBO message. Raw={Raw}",
                json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to process BitMart BBO message. Raw={Raw}",
                json);
        }

        await Task.CompletedTask;
    }

    private void ProcessMessage(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (TryGetStringProperty(root, "action", out var action))
        {
            if (action.Equals("subscribe", StringComparison.OrdinalIgnoreCase) ||
                action.Equals("pong", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            if (action.Equals("error", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning("BitMart BBO WS error. Raw={Raw}", json);
                return;
            }
        }

        if (!TryGetStringProperty(root, "group", out var group))
            return;

        if (!group.StartsWith($"{Channel}:", StringComparison.OrdinalIgnoreCase))
            return;

        if (!TryGetPropertyIgnoreCase(root, "data", out var data))
            return;

        ProcessTicker(data);
    }

    private void ProcessTicker(JsonElement data)
    {
        if (!TryGetStringProperty(data, "symbol", out var symbol))
            return;

        if (!AllowedSymbols.Contains(symbol))
            return;

        if (!TryGetDecimalProperty(data, "best_bid_price", out var bidPrice))
            return;

        if (!TryGetDecimalProperty(data, "best_ask_price", out var askPrice))
            return;

        if (!TryGetDecimalProperty(data, "best_bid_vol", out var bidAmount))
            bidAmount = 0;

        if (!TryGetDecimalProperty(data, "best_ask_vol", out var askAmount))
            askAmount = 0;

        if (bidPrice <= 0 || askPrice <= 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = TryGetLongProperty(data, "ms_t", out var timestampMs) &&
                                timestampMs > 0
            ? DateTimeOffset.FromUnixTimeMilliseconds(timestampMs)
            : receivedAt;

        var snapshot = new BestBidAskSnapshot(
            ConnectorName: Connector,
            TradingPair: BitMartSymbolMapper.FromBitMartSymbol(symbol),
            BestBidPrice: bidPrice,
            BestBidAmount: bidAmount,
            BestAskPrice: askPrice,
            BestAskAmount: askAmount,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _cache.Set(snapshot);

        if (Interlocked.CompareExchange(ref _successfulUpdatesLogged, 1, 0) == 0)
        {
            _logger.LogInformation(
                "BitMart BBO first update received. Symbol={Symbol}, Bid={Bid}, Ask={Ask}",
                symbol,
                bidPrice,
                askPrice);
        }
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
            if (property.Name.Equals(propertyName, StringComparison.OrdinalIgnoreCase))
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
