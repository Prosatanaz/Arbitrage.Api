using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.BitMart;

public sealed class BitMartOrderBookDepthStream : IOrderBookDepthStream
{
    private const string Connector = "bitmart_perpetual";
    private const string WsUrl = "wss://openapi-ws-v2.bitmart.com/api?protocol=1.1";
    private const string Channel = "futures/depthAll20";
    private const string Speed = "200ms";

    private readonly DepthSubscriptionTargetStore _targetStore;
    private readonly OrderBookDepthCache _depthCache;
    private readonly BitMartContractMetadataStore _metadataStore;
    private readonly ILogger<BitMartOrderBookDepthStream> _logger;

    private readonly HashSet<string> _subscribedSymbols =
        new(StringComparer.OrdinalIgnoreCase);

    private SemaphoreSlim? _sendLock;
    private int _successfulUpdatesLogged;

    public string ConnectorName => Connector;

    public BitMartOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        BitMartContractMetadataStore metadataStore,
        ILogger<BitMartOrderBookDepthStream> logger)
    {
        _targetStore = targetStore;
        _depthCache = depthCache;
        _metadataStore = metadataStore;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken ct)
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
                    "BitMart depth stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _sendLock = new SemaphoreSlim(1, 1);
        _subscribedSymbols.Clear();
        _successfulUpdatesLogged = 0;

        _logger.LogInformation("Connecting to BitMart depth stream: {Url}", WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation("Connected to BitMart depth stream.");

        var receiveTask = ReceiveLoopAsync(socket, ct);
        var pingTask = PingLoopAsync(socket, ct);
        var subscriptionTask = SubscriptionLoopAsync(socket, ct);

        await Task.WhenAny(receiveTask, pingTask, subscriptionTask);
    }

    private async Task SubscriptionLoopAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            var desiredSymbols = _targetStore
                .GetPairsForConnector(Connector)
                .Select(BitMartSymbolMapper.ToBitMartSymbol)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var toSubscribe = desiredSymbols
                .Except(_subscribedSymbols, StringComparer.OrdinalIgnoreCase)
                .ToList();

            var toUnsubscribe = _subscribedSymbols
                .Except(desiredSymbols, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (toSubscribe.Count > 0)
            {
                await SendSubscriptionAsync(socket, "subscribe", toSubscribe, ct);

                foreach (var symbol in toSubscribe)
                    _subscribedSymbols.Add(symbol);

                _logger.LogInformation(
                    "BitMart depth subscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                    toSubscribe.Count,
                    _subscribedSymbols.Count,
                    string.Join(", ", toSubscribe.Take(5)));
            }

            if (toUnsubscribe.Count > 0)
            {
                await SendSubscriptionAsync(socket, "unsubscribe", toUnsubscribe, ct);

                foreach (var symbol in toUnsubscribe)
                {
                    _subscribedSymbols.Remove(symbol);

                    _depthCache.Remove(
                        Connector,
                        BitMartSymbolMapper.FromBitMartSymbol(symbol));
                }

                _logger.LogInformation(
                    "BitMart depth unsubscribe requested. Count={Count}, Total={Total}, Sample={Sample}",
                    toUnsubscribe.Count,
                    _subscribedSymbols.Count,
                    string.Join(", ", toUnsubscribe.Take(5)));
            }

            await Task.Delay(1000, ct);
        }
    }

    private async Task SendSubscriptionAsync(
        ClientWebSocket socket,
        string action,
        IReadOnlyList<string> symbols,
        CancellationToken ct)
    {
        const int batchSize = 20;

        foreach (var batch in symbols.Chunk(batchSize))
        {
            var payload = JsonSerializer.Serialize(new
            {
                action,
                args = batch
                    .Select(symbol => $"{Channel}:{symbol}@{Speed}")
                    .ToList()
            });

            await SendTextAsync(socket, payload, ct);

            await Task.Delay(1000, ct);
        }
    }

    private async Task ReceiveLoopAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        var buffer = new byte[1024 * 256];

        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            var message = await ReceiveTextAsync(socket, buffer, ct);

            if (message is null)
                break;

            try
            {
                ProcessMessage(message);
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to parse BitMart depth message. Raw={Raw}",
                    message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to process BitMart depth message. Raw={Raw}",
                    message);
            }
        }
    }

    private async Task PingLoopAsync(
        ClientWebSocket socket,
        CancellationToken ct)
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

    private void ProcessMessage(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (TryGetStringProperty(root, "action", out var action))
        {
            if (action.Equals("subscribe", StringComparison.OrdinalIgnoreCase) ||
                action.Equals("unsubscribe", StringComparison.OrdinalIgnoreCase) ||
                action.Equals("pong", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            if (action.Equals("error", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning("BitMart depth WS error. Raw={Raw}", json);
                return;
            }
        }

        if (!TryGetStringProperty(root, "group", out var group))
            return;

        if (!group.StartsWith($"{Channel}:", StringComparison.OrdinalIgnoreCase))
            return;

        if (!TryGetPropertyIgnoreCase(root, "data", out var data))
            return;

        ProcessDepth(data);
    }

    private void ProcessDepth(JsonElement data)
    {
        if (!TryGetStringProperty(data, "symbol", out var symbol))
            return;

        if (!_subscribedSymbols.Contains(symbol))
            return;

        if (!TryGetPropertyIgnoreCase(data, "bids", out var bidsElement))
            return;

        if (!TryGetPropertyIgnoreCase(data, "asks", out var asksElement))
            return;

        var contractSize = GetContractSize(symbol);

        var bids = ReadLevels(bidsElement, contractSize)
            .OrderByDescending(x => x.Price)
            .ToList();

        var asks = ReadLevels(asksElement, contractSize)
            .OrderBy(x => x.Price)
            .ToList();

        if (bids.Count == 0 || asks.Count == 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = TryGetLongProperty(data, "ms_t", out var timestampMs) &&
                                timestampMs > 0
            ? DateTimeOffset.FromUnixTimeMilliseconds(timestampMs)
            : receivedAt;

        var snapshot = new OrderBookDepthSnapshot(
            ConnectorName: Connector,
            TradingPair: BitMartSymbolMapper.FromBitMartSymbol(symbol),
            Bids: bids,
            Asks: asks,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _depthCache.Set(snapshot);

        if (Interlocked.CompareExchange(ref _successfulUpdatesLogged, 1, 0) == 0)
        {
            _logger.LogInformation(
                "BitMart depth first update received. Symbol={Symbol}, Bids={Bids}, Asks={Asks}, ContractSize={ContractSize}",
                symbol,
                bids.Count,
                asks.Count,
                contractSize);
        }
    }

    private decimal GetContractSize(string symbol)
    {
        if (_metadataStore.TryGetBySymbol(symbol, out var metadata) &&
            metadata.ContractSize > 0)
        {
            return metadata.ContractSize;
        }

        _logger.LogWarning(
            "BitMart contract metadata was not found. Symbol={Symbol}. Falling back to volume as base amount.",
            symbol);

        return 1m;
    }

    private static IReadOnlyList<OrderBookDepthLevel> ReadLevels(
        JsonElement levelsElement,
        decimal contractSize)
    {
        if (levelsElement.ValueKind != JsonValueKind.Array)
            return [];

        var result = new List<OrderBookDepthLevel>();

        foreach (var level in levelsElement.EnumerateArray())
        {
            if (!TryGetDecimalProperty(level, "price", out var price))
                continue;

            if (!TryGetDecimalProperty(level, "vol", out var volume))
                continue;

            var baseAmount = volume * contractSize;

            if (price <= 0 || baseAmount <= 0)
                continue;

            result.Add(new OrderBookDepthLevel(price, baseAmount));
        }

        return result;
    }

    private async Task SendTextAsync(
        ClientWebSocket socket,
        string payload,
        CancellationToken ct)
    {
        if (_sendLock is null)
            throw new InvalidOperationException("BitMart send lock is not initialized.");

        await _sendLock.WaitAsync(ct);

        try
        {
            if (socket.State != WebSocketState.Open)
                return;

            var bytes = Encoding.UTF8.GetBytes(payload);

            await socket.SendAsync(
                bytes,
                WebSocketMessageType.Text,
                true,
                ct);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private static async Task<string?> ReceiveTextAsync(
        ClientWebSocket socket,
        byte[] buffer,
        CancellationToken ct)
    {
        using var memory = new MemoryStream();

        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, ct);

            if (result.MessageType == WebSocketMessageType.Close)
                return null;

            memory.Write(buffer, 0, result.Count);

            if (result.EndOfMessage)
                break;
        }

        return Encoding.UTF8.GetString(memory.ToArray());
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