using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Mexc;

public sealed class MexcOrderBookDepthStream : IOrderBookDepthStream
{
    private const string Connector = "mexc_perpetual";
    private const string WsUrl = "wss://contract.mexc.com/edge";
    private const string Channel = "push.depth.step";
    private const string DepthStep = "10";

    private readonly DepthSubscriptionTargetStore _targetStore;
    private readonly OrderBookDepthCache _depthCache;
    private readonly MexcContractMetadataStore _metadataStore;
    private readonly ILogger<MexcOrderBookDepthStream> _logger;

    private readonly HashSet<string> _subscribedSymbols =
        new(StringComparer.OrdinalIgnoreCase);

    private SemaphoreSlim? _sendLock;
    private int _successfulUpdatesLogged;

    public string ConnectorName => Connector;

    public MexcOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        MexcContractMetadataStore metadataStore,
        ILogger<MexcOrderBookDepthStream> logger)
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
                    "MEXC depth stream crashed. Reconnecting in 5 seconds...");

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

        _logger.LogInformation("Connecting to MEXC depth stream: {Url}", WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation("Connected to MEXC depth stream.");

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
                .Select(MexcSymbolMapper.ToMexcSymbol)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var toSubscribe = desiredSymbols
                .Except(_subscribedSymbols, StringComparer.OrdinalIgnoreCase)
                .ToList();

            var toUnsubscribe = _subscribedSymbols
                .Except(desiredSymbols, StringComparer.OrdinalIgnoreCase)
                .ToList();

            foreach (var symbol in toSubscribe)
            {
                await SendSubscriptionAsync(
                    socket,
                    method: "sub.depth.step",
                    symbol,
                    ct);

                _subscribedSymbols.Add(symbol);

                _logger.LogInformation(
                    "MEXC depth subscribe requested. Symbol={Symbol}, Total={Total}",
                    symbol,
                    _subscribedSymbols.Count);

                await Task.Delay(100, ct);
            }

            foreach (var symbol in toUnsubscribe)
            {
                await SendSubscriptionAsync(
                    socket,
                    method: "unsub.depth.step",
                    symbol,
                    ct);

                _subscribedSymbols.Remove(symbol);

                _depthCache.Remove(
                    Connector,
                    MexcSymbolMapper.FromMexcSymbol(symbol));

                _logger.LogInformation(
                    "MEXC depth unsubscribe requested. Symbol={Symbol}, Total={Total}",
                    symbol,
                    _subscribedSymbols.Count);

                await Task.Delay(100, ct);
            }

            await Task.Delay(1000, ct);
        }
    }

    private async Task SendSubscriptionAsync(
        ClientWebSocket socket,
        string method,
        string symbol,
        CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(new
        {
            method,
            param = new
            {
                symbol,
                step = DepthStep
            },
            gzip = false
        });

        await SendTextAsync(socket, payload, ct);
    }

    private async Task ReceiveLoopAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        var buffer = new byte[1024 * 512];

        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            var message = await ReceiveTextAsync(socket, buffer, ct);

            if (message is null)
            {
                break;
            }

            try
            {
                ProcessMessage(message);
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to parse MEXC depth message. Raw={Raw}",
                    message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to process MEXC depth message. Raw={Raw}",
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
            {
                break;
            }

            var payload = JsonSerializer.Serialize(new
            {
                method = "ping"
            });

            await SendTextAsync(socket, payload, ct);
        }
    }

    private void ProcessMessage(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (!TryGetStringProperty(root, "channel", out var channel))
        {
            return;
        }

        if (string.Equals(channel, "pong", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (!string.Equals(channel, Channel, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (!TryGetStringProperty(root, "symbol", out var symbol))
        {
            return;
        }

        if (!_subscribedSymbols.Contains(symbol))
        {
            return;
        }

        if (!TryGetPropertyIgnoreCase(root, "data", out var dataElement))
        {
            return;
        }

        ProcessDepth(symbol, dataElement);
    }

    private void ProcessDepth(
        string symbol,
        JsonElement data)
    {
        if (!TryGetPropertyIgnoreCase(data, "bids", out var bidsElement))
        {
            return;
        }

        if (!TryGetPropertyIgnoreCase(data, "asks", out var asksElement))
        {
            return;
        }

        var contractSize = GetContractSize(symbol);

        var bids = ReadLevels(bidsElement, contractSize)
            .OrderByDescending(x => x.Price)
            .ToList();

        var asks = ReadLevels(asksElement, contractSize)
            .OrderBy(x => x.Price)
            .ToList();

        if (bids.Count == 0 || asks.Count == 0)
        {
            return;
        }

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = TryGetLongProperty(data, "ct", out var ct) && ct > 0
            ? ConvertTimestamp(ct)
            : receivedAt;

        var snapshot = new OrderBookDepthSnapshot(
            ConnectorName: Connector,
            TradingPair: MexcSymbolMapper.FromMexcSymbol(symbol),
            Bids: bids,
            Asks: asks,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _depthCache.Set(snapshot);

        if (Interlocked.CompareExchange(ref _successfulUpdatesLogged, 1, 0) == 0)
        {
            _logger.LogInformation(
                "MEXC depth first update received. Symbol={Symbol}, Bids={Bids}, Asks={Asks}, ContractSize={ContractSize}",
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
            "MEXC contract metadata was not found. Symbol={Symbol}. Falling back to contract quantity as base amount.",
            symbol);

        return 1m;
    }

    private static IReadOnlyList<OrderBookDepthLevel> ReadLevels(
        JsonElement levelsElement,
        decimal contractSize)
    {
        if (levelsElement.ValueKind != JsonValueKind.Array)
        {
            return [];
        }

        var result = new List<OrderBookDepthLevel>();

        foreach (var level in levelsElement.EnumerateArray())
        {
            if (level.ValueKind != JsonValueKind.Array)
            {
                continue;
            }

            var values = level.EnumerateArray().ToList();

            if (values.Count < 2)
            {
                continue;
            }

            if (!TryReadDecimal(values[0], out var price))
            {
                continue;
            }

            // MEXC depth step level:
            // [price, orderCount, contractQuantity]
            // If third value exists, use it as contracts amount.
            // If not, fallback to second value.
            var quantityElement = values.Count >= 3
                ? values[2]
                : values[1];

            if (!TryReadDecimal(quantityElement, out var contractsAmount))
            {
                continue;
            }

            var baseAmount = contractsAmount * contractSize;

            if (price <= 0 || baseAmount <= 0)
            {
                continue;
            }

            result.Add(new OrderBookDepthLevel(
                Price: price,
                Amount: baseAmount));
        }

        return result;
    }

    private async Task SendTextAsync(
        ClientWebSocket socket,
        string payload,
        CancellationToken ct)
    {
        if (_sendLock is null)
        {
            throw new InvalidOperationException("MEXC send lock is not initialized.");
        }

        await _sendLock.WaitAsync(ct);

        try
        {
            if (socket.State != WebSocketState.Open)
            {
                return;
            }

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
            {
                return null;
            }

            memory.Write(buffer, 0, result.Count);

            if (result.EndOfMessage)
            {
                break;
            }
        }

        return Encoding.UTF8.GetString(memory.ToArray());
    }

    private static DateTimeOffset ConvertTimestamp(long timestamp)
    {
        if (timestamp > 10_000_000_000)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(timestamp);
        }

        return DateTimeOffset.FromUnixTimeSeconds(timestamp);
    }

    private static bool TryReadDecimal(
        JsonElement element,
        out decimal value)
    {
        value = 0;

        if (element.ValueKind == JsonValueKind.Number)
        {
            return element.TryGetDecimal(out value);
        }

        if (element.ValueKind == JsonValueKind.String)
        {
            return decimal.TryParse(
                element.GetString(),
                NumberStyles.Number,
                CultureInfo.InvariantCulture,
                out value);
        }

        return false;
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