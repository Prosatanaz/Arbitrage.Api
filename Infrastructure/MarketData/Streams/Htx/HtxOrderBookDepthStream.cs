using System.Globalization;
using System.IO.Compression;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Htx;

public sealed class HtxOrderBookDepthStream : IOrderBookDepthStream
{
    private const string Connector = "htx_perpetual";
    private const string WsUrl = "wss://api.hbdm.com/linear-swap-ws";
    private const string DepthStep = "step6";

    private readonly DepthSubscriptionTargetStore _targetStore;
    private readonly OrderBookDepthCache _depthCache;
    private readonly HtxContractMetadataStore _metadataStore;
    private readonly ILogger<HtxOrderBookDepthStream> _logger;

    private readonly HashSet<string> _subscribedContracts =
        new(StringComparer.OrdinalIgnoreCase);

    private SemaphoreSlim? _sendLock;
    private int _successfulUpdatesLogged;
    private int _missingMetadataLogged;

    public string ConnectorName => Connector;

    public HtxOrderBookDepthStream(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        HtxContractMetadataStore metadataStore,
        ILogger<HtxOrderBookDepthStream> logger)
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
                    "HTX depth stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _sendLock = new SemaphoreSlim(1, 1);
        _subscribedContracts.Clear();
        _successfulUpdatesLogged = 0;
        _missingMetadataLogged = 0;

        _logger.LogInformation(
            "Connecting to HTX depth stream: {Url}",
            WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation("Connected to HTX depth stream.");

        var receiveTask = ReceiveLoopAsync(socket, ct);
        var subscriptionTask = SubscriptionLoopAsync(socket, ct);

        await Task.WhenAny(receiveTask, subscriptionTask);
    }

    private async Task SubscriptionLoopAsync(
        ClientWebSocket socket,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            var desiredContracts = _targetStore
                .GetPairsForConnector(Connector)
                .Select(HtxSymbolMapper.ToHtxContractCode)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var toSubscribe = desiredContracts
                .Except(_subscribedContracts, StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x)
                .ToList();

            var toUnsubscribe = _subscribedContracts
                .Except(desiredContracts, StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x)
                .ToList();

            foreach (var contractCode in toSubscribe)
            {
                await SendSubscriptionAsync(
                    socket,
                    operation: "sub",
                    contractCode,
                    ct);

                _subscribedContracts.Add(contractCode);

                _logger.LogInformation(
                    "HTX depth subscribe requested. ContractCode={ContractCode}, Total={Total}",
                    contractCode,
                    _subscribedContracts.Count);

                await Task.Delay(100, ct);
            }

            foreach (var contractCode in toUnsubscribe)
            {
                await SendSubscriptionAsync(
                    socket,
                    operation: "unsub",
                    contractCode,
                    ct);

                _subscribedContracts.Remove(contractCode);

                _depthCache.Remove(
                    Connector,
                    HtxSymbolMapper.FromHtxContractCode(contractCode));

                _logger.LogInformation(
                    "HTX depth unsubscribe requested. ContractCode={ContractCode}, Total={Total}",
                    contractCode,
                    _subscribedContracts.Count);

                await Task.Delay(100, ct);
            }

            await Task.Delay(1000, ct);
        }
    }

    private async Task SendSubscriptionAsync(
        ClientWebSocket socket,
        string operation,
        string contractCode,
        CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(new Dictionary<string, string>
        {
            [operation] = $"market.{contractCode}.depth.{DepthStep}",
            ["id"] = $"depth-{contractCode}"
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
                break;

            try
            {
                await ProcessMessageAsync(socket, message, ct);
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to parse HTX depth message. Raw={Raw}",
                    message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to process HTX depth message. Raw={Raw}",
                    message);
            }
        }
    }

    private async Task ProcessMessageAsync(
        ClientWebSocket socket,
        string json,
        CancellationToken ct)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (TryGetLongProperty(root, "ping", out var ping))
        {
            var pong = JsonSerializer.Serialize(new
            {
                pong = ping
            });

            await SendTextAsync(socket, pong, ct);
            return;
        }

        if (TryGetPropertyIgnoreCase(root, "status", out var statusElement) &&
            statusElement.ValueKind == JsonValueKind.String &&
            string.Equals(statusElement.GetString(), "error", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning("HTX depth WS error. Raw={Raw}", json);
            return;
        }

        if (!TryExtractDepth(root, out var item))
        {
            _logger.LogDebug(
                "HTX depth message ignored. Raw={Raw}",
                json);

            return;
        }

        if (!_subscribedContracts.Contains(item.ContractCode))
            return;

        var bids = item.Bids
            .Where(x => x.Price > 0 && x.Amount > 0)
            .OrderByDescending(x => x.Price)
            .ToList();

        var asks = item.Asks
            .Where(x => x.Price > 0 && x.Amount > 0)
            .OrderBy(x => x.Price)
            .ToList();

        if (bids.Count == 0 || asks.Count == 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = item.TimestampMs > 0
            ? ConvertTimestamp(item.TimestampMs)
            : receivedAt;

        var snapshot = new OrderBookDepthSnapshot(
            ConnectorName: Connector,
            TradingPair: HtxSymbolMapper.FromHtxContractCode(item.ContractCode),
            Bids: bids,
            Asks: asks,
            ExchangeTimestamp: exchangeTimestamp,
            ReceivedAt: receivedAt);

        _depthCache.Set(snapshot);

        if (Interlocked.CompareExchange(ref _successfulUpdatesLogged, 1, 0) == 0)
        {
            _logger.LogInformation(
                "HTX depth first update received. ContractCode={ContractCode}, Bids={Bids}, Asks={Asks}",
                item.ContractCode,
                bids.Count,
                asks.Count);
        }
    }

    private bool TryExtractDepth(
        JsonElement root,
        out HtxDepthItem item)
    {
        item = default;

        if (!TryGetStringProperty(root, "ch", out var channel))
            return false;

        if (!TryExtractContractCodeFromChannel(channel, out var contractCode))
            return false;

        if (!TryGetPropertyIgnoreCase(root, "tick", out var tick))
            return false;

        if (!TryGetPropertyIgnoreCase(tick, "bids", out var bidsElement) ||
            !TryGetPropertyIgnoreCase(tick, "asks", out var asksElement))
        {
            return false;
        }

        var bids = ReadDepthLevels(contractCode, bidsElement);
        var asks = ReadDepthLevels(contractCode, asksElement);

        if (bids.Count == 0 || asks.Count == 0)
            return false;

        var timestampMs = TryGetLongProperty(tick, "ts", out var tickTimestamp)
            ? tickTimestamp
            : TryGetLongProperty(root, "ts", out var rootTimestamp)
                ? rootTimestamp
                : 0;

        item = new HtxDepthItem(
            ContractCode: contractCode,
            Bids: bids,
            Asks: asks,
            TimestampMs: timestampMs);

        return true;
    }

    private static bool TryExtractContractCodeFromChannel(
        string channel,
        out string contractCode)
    {
        contractCode = "";

        // market.BTC-USDT.depth.step6
        const string prefix = "market.";
        const string marker = ".depth.";

        if (!channel.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return false;

        var markerIndex = channel.IndexOf(
            marker,
            StringComparison.OrdinalIgnoreCase);

        if (markerIndex <= prefix.Length)
            return false;

        contractCode = channel[prefix.Length..markerIndex]
            .Trim()
            .ToUpperInvariant();

        return !string.IsNullOrWhiteSpace(contractCode);
    }

    private IReadOnlyList<OrderBookDepthLevel> ReadDepthLevels(
        string contractCode,
        JsonElement levelsElement)
    {
        if (levelsElement.ValueKind != JsonValueKind.Array)
            return [];

        var result = new List<OrderBookDepthLevel>();

        foreach (var level in levelsElement.EnumerateArray())
        {
            if (TryReadDepthLevel(contractCode, level, out var parsed))
                result.Add(parsed);
        }

        return result;
    }

    private bool TryReadDepthLevel(
        string contractCode,
        JsonElement level,
        out OrderBookDepthLevel parsed)
    {
        parsed = new OrderBookDepthLevel(
            Price: 0,
            Amount: 0);

        if (level.ValueKind != JsonValueKind.Array)
            return false;

        var values = level.EnumerateArray().ToList();

        if (values.Count < 2)
            return false;

        if (!TryReadDecimal(values[0], out var price))
            return false;

        if (!TryReadDecimal(values[1], out var rawAmount))
            return false;

        if (price <= 0 || rawAmount <= 0)
            return false;

        var amount = ConvertContractAmountToBaseAmount(
            contractCode,
            rawAmount);

        if (amount <= 0)
            return false;

        parsed = new OrderBookDepthLevel(
            Price: price,
            Amount: amount);

        return true;
    }

    private decimal ConvertContractAmountToBaseAmount(
        string contractCode,
        decimal rawAmount)
    {
        if (_metadataStore.TryGetByContractCode(
                contractCode,
                out var metadata) &&
            metadata.ContractSize > 0)
        {
            return rawAmount * metadata.ContractSize;
        }

        if (Interlocked.CompareExchange(ref _missingMetadataLogged, 1, 0) == 0)
        {
            _logger.LogWarning(
                "HTX contract metadata was not found. Raw amount will be used as base amount. ContractCode={ContractCode}",
                contractCode);
        }

        return rawAmount;
    }

    private async Task SendTextAsync(
        ClientWebSocket socket,
        string payload,
        CancellationToken ct)
    {
        if (_sendLock is null)
            throw new InvalidOperationException("HTX send lock is not initialized.");

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

        var bytes = memory.ToArray();

        return DecompressGzip(bytes);
    }

    private static string DecompressGzip(byte[] bytes)
    {
        using var compressed = new MemoryStream(bytes);
        using var gzip = new GZipStream(compressed, CompressionMode.Decompress);
        using var reader = new StreamReader(gzip, Encoding.UTF8);

        return reader.ReadToEnd();
    }

    private static DateTimeOffset ConvertTimestamp(long timestamp)
    {
        return timestamp > 10_000_000_000
            ? DateTimeOffset.FromUnixTimeMilliseconds(timestamp)
            : DateTimeOffset.FromUnixTimeSeconds(timestamp);
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

    private static bool TryReadDecimal(
        JsonElement element,
        out decimal value)
    {
        value = 0;

        if (element.ValueKind == JsonValueKind.Number)
            return element.TryGetDecimal(out value);

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

    private readonly record struct HtxDepthItem(
        string ContractCode,
        IReadOnlyList<OrderBookDepthLevel> Bids,
        IReadOnlyList<OrderBookDepthLevel> Asks,
        long TimestampMs);
}