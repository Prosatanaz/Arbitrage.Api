using System.Globalization;
using System.IO.Compression;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;
using Arbitrage.Api.Infrastructure.MarketData.Streams;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Htx;

public sealed class HtxBboStream : BboStreamBase
{
    private const string Connector = "htx_perpetual";

    private readonly BestBidAskCache _cache;
    private readonly ILogger<HtxBboStream> _logger;

    private int _successfulUpdatesLogged;

    public HtxBboStream(
        BestBidAskCache cache,
        ILogger<HtxBboStream> logger)
        : base(logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public override string ConnectorName => Connector;

    protected override string WsUrl => "wss://api.hbdm.com/linear-swap-ws";

    protected override int ReceiveBufferSize => 1024 * 256;

    public override Task StartAsync(
        IReadOnlyList<string> tradingPairs,
        CancellationToken ct)
    {
        var pairs = tradingPairs
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        if (pairs.Count == 0)
        {
            _logger.LogWarning("HTX BBO stream has no trading pairs to subscribe.");
            return Task.CompletedTask;
        }

        return base.StartAsync(pairs, ct);
    }

    protected override void OnConnecting()
    {
        _successfulUpdatesLogged = 0;
        _logger.LogInformation("Connecting to HTX BBO stream: {Url}", WsUrl);
    }

    protected override string MapTradingPairToExchangeSymbol(string tradingPair) =>
        HtxSymbolMapper.ToHtxContractCode(tradingPair);

    protected override async Task SubscribeAsync(
        ClientWebSocket socket,
        IReadOnlyList<string> symbols,
        CancellationToken ct)
    {
        foreach (var contractCode in symbols)
        {
            var payload = JsonSerializer.Serialize(new
            {
                sub = $"market.{contractCode}.bbo",
                id = $"bbo-{contractCode}"
            });

            await SendTextAsync(socket, payload, ct);
            await Task.Delay(25, ct);
        }

        _logger.LogInformation(
            "HTX BBO subscribe requested. Count={Count}, Sample={Sample}",
            symbols.Count,
            string.Join(", ", symbols.Take(10)));
    }

    protected override bool TryDecompress(
        byte[] buffer,
        int count,
        WebSocketMessageType messageType,
        out string text)
    {
        if (messageType == WebSocketMessageType.Binary)
        {
            using var compressed = new MemoryStream(buffer, 0, count);
            using var gzip = new GZipStream(compressed, CompressionMode.Decompress);
            using var reader = new StreamReader(gzip, Encoding.UTF8);

            text = reader.ReadToEnd();
            return true;
        }

        text = Encoding.UTF8.GetString(buffer, 0, count);
        return true;
    }

    protected override async Task ProcessMessageAsync(
        ClientWebSocket socket,
        string json,
        CancellationToken ct)
    {
        try
        {
            await HandleMessageAsync(socket, json, ct);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to parse HTX BBO message. Raw={Raw}",
                json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to process HTX BBO message. Raw={Raw}",
                json);
        }
    }

    private async Task HandleMessageAsync(
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
            _logger.LogWarning("HTX BBO WS error. Raw={Raw}", json);
            return;
        }

        if (!TryGetStringProperty(root, "ch", out var channel))
            return;

        if (!channel.EndsWith(".bbo", StringComparison.OrdinalIgnoreCase))
            return;

        var tradingPair = ExtractTradingPairFromChannel(channel);

        if (tradingPair is null)
            return;

        if (!TryGetPropertyIgnoreCase(root, "tick", out var tick))
            return;

        if (!TryGetArrayProperty(tick, "bid", out var bid) ||
            !TryGetArrayProperty(tick, "ask", out var ask))
        {
            return;
        }

        if (!TryReadPriceAmount(bid, out var bidPrice, out var bidAmount))
            return;

        if (!TryReadPriceAmount(ask, out var askPrice, out var askAmount))
            return;

        if (bidPrice <= 0 || askPrice <= 0 || bidAmount <= 0 || askAmount <= 0)
            return;

        var receivedAt = DateTimeOffset.UtcNow;

        var exchangeTimestamp = TryGetLongProperty(root, "ts", out var ts) && ts > 0
            ? ConvertTimestamp(ts)
            : receivedAt;

        var snapshot = new BestBidAskSnapshot(
            ConnectorName: Connector,
            TradingPair: tradingPair,
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
                "HTX BBO first update received. Pair={Pair}, Bid={BidPrice}, Ask={AskPrice}",
                tradingPair,
                bidPrice,
                askPrice);
        }
    }

    private static string? ExtractTradingPairFromChannel(string channel)
    {
        // market.BTC-USDT.bbo
        const string prefix = "market.";
        const string suffix = ".bbo";

        if (!channel.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) ||
            !channel.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        var symbol = channel[prefix.Length..^suffix.Length];

        return HtxSymbolMapper.FromHtxContractCode(symbol);
    }

    private static bool TryReadPriceAmount(
        JsonElement array,
        out decimal price,
        out decimal amount)
    {
        price = 0;
        amount = 0;

        if (array.ValueKind != JsonValueKind.Array)
            return false;

        var values = array.EnumerateArray().ToList();

        if (values.Count < 2)
            return false;

        return TryReadDecimal(values[0], out price) &&
               TryReadDecimal(values[1], out amount);
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

    private static bool TryGetArrayProperty(
        JsonElement element,
        string propertyName,
        out JsonElement value)
    {
        if (!TryGetPropertyIgnoreCase(element, propertyName, out value))
            return false;

        return value.ValueKind == JsonValueKind.Array;
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
}
