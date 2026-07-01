using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Binance;

public sealed class BinanceBboStream : IBestBidAskStream
{
    private const string Connector = "binance_perpetual";
    private const string WsUrl = "wss://fstream.binance.com/ws/!bookTicker";

    private readonly BestBidAskCache _cache;
    private readonly ILogger<BinanceBboStream> _logger;

    internal HashSet<string> _allowedSymbols = new(StringComparer.OrdinalIgnoreCase);

    public string ConnectorName => Connector;

    public BinanceBboStream(
        BestBidAskCache cache,
        ILogger<BinanceBboStream> logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public async Task StartAsync(
        IReadOnlyList<string> tradingPairs,
        CancellationToken ct)
    {
        _allowedSymbols = tradingPairs
            .Select(BinanceSymbolMapper.ToBinanceSymbol)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

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
                _logger.LogError(ex, "Binance BBO stream crashed. Reconnecting in 5 seconds...");

                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    private async Task RunConnectionAsync(CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        _logger.LogInformation("Connecting to Binance BBO stream: {Url}", WsUrl);

        await socket.ConnectAsync(new Uri(WsUrl), ct);

        _logger.LogInformation(
            "Connected to Binance BBO stream. AllowedSymbols={Count}",
            _allowedSymbols.Count);

        var buffer = new byte[1024 * 16];

        while (!ct.IsCancellationRequested &&
               socket.State == WebSocketState.Open)
        {
            var message = await ReceiveTextAsync(socket, buffer, ct);

            if (message is null)
                break;

            ProcessMessage(message);
        }
    }

    internal void ProcessMessage(string json)
    {
        var dto = JsonSerializer.Deserialize<BinanceBookTickerMessage>(json);

        if (dto is null)
            return;

        if (!_allowedSymbols.Contains(dto.Symbol))
            return;

        if (!decimal.TryParse(dto.BestBidPrice, NumberStyles.Number, CultureInfo.InvariantCulture, out var bidPrice))
            return;

        if (!decimal.TryParse(dto.BestBidQuantity, NumberStyles.Number, CultureInfo.InvariantCulture, out var bidAmount))
            return;

        if (!decimal.TryParse(dto.BestAskPrice, NumberStyles.Number, CultureInfo.InvariantCulture, out var askPrice))
            return;

        if (!decimal.TryParse(dto.BestAskQuantity, NumberStyles.Number, CultureInfo.InvariantCulture, out var askAmount))
            return;

        if (bidPrice <= 0 || askPrice <= 0)
            return;

        var now = DateTimeOffset.UtcNow;

        var snapshot = new BestBidAskSnapshot(
            ConnectorName: ConnectorName,
            TradingPair: BinanceSymbolMapper.FromBinanceSymbol(dto.Symbol),
            BestBidPrice: bidPrice,
            BestBidAmount: bidAmount,
            BestAskPrice: askPrice,
            BestAskAmount: askAmount,
            ExchangeTimestamp: now,
            ReceivedAt: now);

        _cache.Set(snapshot);
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

    private sealed class BinanceBookTickerMessage
    {
        [JsonPropertyName("s")]
        public string Symbol { get; init; } = default!;

        [JsonPropertyName("b")]
        public string BestBidPrice { get; init; } = default!;

        [JsonPropertyName("B")]
        public string BestBidQuantity { get; init; } = default!;

        [JsonPropertyName("a")]
        public string BestAskPrice { get; init; } = default!;

        [JsonPropertyName("A")]
        public string BestAskQuantity { get; init; } = default!;
    }
}