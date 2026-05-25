using System.Text.Json;
using System.Text.Json.Serialization;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams.KuCoin;

public sealed record KuCoinFuturesWebSocketConnectionInfo(
    string WebSocketUrl,
    int PingIntervalMs);

public sealed class KuCoinFuturesWebSocketTokenProvider
{
    public const string HttpClientName = "KuCoinFutures";

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<KuCoinFuturesWebSocketTokenProvider> _logger;

    public KuCoinFuturesWebSocketTokenProvider(
        IHttpClientFactory httpClientFactory,
        ILogger<KuCoinFuturesWebSocketTokenProvider> logger)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    public async Task<KuCoinFuturesWebSocketConnectionInfo> GetConnectionInfoAsync(
        CancellationToken ct)
    {
        var httpClient = _httpClientFactory.CreateClient(HttpClientName);

        using var response = await httpClient.PostAsync(
            "/api/v1/bullet-public",
            content: null,
            cancellationToken: ct);

        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync(ct);

        var dto = await JsonSerializer.DeserializeAsync<KuCoinBulletPublicResponse>(
            stream,
            cancellationToken: ct);

        if (dto?.Data is null ||
            string.IsNullOrWhiteSpace(dto.Data.Token) ||
            dto.Data.InstanceServers.Count == 0)
        {
            throw new InvalidOperationException(
                "KuCoin futures websocket token response is invalid.");
        }

        var server = dto.Data.InstanceServers[0];

        var endpoint = NormalizeEndpoint(server.Endpoint);
        var connectId = Guid.NewGuid().ToString("N");
        var token = Uri.EscapeDataString(dto.Data.Token);

        var wsUrl = $"{endpoint}?token={token}&connectId={connectId}";

        _logger.LogInformation(
            "KuCoin futures websocket connection info received. Endpoint={Endpoint}, PingIntervalMs={PingIntervalMs}",
            endpoint,
            server.PingInterval);

        return new KuCoinFuturesWebSocketConnectionInfo(
            WebSocketUrl: wsUrl,
            PingIntervalMs: server.PingInterval > 0
                ? server.PingInterval
                : 18000);
    }

    private static string NormalizeEndpoint(string endpoint)
    {
        var normalized = endpoint.TrimEnd('/');

        return normalized.EndsWith(
            "/endpoint",
            StringComparison.OrdinalIgnoreCase)
                ? normalized
                : $"{normalized}/endpoint";
    }

    private sealed class KuCoinBulletPublicResponse
    {
        [JsonPropertyName("code")]
        public string Code { get; init; } = "";

        [JsonPropertyName("data")]
        public KuCoinBulletPublicData? Data { get; init; }
    }

    private sealed class KuCoinBulletPublicData
    {
        [JsonPropertyName("token")]
        public string Token { get; init; } = "";

        [JsonPropertyName("instanceServers")]
        public List<KuCoinInstanceServer> InstanceServers { get; init; } = new();
    }

    private sealed class KuCoinInstanceServer
    {
        [JsonPropertyName("endpoint")]
        public string Endpoint { get; init; } = "";

        [JsonPropertyName("pingInterval")]
        public int PingInterval { get; init; }

        [JsonPropertyName("pingTimeout")]
        public int PingTimeout { get; init; }
    }
}