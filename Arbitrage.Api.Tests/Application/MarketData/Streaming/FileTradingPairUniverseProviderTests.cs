using System.Text;
using Arbitrage.Api.Application.MarketData.Streaming;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace Arbitrage.Api.Tests.Application.MarketData.Streaming;

public class FileTradingPairUniverseProviderTests
{
    [Fact]
    public async Task GetRankedTradingPairsForConnectorAsync_OrdersByCrossExchangeSupport_AndExcludesUnsupported()
    {
        const string json = """
        {
            "pairs": [
                { "tradingPair": "BBB-USDT", "supportedConnectors": ["bingx_perpetual", "mexc_perpetual"] },
                { "tradingPair": "AAA-USDT", "supportedConnectors": ["bingx_perpetual", "bybit_perpetual", "htx_perpetual", "okx_perpetual"] },
                { "tradingPair": "CCC-USDT", "supportedConnectors": ["bingx_perpetual"] },
                { "tradingPair": "DDD-USDT", "supportedConnectors": [] },
                { "tradingPair": "EEE-USDT", "supportedConnectors": ["bybit_perpetual", "okx_perpetual"] }
            ]
        }
        """;

        var provider = CreateProvider(json, out var path);

        try
        {
            var ranked = await provider.GetRankedTradingPairsForConnectorAsync(
                "bingx_perpetual",
                CancellationToken.None);

            // DDD (empty => supported everywhere => max relevance), then by descending
            // supported-connector count: AAA (4), BBB (2), CCC (1). EEE is not on BingX.
            Assert.Equal(
                new[] { "DDD-USDT", "AAA-USDT", "BBB-USDT", "CCC-USDT" },
                ranked);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task GetRankedTradingPairsForConnectorAsync_TieBreaksAlphabetically()
    {
        const string json = """
        {
            "pairs": [
                { "tradingPair": "ZZZ-USDT", "supportedConnectors": ["bingx_perpetual", "bybit_perpetual"] },
                { "tradingPair": "AAA-USDT", "supportedConnectors": ["bingx_perpetual", "bybit_perpetual"] }
            ]
        }
        """;

        var provider = CreateProvider(json, out var path);

        try
        {
            var ranked = await provider.GetRankedTradingPairsForConnectorAsync(
                "bingx_perpetual",
                CancellationToken.None);

            Assert.Equal(new[] { "AAA-USDT", "ZZZ-USDT" }, ranked);
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static FileTradingPairUniverseProvider CreateProvider(
        string json,
        out string path)
    {
        path = Path.Combine(
            Path.GetTempPath(),
            $"universe-{Guid.NewGuid():N}.json");

        File.WriteAllText(path, json, Encoding.UTF8);

        var options = Options.Create(new MarketDataStreamOptions
        {
            DiscoveredPairsPath = path
        });

        return new FileTradingPairUniverseProvider(
            options,
            NullLogger<FileTradingPairUniverseProvider>.Instance,
            new StubHostEnvironment());
    }

    private sealed class StubHostEnvironment : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = "Test";
        public string ApplicationName { get; set; } = "Arbitrage.Api.Tests";
        public string ContentRootPath { get; set; } = Path.GetTempPath();
        public IFileProvider ContentRootFileProvider { get; set; } =
            new NullFileProvider();
    }
}
