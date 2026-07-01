using System.Globalization;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Infrastructure.MarketData.Streams.Binance;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Arbitrage.Api.Tests.Infrastructure.MarketData.Streams.Binance;

public class BinanceBboStreamTests
{
    [Fact]
    public void ProcessMessage_UnderCommaDecimalCulture_ParsesDotSeparatedPricesCorrectly()
    {
        var originalCulture = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");

        try
        {
            var cache = new BestBidAskCache();
            var stream = new BinanceBboStream(cache, NullLogger<BinanceBboStream>.Instance);
            stream._allowedSymbols.Add("BTCUSDT");

            const string json = """
                {"s":"BTCUSDT","b":"64123.45","B":"1.230","a":"64124.10","A":"0.870"}
                """;

            stream.ProcessMessage(json);

            var found = cache.TryGet("binance_perpetual", "BTC-USDT", out var snapshot);

            Assert.True(found);
            Assert.Equal(64123.45m, snapshot!.BestBidPrice);
            Assert.Equal(1.230m, snapshot.BestBidAmount);
            Assert.Equal(64124.10m, snapshot.BestAskPrice);
            Assert.Equal(0.870m, snapshot.BestAskAmount);
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
        }
    }

    [Fact]
    public void ProcessMessage_SymbolNotAllowed_DoesNotUpdateCache()
    {
        var cache = new BestBidAskCache();
        var stream = new BinanceBboStream(cache, NullLogger<BinanceBboStream>.Instance);

        const string json = """
            {"s":"ETHUSDT","b":"1000.0","B":"1.0","a":"1000.5","A":"1.0"}
            """;

        stream.ProcessMessage(json);

        Assert.False(cache.TryGet("binance_perpetual", "ETH-USDT", out _));
    }
}
