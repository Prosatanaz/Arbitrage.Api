using System.IO.Compression;
using System.Text;
using Arbitrage.Api.Infrastructure.MarketData.Streams;
using Xunit;

namespace Arbitrage.Api.Tests.Infrastructure.MarketData.Streams;

public class GzipTextDecoderTests
{
    [Fact]
    public void TryDecompress_ValidGzipBytes_ReturnsOriginalText()
    {
        const string original = "{\"channel\":\"push.depth.step\"}";
        var gzipped = Gzip(original);

        var success = GzipTextDecoder.TryDecompress(gzipped, gzipped.Length, out var text);

        Assert.True(success);
        Assert.Equal(original, text);
    }

    [Fact]
    public void TryDecompress_PlainNonGzipBytes_ReturnsFalseWithoutThrowing()
    {
        var plain = Encoding.UTF8.GetBytes("{\"not\":\"gzip\"}");

        var success = GzipTextDecoder.TryDecompress(plain, plain.Length, out var text);

        Assert.False(success);
        Assert.Equal("", text);
    }

    [Fact]
    public void TryDecompress_RespectsCountEvenWhenBufferIsLarger()
    {
        const string original = "hello";
        var gzipped = Gzip(original);

        var oversized = new byte[gzipped.Length + 32];
        Array.Copy(gzipped, oversized, gzipped.Length);

        var success = GzipTextDecoder.TryDecompress(oversized, gzipped.Length, out var text);

        Assert.True(success);
        Assert.Equal(original, text);
    }

    private static byte[] Gzip(string text)
    {
        using var output = new MemoryStream();

        using (var gzip = new GZipStream(output, CompressionMode.Compress, leaveOpen: true))
        {
            var bytes = Encoding.UTF8.GetBytes(text);
            gzip.Write(bytes, 0, bytes.Length);
        }

        return output.ToArray();
    }
}
