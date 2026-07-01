using System.IO.Compression;
using System.Text;

namespace Arbitrage.Api.Infrastructure.MarketData.Streams;

public static class GzipTextDecoder
{
    public static bool TryDecompress(byte[] buffer, int count, out string text)
    {
        try
        {
            using var compressed = new MemoryStream(buffer, 0, count);
            using var gzip = new GZipStream(compressed, CompressionMode.Decompress);
            using var reader = new StreamReader(gzip, Encoding.UTF8);

            text = reader.ReadToEnd();
            return true;
        }
        catch
        {
            text = "";
            return false;
        }
    }
}
