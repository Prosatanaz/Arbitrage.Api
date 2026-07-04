using System.Security.Cryptography;
using System.Text;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.Binance;

public sealed class BinanceAuthSigner
{
    public string Sign(
        string apiSecret,
        string queryString)
    {
        using var hmac = new HMACSHA256(
            Encoding.UTF8.GetBytes(apiSecret));

        var hash = hmac.ComputeHash(
            Encoding.UTF8.GetBytes(queryString));

        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
