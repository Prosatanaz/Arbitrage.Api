using System.Security.Cryptography;
using System.Text;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.BingX;

public sealed class BingXAuthSigner
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
