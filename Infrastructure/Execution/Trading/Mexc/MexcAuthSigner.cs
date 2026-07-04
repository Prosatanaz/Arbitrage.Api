using System.Security.Cryptography;
using System.Text;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.Mexc;

public sealed class MexcAuthSigner
{
    public string Sign(
        string apiKey,
        string apiSecret,
        string timestampMs,
        string paramString)
    {
        var payload = apiKey + timestampMs + paramString;

        using var hmac = new HMACSHA256(
            Encoding.UTF8.GetBytes(apiSecret));

        var hash = hmac.ComputeHash(
            Encoding.UTF8.GetBytes(payload));

        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
