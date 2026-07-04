using System.Security.Cryptography;
using System.Text;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.Bitget;

public sealed class BitgetAuthSigner
{
    public string Sign(
        string apiSecret,
        string timestampMs,
        string method,
        string requestPath,
        string body)
    {
        var payload = timestampMs + method.ToUpperInvariant() + requestPath + body;

        using var hmac = new HMACSHA256(
            Encoding.UTF8.GetBytes(apiSecret));

        var hash = hmac.ComputeHash(
            Encoding.UTF8.GetBytes(payload));

        return Convert.ToBase64String(hash);
    }
}
