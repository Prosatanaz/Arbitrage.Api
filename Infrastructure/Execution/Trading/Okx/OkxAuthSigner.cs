using System.Security.Cryptography;
using System.Text;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.Okx;

public sealed class OkxAuthSigner
{
    public string Sign(
        string apiSecret,
        string timestampIso,
        string method,
        string requestPath,
        string body)
    {
        var payload = timestampIso + method.ToUpperInvariant() + requestPath + body;

        using var hmac = new HMACSHA256(
            Encoding.UTF8.GetBytes(apiSecret));

        var hash = hmac.ComputeHash(
            Encoding.UTF8.GetBytes(payload));

        return Convert.ToBase64String(hash);
    }
}
