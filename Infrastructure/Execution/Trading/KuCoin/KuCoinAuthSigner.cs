using System.Security.Cryptography;
using System.Text;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.KuCoin;

public sealed class KuCoinAuthSigner
{
    public string Sign(
        string apiSecret,
        string timestampMs,
        string method,
        string endpointWithQuery,
        string body)
    {
        var payload = timestampMs + method.ToUpperInvariant() + endpointWithQuery + body;

        return HmacBase64(
            apiSecret,
            payload);
    }

    public string SignPassphrase(
        string apiSecret,
        string passphrase)
    {
        return HmacBase64(
            apiSecret,
            passphrase);
    }

    private static string HmacBase64(
        string apiSecret,
        string payload)
    {
        using var hmac = new HMACSHA256(
            Encoding.UTF8.GetBytes(apiSecret));

        var hash = hmac.ComputeHash(
            Encoding.UTF8.GetBytes(payload));

        return Convert.ToBase64String(hash);
    }
}
