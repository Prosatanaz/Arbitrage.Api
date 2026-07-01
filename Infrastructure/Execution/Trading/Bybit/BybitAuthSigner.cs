using System.Security.Cryptography;
using System.Text;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.Bybit;

public sealed class BybitAuthSigner
{
    public string SignGet(
        string timestampMs,
        string apiKey,
        string apiSecret,
        string recvWindow,
        string queryString)
    {
        var payload = timestampMs + apiKey + recvWindow + queryString;

        using var hmac = new HMACSHA256(
            Encoding.UTF8.GetBytes(apiSecret));

        var hash = hmac.ComputeHash(
            Encoding.UTF8.GetBytes(payload));

        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}