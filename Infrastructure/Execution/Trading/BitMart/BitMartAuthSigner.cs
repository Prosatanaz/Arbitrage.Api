using System.Security.Cryptography;
using System.Text;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.BitMart;

public sealed class BitMartAuthSigner
{
    public string Sign(
        string apiSecret,
        string timestampMs,
        string memo,
        string body)
    {
        var payload = $"{timestampMs}#{memo}#{body}";

        using var hmac = new HMACSHA256(
            Encoding.UTF8.GetBytes(apiSecret));

        var hash = hmac.ComputeHash(
            Encoding.UTF8.GetBytes(payload));

        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
