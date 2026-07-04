using System.Security.Cryptography;
using System.Text;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.GateIo;

public sealed class GateIoAuthSigner
{
    public string HashBody(string body)
    {
        var hash = SHA512.HashData(Encoding.UTF8.GetBytes(body));

        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    public string Sign(
        string apiSecret,
        string method,
        string urlPath,
        string queryString,
        string hashedBody,
        string timestampSeconds)
    {
        var payload = string.Join(
            "\n",
            method.ToUpperInvariant(),
            urlPath,
            queryString,
            hashedBody,
            timestampSeconds);

        using var hmac = new HMACSHA512(
            Encoding.UTF8.GetBytes(apiSecret));

        var hash = hmac.ComputeHash(
            Encoding.UTF8.GetBytes(payload));

        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
