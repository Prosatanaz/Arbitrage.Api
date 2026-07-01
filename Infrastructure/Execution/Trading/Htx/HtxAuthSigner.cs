using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Arbitrage.Api.Infrastructure.Execution.Trading.Htx;

public sealed class HtxAuthSigner
{
    public string BuildSignedQueryString(
        string method,
        string host,
        string path,
        string accessKey,
        string secretKey,
        IReadOnlyDictionary<string, string>? extraParameters = null)
    {
        var parameters = new SortedDictionary<string, string>(StringComparer.Ordinal)
        {
            ["AccessKeyId"] = accessKey,
            ["SignatureMethod"] = "HmacSHA256",
            ["SignatureVersion"] = "2",
            ["Timestamp"] = DateTime.UtcNow.ToString(
                "yyyy-MM-dd'T'HH:mm:ss",
                CultureInfo.InvariantCulture)
        };

        if (extraParameters is not null)
        {
            foreach (var item in extraParameters)
            {
                parameters[item.Key] = item.Value;
            }
        }

        var canonicalQuery = BuildCanonicalQueryString(parameters);

        var payload = string.Join(
            "\n",
            method.ToUpperInvariant(),
            host,
            path,
            canonicalQuery);

        using var hmac = new HMACSHA256(
            Encoding.UTF8.GetBytes(secretKey));

        var hash = hmac.ComputeHash(
            Encoding.UTF8.GetBytes(payload));

        var signature = Convert.ToBase64String(hash);

        parameters["Signature"] = signature;

        return BuildCanonicalQueryString(parameters);
    }

    private static string BuildCanonicalQueryString(
        SortedDictionary<string, string> parameters)
    {
        return string.Join(
            "&",
            parameters.Select(x =>
                $"{UrlEncode(x.Key)}={UrlEncode(x.Value)}"));
    }

    private static string UrlEncode(string value)
    {
        return Uri.EscapeDataString(value)
            .Replace("%7E", "~", StringComparison.Ordinal);
    }
}