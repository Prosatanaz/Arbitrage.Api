using Microsoft.AspNetCore.DataProtection;

namespace Arbitrage.Api.Application.Execution.Credentials;

public sealed class ApiSecretProtector
{
    private readonly IDataProtector _protector;

    public ApiSecretProtector(IDataProtectionProvider provider)
    {
        _protector = provider.CreateProtector("exchange-api-credentials-v1");
    }

    public string Protect(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException("Secret value is empty.", nameof(value));

        return _protector.Protect(value);
    }

    public string? ProtectOptional(string? value)
    {
        return string.IsNullOrWhiteSpace(value)
            ? null
            : _protector.Protect(value);
    }

    public string Unprotect(string protectedValue)
    {
        if (string.IsNullOrWhiteSpace(protectedValue))
            throw new ArgumentException("Protected value is empty.", nameof(protectedValue));

        return _protector.Unprotect(protectedValue);
    }

    public string? UnprotectOptional(string? protectedValue)
    {
        return string.IsNullOrWhiteSpace(protectedValue)
            ? null
            : _protector.Unprotect(protectedValue);
    }
}