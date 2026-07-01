namespace Arbitrage.Api.Application.Execution.Credentials;

public sealed class UpsertExchangeCredentialRequest
{
    public string ApiKey { get; init; } = "";

    public string ApiSecret { get; init; } = "";

    public string? Passphrase { get; init; }

    public bool IsEnabled { get; init; } = true;
}

public sealed record ExchangeCredentialSummary(
    string ConnectorName,
    bool IsConfigured,
    string? MaskedApiKey,
    bool IsEnabled,
    DateTimeOffset? LastCheckedAt,
    string? LastCheckStatus,
    string? LastCheckError,
    DateTimeOffset? CreatedAt,
    DateTimeOffset? UpdatedAt);

public sealed record ExchangeApiCredentialSecret(
    string ConnectorName,
    string ApiKey,
    string ApiSecret,
    string? Passphrase,
    bool IsEnabled);

public sealed class StoredExchangeApiCredential
{
    public string ConnectorName { get; set; } = "";

    public string ApiKey { get; set; } = "";

    public string EncryptedApiSecret { get; set; } = "";

    public string? EncryptedPassphrase { get; set; }

    public bool IsEnabled { get; set; }

    public DateTimeOffset? LastCheckedAt { get; set; }

    public string? LastCheckStatus { get; set; }

    public string? LastCheckError { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    public DateTimeOffset UpdatedAt { get; set; }
}