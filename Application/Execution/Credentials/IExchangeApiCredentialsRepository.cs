namespace Arbitrage.Api.Application.Execution.Credentials;

public interface IExchangeApiCredentialsRepository
{
    Task<IReadOnlyList<StoredExchangeApiCredential>> GetAllAsync(
        CancellationToken ct);

    Task<StoredExchangeApiCredential?> GetAsync(
        string connectorName,
        CancellationToken ct);

    Task<StoredExchangeApiCredential> UpsertAsync(
        string connectorName,
        string apiKey,
        string encryptedApiSecret,
        string? encryptedPassphrase,
        bool isEnabled,
        CancellationToken ct);

    Task<bool> DeleteAsync(
        string connectorName,
        CancellationToken ct);

    Task<StoredExchangeApiCredential?> UpdateCheckResultAsync(
        string connectorName,
        string status,
        string? error,
        CancellationToken ct);

    Task<StoredExchangeApiCredential?> SetEnabledAsync(
        string connectorName,
        bool isEnabled,
        CancellationToken ct);
}