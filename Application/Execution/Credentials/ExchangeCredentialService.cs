namespace Arbitrage.Api.Application.Execution.Credentials;

public sealed class ExchangeCredentialService
{
    /// <summary>
    /// Every connector credentials can be configured for, independent of which connectors are
    /// currently allowed to place real orders (<see cref="Arbitrage.Api.Application.Execution.ExecutionOptions.EnabledConnectors"/>).
    /// Keeping these separate lets operators pre-configure keys for an exchange before its
    /// trading client is implemented.
    /// </summary>
    public static readonly IReadOnlyList<string> AllConnectors = new[]
    {
        "binance_perpetual",
        "bybit_perpetual",
        "okx_perpetual",
        "bitget_perpetual",
        "gate_io_perpetual",
        "kucoin_perpetual",
        "mexc_perpetual",
        "bitmart_perpetual",
        "htx_perpetual",
        "bingx_perpetual",
    };

    private readonly IExchangeApiCredentialsRepository _repository;
    private readonly ApiSecretProtector _secretProtector;

    public ExchangeCredentialService(
        IExchangeApiCredentialsRepository repository,
        ApiSecretProtector secretProtector)
    {
        _repository = repository;
        _secretProtector = secretProtector;
    }

    public async Task<IReadOnlyList<ExchangeCredentialSummary>> GetSummariesAsync(
        CancellationToken ct)
    {
        var storedCredentials = await _repository.GetAllAsync(ct);

        var byConnector = storedCredentials.ToDictionary(
            x => x.ConnectorName,
            StringComparer.OrdinalIgnoreCase);

        var connectors = AllConnectors
            .Concat(byConnector.Keys)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(NormalizeConnectorName)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        return connectors
            .Select(connectorName =>
            {
                if (!byConnector.TryGetValue(connectorName, out var item))
                {
                    return new ExchangeCredentialSummary(
                        ConnectorName: connectorName,
                        IsConfigured: false,
                        MaskedApiKey: null,
                        IsEnabled: false,
                        LastCheckedAt: null,
                        LastCheckStatus: null,
                        LastCheckError: null,
                        CreatedAt: null,
                        UpdatedAt: null);
                }

                return ToSummary(item);
            })
            .ToList();
    }

    public async Task<ExchangeCredentialSummary> UpsertAsync(
        string connectorName,
        UpsertExchangeCredentialRequest request,
        CancellationToken ct)
    {
        var normalizedConnectorName = NormalizeConnectorName(connectorName);

        ValidateConnectorIsSupported(normalizedConnectorName);

        if (string.IsNullOrWhiteSpace(request.ApiKey))
            throw new InvalidOperationException("API key is required.");

        if (string.IsNullOrWhiteSpace(request.ApiSecret))
            throw new InvalidOperationException("API secret is required.");

        var encryptedApiSecret = _secretProtector.Protect(request.ApiSecret.Trim());
        var encryptedPassphrase = _secretProtector.ProtectOptional(request.Passphrase?.Trim());

        var stored = await _repository.UpsertAsync(
            connectorName: normalizedConnectorName,
            apiKey: request.ApiKey.Trim(),
            encryptedApiSecret: encryptedApiSecret,
            encryptedPassphrase: encryptedPassphrase,
            isEnabled: request.IsEnabled,
            ct: ct);

        return ToSummary(stored);
    }

    public async Task<bool> DeleteAsync(
        string connectorName,
        CancellationToken ct)
    {
        var normalizedConnectorName = NormalizeConnectorName(connectorName);

        ValidateConnectorIsSupported(normalizedConnectorName);

        return await _repository.DeleteAsync(
            normalizedConnectorName,
            ct);
    }

    public async Task<ExchangeApiCredentialSecret?> GetSecretAsync(
        string connectorName,
        CancellationToken ct)
    {
        var normalizedConnectorName = NormalizeConnectorName(connectorName);

        ValidateConnectorIsSupported(normalizedConnectorName);

        var stored = await _repository.GetAsync(
            normalizedConnectorName,
            ct);

        if (stored is null)
            return null;

        return new ExchangeApiCredentialSecret(
            ConnectorName: stored.ConnectorName,
            ApiKey: stored.ApiKey,
            ApiSecret: _secretProtector.Unprotect(stored.EncryptedApiSecret),
            Passphrase: _secretProtector.UnprotectOptional(stored.EncryptedPassphrase),
            IsEnabled: stored.IsEnabled);
    }

    public async Task<ExchangeCredentialSummary?> UpdateCheckResultAsync(
        string connectorName,
        string status,
        string? error,
        CancellationToken ct)
    {
        var normalizedConnectorName = NormalizeConnectorName(connectorName);

        var stored = await _repository.UpdateCheckResultAsync(
            normalizedConnectorName,
            status,
            error,
            ct);

        return stored is null
            ? null
            : ToSummary(stored);
    }

    public bool IsConnectorSupported(string connectorName)
    {
        var normalizedConnectorName = NormalizeConnectorName(connectorName);

        return AllConnectors.Contains(
            normalizedConnectorName,
            StringComparer.OrdinalIgnoreCase);
    }

    public static string NormalizeConnectorName(string connectorName)
    {
        return string.IsNullOrWhiteSpace(connectorName)
            ? ""
            : connectorName.Trim().ToLowerInvariant();
    }

    private void ValidateConnectorIsSupported(string connectorName)
    {
        if (!IsConnectorSupported(connectorName))
        {
            throw new InvalidOperationException(
                $"Connector is not enabled for execution credentials. Connector={connectorName}.");
        }
    }

    private static ExchangeCredentialSummary ToSummary(
        StoredExchangeApiCredential item)
    {
        return new ExchangeCredentialSummary(
            ConnectorName: item.ConnectorName,
            IsConfigured: true,
            MaskedApiKey: MaskApiKey(item.ApiKey),
            IsEnabled: item.IsEnabled,
            LastCheckedAt: item.LastCheckedAt,
            LastCheckStatus: item.LastCheckStatus,
            LastCheckError: item.LastCheckError,
            CreatedAt: item.CreatedAt,
            UpdatedAt: item.UpdatedAt);
    }

    private static string MaskApiKey(string apiKey)
    {
        if (string.IsNullOrWhiteSpace(apiKey))
            return "";

        var trimmed = apiKey.Trim();

        if (trimmed.Length <= 8)
            return "****";

        return $"{trimmed[..4]}****{trimmed[^4..]}";
    }
}