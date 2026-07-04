using Arbitrage.Api.Application.Execution.Credentials;
using Dapper;
using Npgsql;

namespace Arbitrage.Api.Infrastructure.Persistence.Postgres;

public sealed class PostgresExchangeApiCredentialsRepository : IExchangeApiCredentialsRepository
{
    private readonly PostgresConnectionFactory _connectionFactory;

    public PostgresExchangeApiCredentialsRepository(
        PostgresConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<IReadOnlyList<StoredExchangeApiCredential>> GetAllAsync(
        CancellationToken ct)
    {
        const string sql = """
        select
            connector_name as "ConnectorName",
            api_key as "ApiKey",
            encrypted_api_secret as "EncryptedApiSecret",
            encrypted_passphrase as "EncryptedPassphrase",
            is_enabled as "IsEnabled",
            last_checked_at as "LastCheckedAt",
            last_check_status as "LastCheckStatus",
            last_check_error as "LastCheckError",
            created_at as "CreatedAt",
            updated_at as "UpdatedAt"
        from exchange_api_credentials
        order by connector_name;
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var result = await connection.QueryAsync<StoredExchangeApiCredential>(
            new CommandDefinition(
                sql,
                cancellationToken: ct));

        return result.ToList();
    }

    public async Task<StoredExchangeApiCredential?> GetAsync(
        string connectorName,
        CancellationToken ct)
    {
        const string sql = """
        select
            connector_name as "ConnectorName",
            api_key as "ApiKey",
            encrypted_api_secret as "EncryptedApiSecret",
            encrypted_passphrase as "EncryptedPassphrase",
            is_enabled as "IsEnabled",
            last_checked_at as "LastCheckedAt",
            last_check_status as "LastCheckStatus",
            last_check_error as "LastCheckError",
            created_at as "CreatedAt",
            updated_at as "UpdatedAt"
        from exchange_api_credentials
        where connector_name = @ConnectorName;
        """;

        await using var connection = await OpenConnectionAsync(ct);

        return await connection.QuerySingleOrDefaultAsync<StoredExchangeApiCredential>(
            new CommandDefinition(
                sql,
                new { ConnectorName = connectorName },
                cancellationToken: ct));
    }

    public async Task<StoredExchangeApiCredential> UpsertAsync(
        string connectorName,
        string apiKey,
        string encryptedApiSecret,
        string? encryptedPassphrase,
        bool isEnabled,
        CancellationToken ct)
    {
        const string sql = """
        insert into exchange_api_credentials (
            connector_name,
            api_key,
            encrypted_api_secret,
            encrypted_passphrase,
            is_enabled,
            updated_at
        )
        values (
            @ConnectorName,
            @ApiKey,
            @EncryptedApiSecret,
            @EncryptedPassphrase,
            @IsEnabled,
            now()
        )
        on conflict (connector_name) do update
        set
            api_key = excluded.api_key,
            encrypted_api_secret = excluded.encrypted_api_secret,
            encrypted_passphrase = excluded.encrypted_passphrase,
            is_enabled = excluded.is_enabled,
            last_check_status = null,
            last_check_error = null,
            updated_at = now()
        returning
            connector_name as "ConnectorName",
            api_key as "ApiKey",
            encrypted_api_secret as "EncryptedApiSecret",
            encrypted_passphrase as "EncryptedPassphrase",
            is_enabled as "IsEnabled",
            last_checked_at as "LastCheckedAt",
            last_check_status as "LastCheckStatus",
            last_check_error as "LastCheckError",
            created_at as "CreatedAt",
            updated_at as "UpdatedAt";
        """;

        await using var connection = await OpenConnectionAsync(ct);

        return await connection.QuerySingleAsync<StoredExchangeApiCredential>(
            new CommandDefinition(
                sql,
                new
                {
                    ConnectorName = connectorName,
                    ApiKey = apiKey,
                    EncryptedApiSecret = encryptedApiSecret,
                    EncryptedPassphrase = encryptedPassphrase,
                    IsEnabled = isEnabled
                },
                cancellationToken: ct));
    }

    public async Task<bool> DeleteAsync(
        string connectorName,
        CancellationToken ct)
    {
        const string sql = """
        delete from exchange_api_credentials
        where connector_name = @ConnectorName;
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var affected = await connection.ExecuteAsync(
            new CommandDefinition(
                sql,
                new { ConnectorName = connectorName },
                cancellationToken: ct));

        return affected > 0;
    }

    public async Task<StoredExchangeApiCredential?> UpdateCheckResultAsync(
        string connectorName,
        string status,
        string? error,
        CancellationToken ct)
    {
        const string sql = """
        update exchange_api_credentials
        set
            last_checked_at = now(),
            last_check_status = @Status,
            last_check_error = @Error,
            updated_at = now()
        where connector_name = @ConnectorName
        returning
            connector_name as "ConnectorName",
            api_key as "ApiKey",
            encrypted_api_secret as "EncryptedApiSecret",
            encrypted_passphrase as "EncryptedPassphrase",
            is_enabled as "IsEnabled",
            last_checked_at as "LastCheckedAt",
            last_check_status as "LastCheckStatus",
            last_check_error as "LastCheckError",
            created_at as "CreatedAt",
            updated_at as "UpdatedAt";
        """;

        await using var connection = await OpenConnectionAsync(ct);

        return await connection.QuerySingleOrDefaultAsync<StoredExchangeApiCredential>(
            new CommandDefinition(
                sql,
                new
                {
                    ConnectorName = connectorName,
                    Status = status,
                    Error = error
                },
                cancellationToken: ct));
    }

    public async Task<StoredExchangeApiCredential?> SetEnabledAsync(
        string connectorName,
        bool isEnabled,
        CancellationToken ct)
    {
        const string sql = """
        update exchange_api_credentials
        set
            is_enabled = @IsEnabled,
            updated_at = now()
        where connector_name = @ConnectorName
        returning
            connector_name as "ConnectorName",
            api_key as "ApiKey",
            encrypted_api_secret as "EncryptedApiSecret",
            encrypted_passphrase as "EncryptedPassphrase",
            is_enabled as "IsEnabled",
            last_checked_at as "LastCheckedAt",
            last_check_status as "LastCheckStatus",
            last_check_error as "LastCheckError",
            created_at as "CreatedAt",
            updated_at as "UpdatedAt";
        """;

        await using var connection = await OpenConnectionAsync(ct);

        return await connection.QuerySingleOrDefaultAsync<StoredExchangeApiCredential>(
            new CommandDefinition(
                sql,
                new
                {
                    ConnectorName = connectorName,
                    IsEnabled = isEnabled
                },
                cancellationToken: ct));
    }

    private async Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var connection = _connectionFactory.CreateConnection();

        await connection.OpenAsync(ct);

        return connection;
    }
}