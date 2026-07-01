using Dapper;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Persistence.Postgres;

public sealed class ExchangeApiCredentialsSchemaInitializer : IHostedService
{
    private readonly PostgresConnectionFactory _connectionFactory;
    private readonly PostgresOptions _options;
    private readonly ILogger<ExchangeApiCredentialsSchemaInitializer> _logger;

    public ExchangeApiCredentialsSchemaInitializer(
        PostgresConnectionFactory connectionFactory,
        IOptions<PostgresOptions> options,
        ILogger<ExchangeApiCredentialsSchemaInitializer> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Exchange API credentials schema initialization skipped because Postgres is disabled.");
            return;
        }

        await using var connection = _connectionFactory.CreateConnection();

        await connection.OpenAsync(cancellationToken);

        await connection.ExecuteAsync(
            new CommandDefinition(
                """
                create table if not exists exchange_api_credentials (
                    connector_name text primary key,

                    api_key text not null,
                    encrypted_api_secret text not null,
                    encrypted_passphrase text null,

                    is_enabled boolean not null default true,

                    last_checked_at timestamptz null,
                    last_check_status text null,
                    last_check_error text null,

                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                );
                """,
                cancellationToken: cancellationToken));

        _logger.LogInformation("Exchange API credentials schema initialized.");
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}