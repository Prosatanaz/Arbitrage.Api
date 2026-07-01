using Dapper;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Persistence.Postgres;

public sealed class ExecutionSchemaInitializer : IHostedService
{
    private readonly PostgresConnectionFactory _connectionFactory;
    private readonly PostgresOptions _options;
    private readonly ILogger<ExecutionSchemaInitializer> _logger;

    public ExecutionSchemaInitializer(
        PostgresConnectionFactory connectionFactory,
        IOptions<PostgresOptions> options,
        ILogger<ExecutionSchemaInitializer> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Execution schema initialization skipped because Postgres is disabled.");
            return;
        }

        await using var connection = _connectionFactory.CreateConnection();

        await connection.OpenAsync(cancellationToken);

        await connection.ExecuteAsync(
            new CommandDefinition(
                """
                create table if not exists execution_runtime_state (
                    id int primary key,

                    runtime_status text not null,
                    kill_switch_enabled boolean not null,

                    remaining_attempts int not null,
                    max_notional_usd numeric(38, 18) null,
                    armed_until timestamptz null,

                    last_attempt_id uuid null,
                    last_status_reason text null,

                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now(),

                    constraint execution_runtime_state_singleton check (id = 1),
                    constraint execution_runtime_state_remaining_attempts_non_negative check (remaining_attempts >= 0)
                );

                insert into execution_runtime_state (
                    id,
                    runtime_status,
                    kill_switch_enabled,
                    remaining_attempts,
                    last_status_reason
                )
                values (
                    1,
                    'KillSwitch',
                    true,
                    0,
                    'Execution runtime state initialized.'
                )
                on conflict (id) do nothing;
                """,
                cancellationToken: cancellationToken));

        _logger.LogInformation("Execution schema initialized.");
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}