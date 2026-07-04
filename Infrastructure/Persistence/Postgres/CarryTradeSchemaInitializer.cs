using Dapper;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Persistence.Postgres;

public sealed class CarryTradeSchemaInitializer : IHostedService
{
    private readonly PostgresConnectionFactory _connectionFactory;
    private readonly PostgresOptions _options;
    private readonly ILogger<CarryTradeSchemaInitializer> _logger;

    public CarryTradeSchemaInitializer(
        PostgresConnectionFactory connectionFactory,
        IOptions<PostgresOptions> options,
        ILogger<CarryTradeSchemaInitializer> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Carry trade schema initialization skipped because Postgres is disabled.");
            return;
        }

        await using var connection = _connectionFactory.CreateConnection();

        await connection.OpenAsync(cancellationToken);

        await connection.ExecuteAsync(
            new CommandDefinition(
                """
                create table if not exists carry_trades (
                    id uuid primary key,
                    attempt_id uuid not null,

                    trading_pair text not null,
                    long_connector text not null,
                    short_connector text not null,

                    notional_usd numeric(38, 18) not null,
                    base_quantity numeric(38, 18) not null,

                    entry_long_price numeric(38, 18) not null,
                    entry_short_price numeric(38, 18) not null,
                    entry_fees_usd numeric(38, 18) not null,
                    entry_net_edge_pct numeric(38, 18) not null,

                    opened_at timestamptz not null,

                    status text not null,

                    exit_long_price numeric(38, 18) null,
                    exit_short_price numeric(38, 18) null,
                    exit_fees_usd numeric(38, 18) null,
                    close_reason text null,
                    closed_at timestamptz null,
                    realized_pnl_usd numeric(38, 18) null,

                    error text null,

                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                );

                create index if not exists ix_carry_trades_status on carry_trades (status);
                create index if not exists ix_carry_trades_opened_at on carry_trades (opened_at desc);
                """,
                cancellationToken: cancellationToken));

        _logger.LogInformation("Carry trade schema initialized.");
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
