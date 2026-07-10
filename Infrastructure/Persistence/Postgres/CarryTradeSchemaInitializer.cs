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

                -- Additive decision-basis columns (nullable so old rows stay valid).
                alter table carry_trades add column if not exists entry_gross_spread_pct numeric(38, 18) null;
                alter table carry_trades add column if not exists entry_estimated_fees_pct numeric(38, 18) null;
                alter table carry_trades add column if not exists entry_reference_price numeric(38, 18) null;
                alter table carry_trades add column if not exists exit_net_edge_pct numeric(38, 18) null;

                -- Per-leg, per-exchange execution detail. Append-only.
                create table if not exists carry_trade_legs (
                    id bigint generated always as identity primary key,
                    trade_id uuid not null references carry_trades (id),

                    phase text not null,
                    connector text not null,
                    role text not null,
                    side text not null,
                    reduce_only boolean not null,

                    requested_quantity numeric(38, 18) not null,
                    filled_quantity numeric(38, 18) not null,
                    average_fill_price numeric(38, 18) not null,
                    fee_paid_usd numeric(38, 18) not null,

                    exchange_order_id text not null,
                    client_order_id text not null,
                    status text not null,

                    filled_at timestamptz not null,
                    created_at timestamptz not null default now()
                );

                create index if not exists ix_carry_trade_legs_trade_id on carry_trade_legs (trade_id);

                -- Append-only action / decision log. trade_id is nullable so pre-open decisions
                -- can be recorded even before a trade row exists.
                create table if not exists carry_trade_events (
                    id bigint generated always as identity primary key,
                    trade_id uuid null,
                    attempt_id uuid null,

                    event_type text not null,
                    reason text not null,
                    details jsonb null,

                    created_at timestamptz not null default now()
                );

                create index if not exists ix_carry_trade_events_trade_id on carry_trade_events (trade_id);
                create index if not exists ix_carry_trade_events_created_at on carry_trade_events (created_at desc);
                """,
                cancellationToken: cancellationToken));

        _logger.LogInformation("Carry trade schema initialized.");
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
