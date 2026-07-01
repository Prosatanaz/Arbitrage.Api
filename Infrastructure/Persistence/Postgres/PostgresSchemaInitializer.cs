using Dapper;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Persistence.Postgres;

public sealed class PostgresSchemaInitializer : IHostedService
{
    private readonly PostgresConnectionFactory _connectionFactory;
    private readonly PostgresOptions _options;
    private readonly ILogger<PostgresSchemaInitializer> _logger;

    public PostgresSchemaInitializer(
        PostgresConnectionFactory connectionFactory,
        IOptions<PostgresOptions> options,
        ILogger<PostgresSchemaInitializer> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Postgres persistence is disabled.");
            return;
        }

        await using var connection = _connectionFactory.CreateConnection();

        await connection.OpenAsync(cancellationToken);

        await connection.ExecuteAsync(
            new CommandDefinition(
                """
                create table if not exists validated_opportunities (
                    id bigserial primary key,

                    validated_at timestamptz not null,
                    detected_at timestamptz not null,

                    trading_pair text not null,
                    long_connector text not null,
                    short_connector text not null,

                    status text not null,

                    notional_usd numeric(38, 18) not null,
                    requested_base_amount numeric(38, 18) null,

                    buy_average_price numeric(38, 18) null,
                    sell_average_price numeric(38, 18) null,

                    buy_quote_amount numeric(38, 18) null,
                    sell_quote_amount numeric(38, 18) null,

                    gross_spread_pct numeric(38, 18) null,
                    estimated_fees_pct numeric(38, 18) null,
                    net_edge_pct numeric(38, 18) null,

                    buy_slippage_pct numeric(38, 18) null,
                    sell_slippage_pct numeric(38, 18) null,

                    buy_levels_used int null,
                    sell_levels_used int null,

                    is_buy_fully_fillable boolean null,
                    is_sell_fully_fillable boolean null,

                    reason text null,

                    created_at timestamptz not null default now()
                );

                create index if not exists ix_validated_opportunities_validated_at
                    on validated_opportunities (validated_at desc);

                create index if not exists ix_validated_opportunities_pair_time
                    on validated_opportunities (trading_pair, validated_at desc);

                create index if not exists ix_validated_opportunities_connectors_time
                    on validated_opportunities (long_connector, short_connector, validated_at desc);

                create index if not exists ix_validated_opportunities_status_time
                    on validated_opportunities (status, validated_at desc);

                create index if not exists ix_validated_opportunities_positive_valid_time
                    on validated_opportunities (validated_at desc)
                    where status = 'Valid' and net_edge_pct > 0;
                """,
                cancellationToken: cancellationToken));

        _logger.LogInformation("Postgres schema initialized.");
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}