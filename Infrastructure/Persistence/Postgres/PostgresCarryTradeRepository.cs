using Arbitrage.Api.Application.Execution.CarryTrades;
using Dapper;
using Npgsql;

namespace Arbitrage.Api.Infrastructure.Persistence.Postgres;

public sealed class PostgresCarryTradeRepository : ICarryTradeRepository
{
    private readonly PostgresConnectionFactory _connectionFactory;

    public PostgresCarryTradeRepository(
        PostgresConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<CarryTrade> InsertOpenAsync(
        OpenCarryTradeRequest request,
        CancellationToken ct)
    {
        const string sql = """
        insert into carry_trades (
            id, attempt_id, trading_pair, long_connector, short_connector,
            notional_usd, base_quantity,
            entry_long_price, entry_short_price, entry_fees_usd, entry_net_edge_pct,
            opened_at, status
        )
        values (
            @Id, @AttemptId, @TradingPair, @LongConnector, @ShortConnector,
            @NotionalUsd, @BaseQuantity,
            @EntryLongPrice, @EntryShortPrice, @EntryFeesUsd, @EntryNetEdgePct,
            now(), 'Open'
        )
        returning
        """ + "\n" + SelectColumns;

        await using var connection = await OpenConnectionAsync(ct);

        var row = await connection.QuerySingleAsync<CarryTradeRow>(
            new CommandDefinition(
                sql,
                new
                {
                    Id = Guid.NewGuid(),
                    request.AttemptId,
                    request.TradingPair,
                    request.LongConnector,
                    request.ShortConnector,
                    request.NotionalUsd,
                    request.BaseQuantity,
                    request.EntryLongPrice,
                    request.EntryShortPrice,
                    request.EntryFeesUsd,
                    request.EntryNetEdgePct
                },
                cancellationToken: ct));

        return row.ToModel();
    }

    public async Task<CarryTrade?> GetOpenAsync(CancellationToken ct)
    {
        const string sql = """
        select
        """ + "\n" + SelectColumns + """
        from carry_trades
        where status in ('Open', 'Closing')
        order by opened_at desc
        limit 1;
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var row = await connection.QuerySingleOrDefaultAsync<CarryTradeRow>(
            new CommandDefinition(
                sql,
                cancellationToken: ct));

        return row?.ToModel();
    }

    public async Task<CarryTrade?> GetByIdAsync(
        Guid id,
        CancellationToken ct)
    {
        const string sql = """
        select
        """ + "\n" + SelectColumns + """
        from carry_trades
        where id = @Id;
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var row = await connection.QuerySingleOrDefaultAsync<CarryTradeRow>(
            new CommandDefinition(
                sql,
                new { Id = id },
                cancellationToken: ct));

        return row?.ToModel();
    }

    public async Task<bool> MarkClosingAsync(
        Guid id,
        CancellationToken ct)
    {
        const string sql = """
        update carry_trades
        set status = 'Closing', updated_at = now()
        where id = @Id
          and status = 'Open';
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var affected = await connection.ExecuteAsync(
            new CommandDefinition(
                sql,
                new { Id = id },
                cancellationToken: ct));

        return affected > 0;
    }

    public async Task<CarryTrade> MarkClosedAsync(
        CloseCarryTradeRequest request,
        CancellationToken ct)
    {
        const string sql = """
        update carry_trades
        set
            status = 'Closed',
            exit_long_price = @ExitLongPrice,
            exit_short_price = @ExitShortPrice,
            exit_fees_usd = @ExitFeesUsd,
            realized_pnl_usd = @RealizedPnlUsd,
            close_reason = @CloseReason,
            closed_at = now(),
            updated_at = now()
        where id = @Id
        returning
        """ + "\n" + SelectColumns;

        await using var connection = await OpenConnectionAsync(ct);

        var row = await connection.QuerySingleAsync<CarryTradeRow>(
            new CommandDefinition(
                sql,
                new
                {
                    request.Id,
                    request.ExitLongPrice,
                    request.ExitShortPrice,
                    request.ExitFeesUsd,
                    request.RealizedPnlUsd,
                    request.CloseReason
                },
                cancellationToken: ct));

        return row.ToModel();
    }

    public async Task<CarryTrade> MarkFailedAsync(
        Guid id,
        string error,
        CancellationToken ct)
    {
        const string sql = """
        update carry_trades
        set
            status = 'Failed',
            error = @Error,
            closed_at = now(),
            updated_at = now()
        where id = @Id
        returning
        """ + "\n" + SelectColumns;

        await using var connection = await OpenConnectionAsync(ct);

        var row = await connection.QuerySingleAsync<CarryTradeRow>(
            new CommandDefinition(
                sql,
                new { Id = id, Error = error },
                cancellationToken: ct));

        return row.ToModel();
    }

    public async Task<IReadOnlyList<CarryTrade>> ListRecentAsync(
        int hours,
        int limit,
        CancellationToken ct)
    {
        const string sql = """
        select
        """ + "\n" + SelectColumns + """
        from carry_trades
        where opened_at >= now() - (@Hours * interval '1 hour')
        order by opened_at desc
        limit @Limit;
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var rows = (await connection.QueryAsync<CarryTradeRow>(
            new CommandDefinition(
                sql,
                new { Hours = hours, Limit = limit },
                cancellationToken: ct)))
            .ToList();

        return rows.Select(x => x.ToModel()).ToList();
    }

    private async Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var connection = _connectionFactory.CreateConnection();

        await connection.OpenAsync(ct);

        return connection;
    }

    private const string SelectColumns = """
        id as "Id",
        attempt_id as "AttemptId",
        trading_pair as "TradingPair",
        long_connector as "LongConnector",
        short_connector as "ShortConnector",
        notional_usd as "NotionalUsd",
        base_quantity as "BaseQuantity",
        entry_long_price as "EntryLongPrice",
        entry_short_price as "EntryShortPrice",
        entry_fees_usd as "EntryFeesUsd",
        entry_net_edge_pct as "EntryNetEdgePct",
        opened_at as "OpenedAt",
        status as "Status",
        exit_long_price as "ExitLongPrice",
        exit_short_price as "ExitShortPrice",
        exit_fees_usd as "ExitFeesUsd",
        close_reason as "CloseReason",
        closed_at as "ClosedAt",
        realized_pnl_usd as "RealizedPnlUsd",
        error as "Error"

        """;

    private sealed class CarryTradeRow
    {
        public Guid Id { get; set; }

        public Guid AttemptId { get; set; }

        public string TradingPair { get; set; } = "";

        public string LongConnector { get; set; } = "";

        public string ShortConnector { get; set; } = "";

        public decimal NotionalUsd { get; set; }

        public decimal BaseQuantity { get; set; }

        public decimal EntryLongPrice { get; set; }

        public decimal EntryShortPrice { get; set; }

        public decimal EntryFeesUsd { get; set; }

        public decimal EntryNetEdgePct { get; set; }

        public DateTimeOffset OpenedAt { get; set; }

        public string Status { get; set; } = "";

        public decimal? ExitLongPrice { get; set; }

        public decimal? ExitShortPrice { get; set; }

        public decimal? ExitFeesUsd { get; set; }

        public string? CloseReason { get; set; }

        public DateTimeOffset? ClosedAt { get; set; }

        public decimal? RealizedPnlUsd { get; set; }

        public string? Error { get; set; }

        public CarryTrade ToModel()
        {
            return new CarryTrade(
                Id: Id,
                AttemptId: AttemptId,
                TradingPair: TradingPair,
                LongConnector: LongConnector,
                ShortConnector: ShortConnector,
                NotionalUsd: NotionalUsd,
                BaseQuantity: BaseQuantity,
                EntryLongPrice: EntryLongPrice,
                EntryShortPrice: EntryShortPrice,
                EntryFeesUsd: EntryFeesUsd,
                EntryNetEdgePct: EntryNetEdgePct,
                OpenedAt: OpenedAt,
                Status: Enum.TryParse<CarryTradeStatus>(Status, ignoreCase: true, out var parsedStatus)
                    ? parsedStatus
                    : CarryTradeStatus.Failed,
                ExitLongPrice: ExitLongPrice,
                ExitShortPrice: ExitShortPrice,
                ExitFeesUsd: ExitFeesUsd,
                CloseReason: CloseReason,
                ClosedAt: ClosedAt,
                RealizedPnlUsd: RealizedPnlUsd,
                Error: Error);
        }
    }
}
