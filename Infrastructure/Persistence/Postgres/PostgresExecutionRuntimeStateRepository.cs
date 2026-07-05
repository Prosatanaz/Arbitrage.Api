using Arbitrage.Api.Application.Execution;
using Dapper;
using Npgsql;

namespace Arbitrage.Api.Infrastructure.Persistence.Postgres;

public sealed class PostgresExecutionRuntimeStateRepository : IExecutionRuntimeStateRepository
{
    private readonly PostgresConnectionFactory _connectionFactory;

    public PostgresExecutionRuntimeStateRepository(
        PostgresConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<ExecutionRuntimeState> GetAsync(CancellationToken ct)
    {
        await using var connection = await OpenConnectionAsync(ct);

        var row = await connection.QuerySingleAsync<ExecutionRuntimeStateRow>(
            new CommandDefinition(
                SelectStateSql,
                cancellationToken: ct));

        return row.ToModel();
    }

    public async Task<ExecutionRuntimeState> ArmOnceAsync(
        decimal maxNotionalUsd,
        DateTimeOffset armedUntil,
        string reason,
        CancellationToken ct)
    {
        const string sql = """
        update execution_runtime_state
        set
            runtime_status = 'Armed',
            kill_switch_enabled = false,
            remaining_attempts = 1,
            max_notional_usd = @MaxNotionalUsd,
            armed_until = @ArmedUntil,
            last_status_reason = @Reason,
            updated_at = now()
        where id = 1
          and runtime_status <> 'Executing'
        returning
            runtime_status as "RuntimeStatus",
            kill_switch_enabled as "KillSwitchEnabled",
            manual_trading_enabled as "ManualTradingEnabled",
            remaining_attempts as "RemainingAttempts",
            max_notional_usd as "MaxNotionalUsd",
            armed_until as "ArmedUntil",
            last_attempt_id as "LastAttemptId",
            last_status_reason as "LastStatusReason",
            created_at as "CreatedAt",
            updated_at as "UpdatedAt";
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var row = await connection.QuerySingleOrDefaultAsync<ExecutionRuntimeStateRow>(
            new CommandDefinition(
                sql,
                new
                {
                    MaxNotionalUsd = maxNotionalUsd,
                    ArmedUntil = armedUntil,
                    Reason = reason
                },
                cancellationToken: ct));

        return row?.ToModel()
            ?? await GetAsync(ct);
    }

    public async Task<ExecutionRuntimeState> DisarmAsync(
        string reason,
        CancellationToken ct)
    {
        const string sql = """
        update execution_runtime_state
        set
            runtime_status = 'Disabled',
            kill_switch_enabled = true,
            remaining_attempts = 0,
            max_notional_usd = null,
            armed_until = null,
            last_status_reason = @Reason,
            updated_at = now()
        where id = 1
        returning
            runtime_status as "RuntimeStatus",
            kill_switch_enabled as "KillSwitchEnabled",
            manual_trading_enabled as "ManualTradingEnabled",
            remaining_attempts as "RemainingAttempts",
            max_notional_usd as "MaxNotionalUsd",
            armed_until as "ArmedUntil",
            last_attempt_id as "LastAttemptId",
            last_status_reason as "LastStatusReason",
            created_at as "CreatedAt",
            updated_at as "UpdatedAt";
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var row = await connection.QuerySingleAsync<ExecutionRuntimeStateRow>(
            new CommandDefinition(
                sql,
                new { Reason = reason },
                cancellationToken: ct));

        return row.ToModel();
    }

    public async Task<ExecutionRuntimeState> SetKillSwitchAsync(
        bool enabled,
        string reason,
        CancellationToken ct)
    {
        var status = enabled
            ? ExecutionRuntimeStatus.KillSwitch.ToString()
            : ExecutionRuntimeStatus.Disabled.ToString();

        const string sql = """
        update execution_runtime_state
        set
            runtime_status = @RuntimeStatus,
            kill_switch_enabled = @KillSwitchEnabled,
            remaining_attempts = 0,
            max_notional_usd = null,
            armed_until = null,
            last_status_reason = @Reason,
            updated_at = now()
        where id = 1
        returning
            runtime_status as "RuntimeStatus",
            kill_switch_enabled as "KillSwitchEnabled",
            manual_trading_enabled as "ManualTradingEnabled",
            remaining_attempts as "RemainingAttempts",
            max_notional_usd as "MaxNotionalUsd",
            armed_until as "ArmedUntil",
            last_attempt_id as "LastAttemptId",
            last_status_reason as "LastStatusReason",
            created_at as "CreatedAt",
            updated_at as "UpdatedAt";
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var row = await connection.QuerySingleAsync<ExecutionRuntimeStateRow>(
            new CommandDefinition(
                sql,
                new
                {
                    RuntimeStatus = status,
                    KillSwitchEnabled = enabled,
                    Reason = reason
                },
                cancellationToken: ct));

        return row.ToModel();
    }

    public async Task<ExecutionRuntimeState> SetManualTradingEnabledAsync(
        bool enabled,
        string reason,
        CancellationToken ct)
    {
        // Only flips the manual-trading toggle; deliberately leaves runtime_status, arming and
        // the kill switch untouched so this is an independent second gate on manual opens.
        const string sql = """
        update execution_runtime_state
        set
            manual_trading_enabled = @ManualTradingEnabled,
            last_status_reason = @Reason,
            updated_at = now()
        where id = 1
        returning
            runtime_status as "RuntimeStatus",
            kill_switch_enabled as "KillSwitchEnabled",
            manual_trading_enabled as "ManualTradingEnabled",
            remaining_attempts as "RemainingAttempts",
            max_notional_usd as "MaxNotionalUsd",
            armed_until as "ArmedUntil",
            last_attempt_id as "LastAttemptId",
            last_status_reason as "LastStatusReason",
            created_at as "CreatedAt",
            updated_at as "UpdatedAt";
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var row = await connection.QuerySingleAsync<ExecutionRuntimeStateRow>(
            new CommandDefinition(
                sql,
                new
                {
                    ManualTradingEnabled = enabled,
                    Reason = reason
                },
                cancellationToken: ct));

        return row.ToModel();
    }

    public async Task<ExecutionRuntimeState?> TryAcquireAttemptAsync(
        Guid attemptId,
        decimal notionalUsd,
        string tradingPair,
        string longConnector,
        string shortConnector,
        string reason,
        CancellationToken ct)
    {
        const string sql = """
        update execution_runtime_state
        set
            runtime_status = 'Executing',
            remaining_attempts = 0,
            armed_until = null,
            last_attempt_id = @AttemptId,
            last_status_reason = @Reason,
            updated_at = now()
        where id = 1
          and runtime_status = 'Armed'
          and kill_switch_enabled = false
          and remaining_attempts = 1
          and armed_until is not null
          and armed_until > now()
          and max_notional_usd is not null
          and @NotionalUsd <= max_notional_usd
        returning
            runtime_status as "RuntimeStatus",
            kill_switch_enabled as "KillSwitchEnabled",
            manual_trading_enabled as "ManualTradingEnabled",
            remaining_attempts as "RemainingAttempts",
            max_notional_usd as "MaxNotionalUsd",
            armed_until as "ArmedUntil",
            last_attempt_id as "LastAttemptId",
            last_status_reason as "LastStatusReason",
            created_at as "CreatedAt",
            updated_at as "UpdatedAt";
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var row = await connection.QuerySingleOrDefaultAsync<ExecutionRuntimeStateRow>(
            new CommandDefinition(
                sql,
                new
                {
                    AttemptId = attemptId,
                    NotionalUsd = notionalUsd,
                    TradingPair = tradingPair,
                    LongConnector = longConnector,
                    ShortConnector = shortConnector,
                    Reason = $"{reason} Pair={tradingPair}, Long={longConnector}, Short={shortConnector}, Notional={notionalUsd}."
                },
                cancellationToken: ct));

        return row?.ToModel();
    }

    public async Task<ExecutionRuntimeState> FinishAttemptAsync(
        Guid attemptId,
        bool succeeded,
        string reason,
        CancellationToken ct)
    {
        var status = succeeded
            ? ExecutionRuntimeStatus.DisarmedAfterAttempt.ToString()
            : ExecutionRuntimeStatus.LockedByError.ToString();

        const string sql = """
        update execution_runtime_state
        set
            runtime_status = @RuntimeStatus,
            kill_switch_enabled = true,
            remaining_attempts = 0,
            max_notional_usd = null,
            armed_until = null,
            last_status_reason = @Reason,
            updated_at = now()
        where id = 1
          and last_attempt_id = @AttemptId
        returning
            runtime_status as "RuntimeStatus",
            kill_switch_enabled as "KillSwitchEnabled",
            manual_trading_enabled as "ManualTradingEnabled",
            remaining_attempts as "RemainingAttempts",
            max_notional_usd as "MaxNotionalUsd",
            armed_until as "ArmedUntil",
            last_attempt_id as "LastAttemptId",
            last_status_reason as "LastStatusReason",
            created_at as "CreatedAt",
            updated_at as "UpdatedAt";
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var row = await connection.QuerySingleOrDefaultAsync<ExecutionRuntimeStateRow>(
            new CommandDefinition(
                sql,
                new
                {
                    RuntimeStatus = status,
                    AttemptId = attemptId,
                    Reason = reason
                },
                cancellationToken: ct));

        return row?.ToModel()
            ?? await GetAsync(ct);
    }

    public async Task<ExecutionRuntimeState> ResetUnsafeStateOnStartupAsync(
        bool killSwitchEnabled,
        string reason,
        CancellationToken ct)
    {
        var status = killSwitchEnabled
            ? ExecutionRuntimeStatus.KillSwitch.ToString()
            : ExecutionRuntimeStatus.Disabled.ToString();

        const string sql = """
        update execution_runtime_state
        set
            runtime_status = @RuntimeStatus,
            kill_switch_enabled = @KillSwitchEnabled,
            remaining_attempts = 0,
            max_notional_usd = null,
            armed_until = null,
            last_status_reason = @Reason,
            updated_at = now()
        where id = 1
          and runtime_status in ('Armed', 'Executing', 'Disabled', 'DisarmedAfterAttempt', 'LockedByError', 'KillSwitch')
        returning
            runtime_status as "RuntimeStatus",
            kill_switch_enabled as "KillSwitchEnabled",
            manual_trading_enabled as "ManualTradingEnabled",
            remaining_attempts as "RemainingAttempts",
            max_notional_usd as "MaxNotionalUsd",
            armed_until as "ArmedUntil",
            last_attempt_id as "LastAttemptId",
            last_status_reason as "LastStatusReason",
            created_at as "CreatedAt",
            updated_at as "UpdatedAt";
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var row = await connection.QuerySingleAsync<ExecutionRuntimeStateRow>(
            new CommandDefinition(
                sql,
                new
                {
                    RuntimeStatus = status,
                    KillSwitchEnabled = killSwitchEnabled,
                    Reason = reason
                },
                cancellationToken: ct));

        return row.ToModel();
    }

    private async Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var connection = _connectionFactory.CreateConnection();

        await connection.OpenAsync(ct);

        return connection;
    }

    private const string SelectStateSql = """
    select
        runtime_status as "RuntimeStatus",
        kill_switch_enabled as "KillSwitchEnabled",
        manual_trading_enabled as "ManualTradingEnabled",
        remaining_attempts as "RemainingAttempts",
        max_notional_usd as "MaxNotionalUsd",
        armed_until as "ArmedUntil",
        last_attempt_id as "LastAttemptId",
        last_status_reason as "LastStatusReason",
        created_at as "CreatedAt",
        updated_at as "UpdatedAt"
    from execution_runtime_state
    where id = 1;
    """;

    private sealed class ExecutionRuntimeStateRow
    {
        public string RuntimeStatus { get; set; } = "";

        public bool KillSwitchEnabled { get; set; }

        public bool ManualTradingEnabled { get; set; }

        public int RemainingAttempts { get; set; }

        public decimal? MaxNotionalUsd { get; set; }

        public DateTimeOffset? ArmedUntil { get; set; }

        public Guid? LastAttemptId { get; set; }

        public string? LastStatusReason { get; set; }

        public DateTimeOffset CreatedAt { get; set; }

        public DateTimeOffset UpdatedAt { get; set; }

        public ExecutionRuntimeState ToModel()
        {
            return new ExecutionRuntimeState(
                RuntimeStatus: Enum.TryParse<ExecutionRuntimeStatus>(
                    RuntimeStatus,
                    ignoreCase: true,
                    out var parsedStatus)
                    ? parsedStatus
                    : ExecutionRuntimeStatus.LockedByError,
                KillSwitchEnabled: KillSwitchEnabled,
                ManualTradingEnabled: ManualTradingEnabled,
                RemainingAttempts: RemainingAttempts,
                MaxNotionalUsd: MaxNotionalUsd,
                ArmedUntil: ArmedUntil,
                LastAttemptId: LastAttemptId,
                LastStatusReason: LastStatusReason,
                CreatedAt: CreatedAt,
                UpdatedAt: UpdatedAt);
        }
    }
}