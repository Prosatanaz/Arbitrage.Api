using Arbitrage.Api.Application.Instruments;
using Arbitrage.Api.Application.SignalQuality;
using Arbitrage.Api.Infrastructure.Persistence.Postgres;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Arbitrage.Api.Controllers;

[ApiController]
[Route("api/analytics/opportunities")]
public sealed class AnalyticsController : ControllerBase
{
    private readonly PostgresConnectionFactory _connectionFactory;
    private readonly PostgresOptions _postgresOptions;
    private readonly SignalQualityOptions _signalQualityOptions;
    private readonly InstrumentFilterOptions _instrumentFilterOptions;

    public AnalyticsController(
        PostgresConnectionFactory connectionFactory,
        IOptions<PostgresOptions> postgresOptions,
        IOptions<SignalQualityOptions> signalQualityOptions,
        IOptions<InstrumentFilterOptions> instrumentFilterOptions)
    {
        _connectionFactory = connectionFactory;
        _postgresOptions = postgresOptions.Value;
        _signalQualityOptions = signalQualityOptions.Value;
        _instrumentFilterOptions = instrumentFilterOptions.Value;
    }

    [HttpGet("summary")]
    public async Task<IActionResult> Summary(
        [FromQuery] int hours = 12,
        [FromQuery] decimal? minEntryEdgePct = null,
        CancellationToken ct = default)
    {
        if (PostgresDisabled() is { } disabled)
            return disabled;

        var parameters = BuildParameters(
            hours,
            minEntryEdgePct);

        var sql = EpisodesCte + """
        ,
        qualified as (
            select
                *,
                notional_usd * entry_net_edge_pct / 100 as entry_profit_usd,
                notional_usd * avg_net_edge_pct / 100 as avg_profit_usd,
                notional_usd * min_net_edge_pct / 100 as conservative_profit_usd,
                notional_usd * max_net_edge_pct / 100 as best_case_profit_usd
            from episodes
            where samples >= @MinEpisodeSamples
              and duration >= (@MinEpisodeDurationSeconds * interval '1 second')
              and entry_net_edge_pct >= @MinEntryEdgePct
        )
        select
            count(*) as "TradesCount",

            coalesce(round(sum(entry_profit_usd), 6), 0) as "TotalEntryProfitUsd",
            coalesce(round(sum(avg_profit_usd), 6), 0) as "TotalAvgProfitUsd",
            coalesce(round(sum(conservative_profit_usd), 6), 0) as "TotalConservativeProfitUsd",
            coalesce(round(sum(best_case_profit_usd), 6), 0) as "TotalBestCaseProfitUsd",

            coalesce(round(avg(entry_net_edge_pct), 6), 0) as "AvgEntryEdgePct",
            coalesce(round(max(entry_net_edge_pct), 6), 0) as "MaxEntryEdgePct",
            coalesce(round(min(entry_net_edge_pct), 6), 0) as "MinEntryEdgePct",

            min(entry_at) as "FirstEntryAt",
            max(exit_or_last_seen_at) as "LastExitOrSeenAt"
        from qualified;
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var result = await connection.QuerySingleAsync<AnalyticsSummaryRow>(
            new CommandDefinition(
                sql,
                parameters,
                cancellationToken: ct));

        return Ok(result);
    }

    [HttpGet("timeline")]
    public async Task<IActionResult> Timeline(
        [FromQuery] int hours = 12,
        [FromQuery] decimal? minEntryEdgePct = null,
        CancellationToken ct = default)
    {
        if (PostgresDisabled() is { } disabled)
            return disabled;

        var parameters = BuildParameters(
            hours,
            minEntryEdgePct);

        var sql = EpisodesCte + """
        ,
        qualified as (
            select
                *,
                notional_usd * entry_net_edge_pct / 100 as entry_profit_usd,
                notional_usd * min_net_edge_pct / 100 as conservative_profit_usd
            from episodes
            where samples >= @MinEpisodeSamples
              and duration >= (@MinEpisodeDurationSeconds * interval '1 second')
              and entry_net_edge_pct >= @MinEntryEdgePct
        )
        select
            date_trunc('hour', entry_at) as "Hour",

            count(*) as "TradesCount",

            coalesce(round(sum(entry_profit_usd), 6), 0) as "EntryProfitUsd",
            coalesce(round(sum(conservative_profit_usd), 6), 0) as "ConservativeProfitUsd",

            coalesce(round(avg(entry_net_edge_pct), 6), 0) as "AvgEntryEdgePct",
            coalesce(round(max(entry_net_edge_pct), 6), 0) as "MaxEntryEdgePct"
        from qualified
        group by 1
        order by 1;
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var result = (await connection.QueryAsync<AnalyticsTimelineRow>(
            new CommandDefinition(
                sql,
                parameters,
                cancellationToken: ct)))
            .ToList();

        return Ok(result);
    }

    [HttpGet("top")]
    public async Task<IActionResult> Top(
        [FromQuery] int hours = 12,
        [FromQuery] decimal? minEntryEdgePct = null,
        [FromQuery] int limit = 50,
        CancellationToken ct = default)
    {
        if (PostgresDisabled() is { } disabled)
            return disabled;

        var parameters = BuildParameters(
            hours,
            minEntryEdgePct,
            limit);

        var sql = EpisodesCte + """
        ,
        qualified as (
            select
                *,
                notional_usd * entry_net_edge_pct / 100 as entry_profit_usd,
                notional_usd * min_net_edge_pct / 100 as conservative_profit_usd
            from episodes
            where samples >= @MinEpisodeSamples
              and duration >= (@MinEpisodeDurationSeconds * interval '1 second')
              and entry_net_edge_pct >= @MinEntryEdgePct
        )
        select
            trading_pair as "TradingPair",
            long_connector as "LongConnector",
            short_connector as "ShortConnector",

            count(*) as "Trades",

            coalesce(round(sum(entry_profit_usd), 6), 0) as "EntryProfitUsd",
            coalesce(round(sum(conservative_profit_usd), 6), 0) as "ConservativeProfitUsd",

            coalesce(round(avg(entry_net_edge_pct), 6), 0) as "AvgEntryEdgePct",
            coalesce(round(max(entry_net_edge_pct), 6), 0) as "MaxEntryEdgePct",
            coalesce(round(min(entry_net_edge_pct), 6), 0) as "MinEntryEdgePct",

            min(entry_at) as "FirstEntryAt",
            max(exit_or_last_seen_at) as "LastSeenAt"
        from qualified
        group by
            trading_pair,
            long_connector,
            short_connector
        order by
            "ConservativeProfitUsd" desc,
            "EntryProfitUsd" desc
        limit @Limit;
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var result = (await connection.QueryAsync<TopOpportunityRow>(
            new CommandDefinition(
                sql,
                parameters,
                cancellationToken: ct)))
            .ToList();

        return Ok(result);
    }

    [HttpGet("thresholds")]
    public async Task<IActionResult> Thresholds(
        [FromQuery] int hours = 12,
        CancellationToken ct = default)
    {
        if (PostgresDisabled() is { } disabled)
            return disabled;

        var parameters = BuildParameters(
            hours,
            minEntryEdgePct: null);

        var sql = EpisodesCte + """
        ,
        thresholds(min_entry_edge_pct) as (
            values
                (0.05::numeric),
                (0.10::numeric),
                (0.15::numeric),
                (0.20::numeric),
                (0.30::numeric),
                (0.50::numeric)
        )
        select
            t.min_entry_edge_pct as "MinEntryEdgePct",

            count(e.trading_pair) as "TradesCount",

            coalesce(round(sum(e.notional_usd * e.entry_net_edge_pct / 100), 6), 0) as "EntryProfitUsd",
            coalesce(round(sum(e.notional_usd * e.min_net_edge_pct / 100), 6), 0) as "ConservativeProfitUsd",

            coalesce(round(avg(e.entry_net_edge_pct), 6), 0) as "AvgEntryEdgePct",
            coalesce(round(max(e.entry_net_edge_pct), 6), 0) as "MaxEntryEdgePct"
        from thresholds t
        left join episodes e
          on e.entry_net_edge_pct >= t.min_entry_edge_pct
         and e.samples >= @MinEpisodeSamples
         and e.duration >= (@MinEpisodeDurationSeconds * interval '1 second')
        group by
            t.min_entry_edge_pct
        order by
            t.min_entry_edge_pct;
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var result = (await connection.QueryAsync<ThresholdSensitivityRow>(
            new CommandDefinition(
                sql,
                parameters,
                cancellationToken: ct)))
            .ToList();

        return Ok(result);
    }

    [HttpGet("concurrency")]
    public async Task<IActionResult> Concurrency(
        [FromQuery] int hours = 12,
        [FromQuery] decimal? minEntryEdgePct = null,
        CancellationToken ct = default)
    {
        if (PostgresDisabled() is { } disabled)
            return disabled;

        var parameters = BuildParameters(
            hours,
            minEntryEdgePct);

        var sql = EpisodesCte + """
        ,
        qualified as (
            select *
            from episodes
            where samples >= @MinEpisodeSamples
              and duration >= (@MinEpisodeDurationSeconds * interval '1 second')
              and entry_net_edge_pct >= @MinEntryEdgePct
        ),
        timeline as (
            select entry_at as t, 1 as delta from qualified
            union all
            select exit_or_last_seen_at as t, -1 as delta from qualified
        ),
        running as (
            select
                t,
                sum(delta) over (
                    order by t
                    rows between unbounded preceding and current row
                ) as open_trades
            from timeline
        )
        select
            coalesce(max(open_trades), 0) as "MaxConcurrentTrades",
            coalesce(round(avg(open_trades), 2), 0) as "AvgConcurrentTrades",

            coalesce(max(open_trades), 0) * 100 as "PeakNotionalUsd",
            coalesce(max(open_trades), 0) * 100 * 2 as "PeakGrossTwoLegExposureUsd"
        from running;
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var result = await connection.QuerySingleAsync<ConcurrencyRow>(
            new CommandDefinition(
                sql,
                parameters,
                cancellationToken: ct));

        return Ok(result);
    }

    [HttpGet("statuses")]
    public async Task<IActionResult> Statuses(
        [FromQuery] int hours = 12,
        CancellationToken ct = default)
    {
        if (PostgresDisabled() is { } disabled)
            return disabled;

        var parameters = BuildParameters(
            hours,
            minEntryEdgePct: null);

        var sql = """
        select
            status as "Status",
            count(*) as "Rows",
            round(count(*) * 100.0 / sum(count(*)) over (), 2) as "Pct"
        from validated_opportunities
        where validated_at >= now() - (@Hours * interval '1 hour')
        group by status
        order by "Rows" desc;
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var result = (await connection.QueryAsync<StatusDistributionRow>(
            new CommandDefinition(
                sql,
                parameters,
                cancellationToken: ct)))
            .ToList();

        return Ok(result);
    }

    [HttpGet("depth-issues")]
    public async Task<IActionResult> DepthIssues(
        [FromQuery] int hours = 12,
        [FromQuery] int limit = 50,
        CancellationToken ct = default)
    {
        if (PostgresDisabled() is { } disabled)
            return disabled;

        var parameters = BuildParameters(
            hours,
            minEntryEdgePct: null,
            limit);

        var sql = """
        select
            case
                when status = 'MissingBuyDepth' then long_connector
                when status = 'MissingSellDepth' then short_connector
                when status = 'StaleBuyDepth' then long_connector
                when status = 'StaleSellDepth' then short_connector
                else null
            end as "ProblematicConnector",

            status as "Status",

            count(*) as "Rows"
        from validated_opportunities
        where validated_at >= now() - (@Hours * interval '1 hour')
          and status in (
              'MissingBuyDepth',
              'MissingSellDepth',
              'StaleBuyDepth',
              'StaleSellDepth'
          )
        group by
            "ProblematicConnector",
            status
        order by
            "Rows" desc
        limit @Limit;
        """;

        await using var connection = await OpenConnectionAsync(ct);

        var result = (await connection.QueryAsync<DepthIssueRow>(
            new CommandDefinition(
                sql,
                parameters,
                cancellationToken: ct)))
            .ToList();

        return Ok(result);
    }

    private AnalyticsQueryParameters BuildParameters(
        int hours,
        decimal? minEntryEdgePct,
        int limit = 50)
    {
        var safeHours = Math.Clamp(
            hours,
            1,
            168);

        var safeLimit = Math.Clamp(
            limit,
            1,
            500);

        var blockedBaseAssets = _instrumentFilterOptions.BlockedBaseAssets
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var blockedTradingPairsFromInstrumentFilter = _instrumentFilterOptions.BlockedTradingPairs
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var blockedTradingPairsFromSignalQuality = _signalQualityOptions.BlockedTradingPairs
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var blockedTradingPairs = blockedTradingPairsFromInstrumentFilter
            .Concat(blockedTradingPairsFromSignalQuality)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var allowedTradingPairsFromInstrumentFilter = _instrumentFilterOptions.AllowedTradingPairs
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var allowedTradingPairsFromSignalQuality = _signalQualityOptions.AllowedTradingPairs
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var allowedTradingPairs = allowedTradingPairsFromInstrumentFilter
            .Concat(allowedTradingPairsFromSignalQuality)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var blockedConnectors = _signalQualityOptions.BlockedConnectors
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return new AnalyticsQueryParameters(
            Hours: safeHours,
            MinEntryEdgePct: minEntryEdgePct ?? _signalQualityOptions.MinCandidateNetEdgePct,
            MinEpisodeSamples: Math.Max(1, _signalQualityOptions.MinEpisodeSamples),
            MinEpisodeDurationSeconds: Math.Max(0, _signalQualityOptions.MinEpisodeDurationSeconds),
            EpisodeMaxGapSeconds: Math.Max(1, _signalQualityOptions.EpisodeMaxGapSeconds),
            MaxBuySlippagePct: _signalQualityOptions.MaxBuySlippagePct,
            MaxSellSlippagePct: _signalQualityOptions.MaxSellSlippagePct,
            BlockedBaseAssets: blockedBaseAssets,
            BlockedTradingPairs: blockedTradingPairs,
            AllowedTradingPairs: allowedTradingPairs,
            AllowedTradingPairsCount: allowedTradingPairs.Length,
            BlockedConnectors: blockedConnectors,
            Limit: safeLimit);
    }

    private async Task<NpgsqlConnection> OpenConnectionAsync(
        CancellationToken ct)
    {
        var connection = _connectionFactory.CreateConnection();

        await connection.OpenAsync(ct);

        return connection;
    }

    private IActionResult? PostgresDisabled()
    {
        if (_postgresOptions.Enabled)
            return null;

        return StatusCode(
            StatusCodes.Status503ServiceUnavailable,
            new
            {
                Error = "Postgres persistence is disabled."
            });
    }

    private const string EpisodesCte = """
    with valid_rows as (
        select
            v.*,
            split_part(v.trading_pair, '-', 1) as base_asset
        from validated_opportunities v
        where v.validated_at >= now() - (@Hours * interval '1 hour')
          and v.status = 'Valid'
          and v.net_edge_pct > 0

          and not (split_part(v.trading_pair, '-', 1) = any(@BlockedBaseAssets))
          and not (v.trading_pair = any(@BlockedTradingPairs))
          and (@AllowedTradingPairsCount = 0 or v.trading_pair = any(@AllowedTradingPairs))

          and not (v.long_connector = any(@BlockedConnectors))
          and not (v.short_connector = any(@BlockedConnectors))

          and (v.buy_slippage_pct is null or v.buy_slippage_pct <= @MaxBuySlippagePct)
          and (v.sell_slippage_pct is null or v.sell_slippage_pct <= @MaxSellSlippagePct)
    ),
    ordered as (
        select
            *,
            lag(validated_at) over (
                partition by trading_pair, long_connector, short_connector
                order by validated_at
            ) as prev_validated_at
        from valid_rows
    ),
    marked as (
        select
            *,
            sum(
                case
                    when prev_validated_at is null then 1
                    when validated_at - prev_validated_at > (@EpisodeMaxGapSeconds * interval '1 second') then 1
                    else 0
                end
            ) over (
                partition by trading_pair, long_connector, short_connector
                order by validated_at
            ) as episode_id
        from ordered
    ),
    episodes as (
        select
            trading_pair,
            long_connector,
            short_connector,
            episode_id,

            min(validated_at) as entry_at,
            max(validated_at) as exit_or_last_seen_at,
            max(validated_at) - min(validated_at) as duration,

            count(*) as samples,

            (array_agg(notional_usd order by validated_at))[1] as notional_usd,

            (array_agg(net_edge_pct order by validated_at))[1] as entry_net_edge_pct,
            (array_agg(net_edge_pct order by validated_at desc))[1] as latest_net_edge_pct,

            avg(net_edge_pct) as avg_net_edge_pct,
            max(net_edge_pct) as max_net_edge_pct,
            min(net_edge_pct) as min_net_edge_pct,

            (array_agg(buy_average_price order by validated_at))[1] as entry_buy_price,
            (array_agg(sell_average_price order by validated_at))[1] as entry_sell_price,

            (array_agg(buy_average_price order by validated_at desc))[1] as latest_buy_price,
            (array_agg(sell_average_price order by validated_at desc))[1] as latest_sell_price,

            (array_agg(buy_slippage_pct order by validated_at))[1] as entry_buy_slippage_pct,
            (array_agg(sell_slippage_pct order by validated_at))[1] as entry_sell_slippage_pct
        from marked
        group by
            trading_pair,
            long_connector,
            short_connector,
            episode_id
    )
    """;

    private sealed record AnalyticsQueryParameters(
        int Hours,
        decimal MinEntryEdgePct,
        int MinEpisodeSamples,
        int MinEpisodeDurationSeconds,
        int EpisodeMaxGapSeconds,
        decimal MaxBuySlippagePct,
        decimal MaxSellSlippagePct,
        string[] BlockedBaseAssets,
        string[] BlockedTradingPairs,
        string[] AllowedTradingPairs,
        int AllowedTradingPairsCount,
        string[] BlockedConnectors,
        int Limit);

    private sealed class AnalyticsSummaryRow
    {
        public long TradesCount { get; set; }

        public decimal TotalEntryProfitUsd { get; set; }

        public decimal TotalAvgProfitUsd { get; set; }

        public decimal TotalConservativeProfitUsd { get; set; }

        public decimal TotalBestCaseProfitUsd { get; set; }

        public decimal AvgEntryEdgePct { get; set; }

        public decimal MaxEntryEdgePct { get; set; }

        public decimal MinEntryEdgePct { get; set; }

        public DateTimeOffset? FirstEntryAt { get; set; }

        public DateTimeOffset? LastExitOrSeenAt { get; set; }
    }

    private sealed class AnalyticsTimelineRow
    {
        public DateTimeOffset Hour { get; set; }

        public long TradesCount { get; set; }

        public decimal EntryProfitUsd { get; set; }

        public decimal ConservativeProfitUsd { get; set; }

        public decimal AvgEntryEdgePct { get; set; }

        public decimal MaxEntryEdgePct { get; set; }
    }

    private sealed class TopOpportunityRow
    {
        public string TradingPair { get; set; } = "";

        public string LongConnector { get; set; } = "";

        public string ShortConnector { get; set; } = "";

        public long Trades { get; set; }

        public decimal EntryProfitUsd { get; set; }

        public decimal ConservativeProfitUsd { get; set; }

        public decimal AvgEntryEdgePct { get; set; }

        public decimal MaxEntryEdgePct { get; set; }

        public decimal MinEntryEdgePct { get; set; }

        public DateTimeOffset FirstEntryAt { get; set; }

        public DateTimeOffset LastSeenAt { get; set; }
    }

    private sealed class ThresholdSensitivityRow
    {
        public decimal MinEntryEdgePct { get; set; }

        public long TradesCount { get; set; }

        public decimal EntryProfitUsd { get; set; }

        public decimal ConservativeProfitUsd { get; set; }

        public decimal AvgEntryEdgePct { get; set; }

        public decimal MaxEntryEdgePct { get; set; }
    }

    private sealed class ConcurrencyRow
    {
        public long MaxConcurrentTrades { get; set; }

        public decimal AvgConcurrentTrades { get; set; }

        public long PeakNotionalUsd { get; set; }

        public long PeakGrossTwoLegExposureUsd { get; set; }
    }

    private sealed class StatusDistributionRow
    {
        public string Status { get; set; } = "";

        public long Rows { get; set; }

        public decimal Pct { get; set; }
    }

    private sealed class DepthIssueRow
    {
        public string ProblematicConnector { get; set; } = "";

        public string Status { get; set; } = "";

        public long Rows { get; set; }
    }
}