using System.Collections.Concurrent;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Application.Persistence;
using Dapper;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.Persistence.Postgres;

public sealed class PostgresValidatedOpportunityPersistence : IValidatedOpportunityPersistence
{
    private readonly ConcurrentDictionary<PersistenceSignalKey, DateTimeOffset> _lastPersistedAtBySignal = new();

    private readonly PostgresConnectionFactory _connectionFactory;
    private readonly PostgresOptions _options;
    private readonly ILogger<PostgresValidatedOpportunityPersistence> _logger;

    public PostgresValidatedOpportunityPersistence(
        PostgresConnectionFactory connectionFactory,
        IOptions<PostgresOptions> options,
        ILogger<PostgresValidatedOpportunityPersistence> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task SaveAsync(
        DateTimeOffset validatedAt,
        IReadOnlyList<ValidatedDepthCandidate> items,
        CancellationToken ct)
    {
        if (!_options.Enabled)
            return;

        if (items.Count == 0)
            return;

        var itemsToPersist = ApplyDedupe(
            validatedAt,
            items);

        if (itemsToPersist.Count == 0)
        {
            _logger.LogDebug(
                "Validated opportunities persistence skipped by dedupe. InputCount={InputCount}, ValidatedAt={ValidatedAt}",
                items.Count,
                validatedAt);

            return;
        }

        var rows = itemsToPersist
            .Select(item => new
            {
                ValidatedAt = validatedAt,
                DetectedAt = item.Candidate.DetectedAt,

                TradingPair = item.Candidate.TradingPair,
                LongConnector = item.Candidate.LongConnector,
                ShortConnector = item.Candidate.ShortConnector,

                Status = item.Status.ToString(),

                NotionalUsd = item.NotionalUsd,
                RequestedBaseAmount = item.RequestedBaseAmount,

                BuyAveragePrice = item.BuyQuote?.AveragePrice,
                SellAveragePrice = item.SellQuote?.AveragePrice,

                BuyQuoteAmount = item.BuyQuote?.QuoteAmount,
                SellQuoteAmount = item.SellQuote?.QuoteAmount,

                GrossSpreadPct = item.GrossSpreadPct,
                EstimatedFeesPct = item.EstimatedFeesPct,
                NetEdgePct = item.NetEdgePct,

                BuySlippagePct = item.BuyQuote?.SlippagePct,
                SellSlippagePct = item.SellQuote?.SlippagePct,

                BuyLevelsUsed = item.BuyQuote?.LevelsUsed,
                SellLevelsUsed = item.SellQuote?.LevelsUsed,

                IsBuyFullyFillable = item.BuyQuote?.IsFullyFillable,
                IsSellFullyFillable = item.SellQuote?.IsFullyFillable,

                Reason = item.Reason
            })
            .ToList();

        try
        {
            await using var connection = _connectionFactory.CreateConnection();

            await connection.OpenAsync(ct);

            await connection.ExecuteAsync(
                new CommandDefinition(
                    """
                    insert into validated_opportunities (
                        validated_at,
                        detected_at,

                        trading_pair,
                        long_connector,
                        short_connector,

                        status,

                        notional_usd,
                        requested_base_amount,

                        buy_average_price,
                        sell_average_price,

                        buy_quote_amount,
                        sell_quote_amount,

                        gross_spread_pct,
                        estimated_fees_pct,
                        net_edge_pct,

                        buy_slippage_pct,
                        sell_slippage_pct,

                        buy_levels_used,
                        sell_levels_used,

                        is_buy_fully_fillable,
                        is_sell_fully_fillable,

                        reason
                    )
                    values (
                        @ValidatedAt,
                        @DetectedAt,

                        @TradingPair,
                        @LongConnector,
                        @ShortConnector,

                        @Status,

                        @NotionalUsd,
                        @RequestedBaseAmount,

                        @BuyAveragePrice,
                        @SellAveragePrice,

                        @BuyQuoteAmount,
                        @SellQuoteAmount,

                        @GrossSpreadPct,
                        @EstimatedFeesPct,
                        @NetEdgePct,

                        @BuySlippagePct,
                        @SellSlippagePct,

                        @BuyLevelsUsed,
                        @SellLevelsUsed,

                        @IsBuyFullyFillable,
                        @IsSellFullyFillable,

                        @Reason
                    );
                    """,
                    rows,
                    cancellationToken: ct));

            _logger.LogDebug(
                "Validated opportunities persisted. InputCount={InputCount}, PersistedCount={PersistedCount}, ValidatedAt={ValidatedAt}",
                items.Count,
                rows.Count,
                validatedAt);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to persist validated opportunities. Count={Count}",
                rows.Count);
        }
    }

    private IReadOnlyList<ValidatedDepthCandidate> ApplyDedupe(
        DateTimeOffset validatedAt,
        IReadOnlyList<ValidatedDepthCandidate> items)
    {
        var persistSameSignalInterval = TimeSpan.FromSeconds(
            Math.Max(1, _options.PersistSameSignalIntervalSeconds));

        var result = new List<ValidatedDepthCandidate>();

        foreach (var item in items)
        {
            var key = new PersistenceSignalKey(
                TradingPair: item.Candidate.TradingPair,
                LongConnector: item.Candidate.LongConnector,
                ShortConnector: item.Candidate.ShortConnector,
                Status: item.Status.ToString());

            if (!_lastPersistedAtBySignal.TryGetValue(key, out var lastPersistedAt))
            {
                _lastPersistedAtBySignal[key] = validatedAt;
                result.Add(item);
                continue;
            }

            if (validatedAt - lastPersistedAt < persistSameSignalInterval)
                continue;

            _lastPersistedAtBySignal[key] = validatedAt;
            result.Add(item);
        }

        return result;
    }

    private readonly record struct PersistenceSignalKey(
        string TradingPair,
        string LongConnector,
        string ShortConnector,
        string Status);
}