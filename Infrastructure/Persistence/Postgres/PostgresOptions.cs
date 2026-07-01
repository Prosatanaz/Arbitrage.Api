namespace Arbitrage.Api.Infrastructure.Persistence.Postgres;

public sealed class PostgresOptions
{
    public const string SectionName = "Postgres";

    public bool Enabled { get; init; } = false;

    public string ConnectionString { get; init; } = "";

    public int PersistSameSignalIntervalSeconds { get; init; } = 10;
}