using Microsoft.Extensions.Options;
using Npgsql;

namespace Arbitrage.Api.Infrastructure.Persistence.Postgres;

public sealed class PostgresConnectionFactory
{
    private readonly PostgresOptions _options;

    public PostgresConnectionFactory(
        IOptions<PostgresOptions> options)
    {
        _options = options.Value;
    }

    public NpgsqlConnection CreateConnection()
    {
        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
        {
            throw new InvalidOperationException(
                "Postgres connection string is empty.");
        }

        return new NpgsqlConnection(_options.ConnectionString);
    }
}