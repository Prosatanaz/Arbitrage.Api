using Arbitrage.Api.Application.Execution;
using Arbitrage.Api.Infrastructure.Persistence.Postgres;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Infrastructure.HostedServices;

public sealed class ExecutionStartupSafetyHostedService : IHostedService
{
    private readonly IExecutionRuntimeStateRepository _repository;
    private readonly ExecutionOptions _executionOptions;
    private readonly PostgresOptions _postgresOptions;
    private readonly ILogger<ExecutionStartupSafetyHostedService> _logger;

    public ExecutionStartupSafetyHostedService(
        IExecutionRuntimeStateRepository repository,
        IOptions<ExecutionOptions> executionOptions,
        IOptions<PostgresOptions> postgresOptions,
        ILogger<ExecutionStartupSafetyHostedService> logger)
    {
        _repository = repository;
        _executionOptions = executionOptions.Value;
        _postgresOptions = postgresOptions.Value;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_postgresOptions.Enabled)
        {
            _logger.LogInformation("Execution startup safety skipped because Postgres is disabled.");
            return;
        }

        var state = await _repository.ResetUnsafeStateOnStartupAsync(
            killSwitchEnabled: _executionOptions.KillSwitchEnabledOnStartup,
            reason: "Execution state reset on application startup.",
            ct: cancellationToken);

        _logger.LogWarning(
            "Execution state reset on startup. RuntimeStatus={RuntimeStatus}, KillSwitchEnabled={KillSwitchEnabled}, RemainingAttempts={RemainingAttempts}",
            state.RuntimeStatus,
            state.KillSwitchEnabled,
            state.RemainingAttempts);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}