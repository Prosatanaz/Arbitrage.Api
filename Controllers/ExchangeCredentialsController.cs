using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Microsoft.AspNetCore.Mvc;

namespace Arbitrage.Api.Controllers;

[ApiController]
[Route("api/execution/credentials")]
public sealed class ExchangeCredentialsController : ControllerBase
{
    private readonly ExchangeCredentialService _credentialService;
    private readonly ExchangeTradingClientRegistry _tradingClientRegistry;

    public ExchangeCredentialsController(
        ExchangeCredentialService credentialService,
        ExchangeTradingClientRegistry tradingClientRegistry)
    {
        _credentialService = credentialService;
        _tradingClientRegistry = tradingClientRegistry;
    }

    [HttpGet]
    public async Task<IReadOnlyList<ExchangeCredentialSummary>> List(
        CancellationToken ct)
    {
        return await _credentialService.GetSummariesAsync(ct);
    }

    [HttpPut("{connectorName}")]
    public async Task<IActionResult> Upsert(
        string connectorName,
        [FromBody] UpsertExchangeCredentialRequest request,
        CancellationToken ct)
    {
        try
        {
            var result = await _credentialService.UpsertAsync(
                connectorName,
                request,
                ct);

            return Ok(result);
        }
        catch (InvalidOperationException ex)
        {
            return BadRequest(new
            {
                Error = ex.Message
            });
        }
    }

    [HttpDelete("{connectorName}")]
    public async Task<IActionResult> Delete(
        string connectorName,
        CancellationToken ct)
    {
        try
        {
            var deleted = await _credentialService.DeleteAsync(
                connectorName,
                ct);

            return Ok(new
            {
                ConnectorName = ExchangeCredentialService.NormalizeConnectorName(connectorName),
                Deleted = deleted
            });
        }
        catch (InvalidOperationException ex)
        {
            return BadRequest(new
            {
                Error = ex.Message
            });
        }
    }

    [HttpPost("{connectorName}/check")]
    public async Task<IActionResult> Check(
        string connectorName,
        CancellationToken ct)
    {
        var normalizedConnectorName = ExchangeCredentialService.NormalizeConnectorName(connectorName);

        if (!_credentialService.IsConnectorSupported(normalizedConnectorName))
        {
            return BadRequest(new
            {
                ConnectorName = normalizedConnectorName,
                IsConnected = false,
                Status = "UnsupportedConnector",
                Error = "Connector is not enabled for execution credentials."
            });
        }

        ExchangeApiCredentialSecret? credentials;

        try
        {
            credentials = await _credentialService.GetSecretAsync(
                normalizedConnectorName,
                ct);
        }
        catch (Exception ex)
        {
            await _credentialService.UpdateCheckResultAsync(
                normalizedConnectorName,
                "SecretDecryptFailed",
                ex.Message,
                ct);

            return Ok(new
            {
                ConnectorName = normalizedConnectorName,
                IsConnected = false,
                Status = "SecretDecryptFailed",
                Error = ex.Message,
                CheckedAt = DateTimeOffset.UtcNow
            });
        }

        if (credentials is null)
        {
            return Ok(new
            {
                ConnectorName = normalizedConnectorName,
                IsConnected = false,
                Status = "NotConfigured",
                Error = "API credentials are not configured.",
                CheckedAt = DateTimeOffset.UtcNow
            });
        }

        if (!credentials.IsEnabled)
        {
            await _credentialService.UpdateCheckResultAsync(
                normalizedConnectorName,
                "Disabled",
                "API credentials are disabled.",
                ct);

            return Ok(new
            {
                ConnectorName = normalizedConnectorName,
                IsConnected = false,
                Status = "Disabled",
                Error = "API credentials are disabled.",
                CheckedAt = DateTimeOffset.UtcNow
            });
        }

        if (!_tradingClientRegistry.TryGetClient(
                normalizedConnectorName,
                out var tradingClient))
        {
            await _credentialService.UpdateCheckResultAsync(
                normalizedConnectorName,
                "ClientNotImplemented",
                "Trading client is not implemented for this connector.",
                ct);

            return Ok(new
            {
                ConnectorName = normalizedConnectorName,
                IsConnected = false,
                Status = "ClientNotImplemented",
                Error = "Trading client is not implemented for this connector.",
                CheckedAt = DateTimeOffset.UtcNow
            });
        }

        try
        {
            var checkResult = await tradingClient.CheckConnectionAsync(
                credentials,
                ct);

            await _credentialService.UpdateCheckResultAsync(
                normalizedConnectorName,
                checkResult.Status,
                checkResult.Error,
                ct);

            return Ok(checkResult);
        }
        catch (Exception ex)
        {
            await _credentialService.UpdateCheckResultAsync(
                normalizedConnectorName,
                "ConnectionCheckFailed",
                ex.Message,
                ct);

            return Ok(new
            {
                ConnectorName = normalizedConnectorName,
                IsConnected = false,
                Status = "ConnectionCheckFailed",
                Error = ex.Message,
                CheckedAt = DateTimeOffset.UtcNow
            });
        }
    }
}