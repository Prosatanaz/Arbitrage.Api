namespace Arbitrage.Api.Infrastructure.Execution.Trading.GateIo;

public sealed class GateIoTradingOptions
{
    public const string SectionName = "GateIoTrading";

    public string BaseUrl { get; init; } = "https://api.gateio.ws";
}
