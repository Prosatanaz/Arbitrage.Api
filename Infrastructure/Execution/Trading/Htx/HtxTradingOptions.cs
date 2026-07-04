namespace Arbitrage.Api.Infrastructure.Execution.Trading.Htx;

public sealed class HtxTradingOptions
{
    public const string SectionName = "HtxTrading";

    public string BaseUrl { get; init; } = "https://api.hbdm.com";

    public string SignatureHost { get; init; } = "api.hbdm.com";

    public string AccountInfoPath { get; init; } = "/linear-swap-api/v3/unified_account_info";

    public string ContractInfoPath { get; init; } = "/linear-swap-api/v1/swap_contract_info";

    public string OrderPath { get; init; } = "/linear-swap-api/v1/swap_order";

    public string OrderInfoPath { get; init; } = "/linear-swap-api/v1/swap_order_info";
}