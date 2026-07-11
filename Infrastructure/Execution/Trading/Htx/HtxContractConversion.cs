namespace Arbitrage.Api.Infrastructure.Execution.Trading.Htx;

/// <summary>
/// Converts between the base-asset quantities the carry-trade engine works in and HTX's native
/// order/fill unit, which is whole CONTRACTS (1 contract = contract_size base-asset units). Kept as
/// a separate pure helper so the arithmetic - the part that, if wrong, silently places a wildly
/// mis-sized real order - is unit-testable without HTTP plumbing.
/// </summary>
internal static class HtxContractConversion
{
    /// <summary>
    /// Base-asset units → whole HTX contracts, rounded DOWN so the order never exceeds the intended
    /// notional. Returns 0 when the request is below one contract (the caller must reject that
    /// rather than place a 0-volume order).
    /// </summary>
    public static long BaseToContracts(decimal baseQuantity, decimal contractSize)
    {
        if (contractSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(contractSize), "HTX contract_size must be positive.");

        if (baseQuantity <= 0)
            return 0;

        return (long)Math.Floor(baseQuantity / contractSize);
    }

    /// <summary>HTX contracts → base-asset units.</summary>
    public static decimal ContractsToBase(decimal contracts, decimal contractSize)
        => contracts * contractSize;
}
