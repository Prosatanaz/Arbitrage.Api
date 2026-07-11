using Arbitrage.Api.Infrastructure.Execution.Trading.Htx;
using Xunit;

namespace Arbitrage.Api.Tests.Infrastructure.Execution.Trading.Htx;

public class HtxContractConversionTests
{
    [Fact]
    public void BaseToContracts_ContractSizeOne_IsIdentity()
    {
        // TIA/DYDX had contract_size = 1, which is why the missing conversion went unnoticed.
        Assert.Equal(24, HtxContractConversion.BaseToContracts(24m, 1m));
    }

    [Fact]
    public void BaseToContracts_RealHUsdtCase_DoesNotOverorder100x()
    {
        // H-USDT contract_size = 100. ~$10 at ~$0.0686 => ~145 base units. The bug placed 145
        // CONTRACTS (= 14,500 H ≈ $960); the correct order is floor(145/100) = 1 contract.
        Assert.Equal(1, HtxContractConversion.BaseToContracts(145m, 100m));
    }

    [Fact]
    public void BaseToContracts_RoundsDown_NeverExceedsNotional()
    {
        Assert.Equal(2, HtxContractConversion.BaseToContracts(299m, 100m));
    }

    [Fact]
    public void BaseToContracts_BelowOneContract_ReturnsZeroSoCallerRejects()
    {
        Assert.Equal(0, HtxContractConversion.BaseToContracts(50m, 100m));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void BaseToContracts_NonPositiveQuantity_ReturnsZero(int baseQuantity)
    {
        Assert.Equal(0, HtxContractConversion.BaseToContracts(baseQuantity, 100m));
    }

    [Fact]
    public void BaseToContracts_NonPositiveContractSize_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => HtxContractConversion.BaseToContracts(100m, 0m));
    }

    [Fact]
    public void ContractsToBase_RoundTripsBackToBaseUnits()
    {
        // 1 contract of H-USDT reports as 1 in trade_volume but is 100 base units to the rest
        // of the system - so leg reconciliation and close sizing stay in one unit.
        Assert.Equal(100m, HtxContractConversion.ContractsToBase(1m, 100m));
    }
}
