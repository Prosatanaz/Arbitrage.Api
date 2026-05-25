using Arbitrage.Api.Application.MarketData.Streaming;
using Microsoft.AspNetCore.Mvc;

namespace Arbitrage.Api.Controllers;

[ApiController]
[Route("api/spreads")]
public sealed class SpreadController : ControllerBase
{
    private readonly LatestSpreadCandidateStore _store;

    public SpreadController(
        LatestSpreadCandidateStore store)
    {
        _store = store;
    }

    [HttpGet("current")]
    public object Current()
    {
        var now = DateTimeOffset.UtcNow;
        var snapshot = _store.GetTracked();

        return new
        {
            snapshot.UpdatedAt,
            HasData = snapshot.UpdatedAt is not null,
            Count = snapshot.Items.Count,
            Items = snapshot.Items
                .Select(x => new
                {
                    x.Candidate.TradingPair,
                    x.Candidate.LongConnector,
                    x.Candidate.ShortConnector,
                    x.Candidate.BuyPrice,
                    x.Candidate.SellPrice,
                    x.Candidate.GrossSpread,
                    x.Candidate.GrossSpreadPct,
                    x.Candidate.DetectedAt,
                    x.FirstSeenAt,
                    x.LastSeenAt,
                    AgeMs = (now - x.FirstSeenAt).TotalMilliseconds,
                    LastSeenAgoMs = (now - x.LastSeenAt).TotalMilliseconds
                })
                .OrderByDescending(x => x.GrossSpreadPct)
                .ToList()
        };
    }
}