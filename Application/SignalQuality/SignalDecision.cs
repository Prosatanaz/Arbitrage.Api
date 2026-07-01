namespace Arbitrage.Api.Application.SignalQuality;

public enum SignalDecision
{
    Ignored = 0,
    Candidate = 1,
    Alert = 2,
    Blocked = 3
}