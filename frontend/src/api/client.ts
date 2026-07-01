import type {
    ActiveSignalsResponse,
    ActiveSignalsSummaryResponse,
    AnalyticsSummaryResponse,
    DepthIssueItem,
    SystemHealthResponse,
    TopOpportunityItem,
} from './types';

async function getJson<T>(url: string): Promise<T> {
    const response = await fetch(url);

    if (!response.ok) {
        const text = await response.text();
        throw new Error(`${response.status} ${response.statusText}: ${text}`);
    }

    return response.json() as Promise<T>;
}

export const api = {
    systemHealth: () =>
        getJson<SystemHealthResponse>('/api/system/health'),

    activeSignalsSummary: () =>
        getJson<ActiveSignalsSummaryResponse>('/api/signals/active/summary'),

    activeSignals: () =>
        getJson<ActiveSignalsResponse>('/api/signals/active?includeIgnored=true&includeBlocked=true'),

    analyticsSummary: (hours = 12) =>
        getJson<AnalyticsSummaryResponse>(`/api/analytics/opportunities/summary?hours=${hours}`),

    topOpportunities: (hours = 12, limit = 10) =>
        getJson<TopOpportunityItem[]>(`/api/analytics/opportunities/top?hours=${hours}&limit=${limit}`),

    depthIssues: (hours = 12, limit = 10) =>
        getJson<DepthIssueItem[]>(`/api/analytics/opportunities/depth-issues?hours=${hours}&limit=${limit}`),
};