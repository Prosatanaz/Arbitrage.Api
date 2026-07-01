import { useEffect, useMemo, useState } from 'react'
import './styles.css'

type SystemHealth = {
    status: string
    now?: string
    bbo?: {
        totalSnapshots: number
        connectorCount?: number
        freshCount: number
    }
    depth?: {
        totalTargets: number
        totalCachedSnapshots: number
        targetsRecentlyChanged?: boolean
    }
    opportunities?: {
        hasData: boolean
        count: number
        validCount: number
        positiveValidCount: number
    }
    degradedConnectors?: string[]
}

type SignalsSummary = {
    updatedAt: string | null
    hasData: boolean
    total: number
    valid: number
    alerts: number
    candidates: number
    ignored: number
    blocked: number
    invalid: number
    bestAlert: BestSignalSummary | null
    bestCandidate: BestSignalSummary | null
}

type BestSignalSummary = {
    tradingPair: string
    direction: string
    netEdgePct: number | null
    estimatedProfitUsd: number | null
    decisionReason: string
}

type ActiveSignalsResponse = {
    updatedAt: string | null
    hasData: boolean
    count: number
    items: ActiveSignalItem[]
}

type ActiveSignalItem = {
    tradingPair: string
    direction: string
    longConnector: string
    shortConnector: string
    decision: string
    decisionReason: string
    validationStatus?: string
    validationReason?: string | null
    notionalUsd: number
    estimatedProfitUsd: number | null
    grossSpreadPct: number | null
    estimatedFeesPct: number | null
    netEdgePct: number | null
    buyAveragePrice: number | null
    sellAveragePrice: number | null
    buySlippagePct: number | null
    sellSlippagePct: number | null
    buyLevelsUsed: number | null
    sellLevelsUsed: number | null
    detectedAt: string
    validatedAt: string | null
    signalAgeMs: number
}

type AnalyticsSummary = {
    tradesCount: number
    totalEntryProfitUsd: number
    totalAvgProfitUsd: number
    totalConservativeProfitUsd: number
    totalBestCaseProfitUsd: number
    avgEntryEdgePct: number
    maxEntryEdgePct: number
    minEntryEdgePct: number
    firstEntryAt: string | null
    lastExitOrSeenAt: string | null
}

type TopOpportunity = {
    tradingPair: string
    longConnector: string
    shortConnector: string
    trades: number
    entryProfitUsd: number
    conservativeProfitUsd: number
    avgEntryEdgePct: number
    maxEntryEdgePct: number
    minEntryEdgePct: number
    firstEntryAt: string
    lastSeenAt: string
}

type DepthIssue = {
    problematicConnector: string
    status: string
    rows: number
}

type StatusDistribution = {
    status: string
    rows: number
    pct: number
}

type DashboardState = {
    health: SystemHealth | null
    signalsSummary: SignalsSummary | null
    activeSignals: ActiveSignalsResponse | null
    analyticsSummary: AnalyticsSummary | null
    topOpportunities: TopOpportunity[]
    depthIssues: DepthIssue[]
    statuses: StatusDistribution[]
}

function formatUsd(value: number | null | undefined, digits = 2) {
    if (value === null || value === undefined || Number.isNaN(value)) return '—'
    return `${value.toFixed(digits)} USDT`
}

function formatPct(value: number | null | undefined, digits = 3) {
    if (value === null || value === undefined || Number.isNaN(value)) return '—'
    return `${value.toFixed(digits)}%`
}

function formatNumber(value: number | null | undefined) {
    if (value === null || value === undefined || Number.isNaN(value)) return '—'
    return value.toLocaleString('en-US')
}

function formatAge(ms: number | null | undefined) {
    if (ms === null || ms === undefined || Number.isNaN(ms)) return '—'
    if (ms < 1000) return `${Math.round(ms)} ms`
    if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`
    return `${(ms / 60_000).toFixed(1)} min`
}

function formatTime(value: string | null | undefined) {
    if (!value) return '—'

    const date = new Date(value)

    if (Number.isNaN(date.getTime())) return '—'

    return date.toLocaleTimeString()
}

async function getJson<T>(url: string): Promise<T> {
    const response = await fetch(url)

    if (!response.ok) {
        const text = await response.text()
        throw new Error(`${response.status} ${response.statusText}: ${text}`)
    }

    return response.json() as Promise<T>
}

function getHealthTone(status: string | undefined) {
    if (!status) return 'neutral'
    if (status.toLowerCase() === 'healthy') return 'good'
    if (status.toLowerCase() === 'degraded') return 'warn'
    return 'bad'
}

function getDecisionTone(decision: string | undefined) {
    const normalized = decision?.toLowerCase()

    if (normalized === 'alert') return 'alert'
    if (normalized === 'candidate') return 'candidate'
    if (normalized === 'blocked') return 'blocked'
    if (normalized === 'ignored') return 'ignored'

    return 'neutral'
}

function KpiCard(props: {
    title: string
    value: string | number
    subtitle?: string
    tone?: 'neutral' | 'good' | 'warn' | 'bad' | 'info'
}) {
    const { title, value, subtitle, tone = 'neutral' } = props

    return (
        <article className={`kpi kpi--${tone}`}>
            <div className="kpi__topline">
                <span>{title}</span>
            </div>
            <div className="kpi__value">{value}</div>
            {subtitle && <div className="kpi__subtitle">{subtitle}</div>}
        </article>
    )
}

function EmptyRow({ columns, text }: { columns: number; text: string }) {
    return (
        <tr>
            <td colSpan={columns} className="empty-cell">
                {text}
            </td>
        </tr>
    )
}

function App() {
    const [state, setState] = useState<DashboardState>({
        health: null,
        signalsSummary: null,
        activeSignals: null,
        analyticsSummary: null,
        topOpportunities: [],
        depthIssues: [],
        statuses: [],
    })

    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)
    const [lastRefreshAt, setLastRefreshAt] = useState<Date | null>(null)

    async function refresh() {
        try {
            setError(null)

            const [
                health,
                signalsSummary,
                activeSignals,
                analyticsSummary,
                topOpportunities,
                depthIssues,
                statuses,
            ] = await Promise.all([
                getJson<SystemHealth>('/api/system/health'),
                getJson<SignalsSummary>('/api/signals/active/summary'),
                getJson<ActiveSignalsResponse>(
                    '/api/signals/active?limit=50',
                ),
                getJson<AnalyticsSummary>('/api/analytics/opportunities/summary?hours=12'),
                getJson<TopOpportunity[]>('/api/analytics/opportunities/top?hours=12&limit=10'),
                getJson<DepthIssue[]>('/api/analytics/opportunities/depth-issues?hours=12&limit=10'),
                getJson<StatusDistribution[]>('/api/analytics/opportunities/statuses?hours=12'),
            ])

            setState({
                health,
                signalsSummary,
                activeSignals,
                analyticsSummary,
                topOpportunities,
                depthIssues,
                statuses,
            })

            setLastRefreshAt(new Date())
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Unknown error')
        } finally {
            setLoading(false)
        }
    }

    useEffect(() => {
        refresh()

        const timer = window.setInterval(refresh, 5000)

        return () => window.clearInterval(timer)
    }, [])

    const healthTone = useMemo(
        () => getHealthTone(state.health?.status),
        [state.health?.status],
    )

    const activeAlert = state.signalsSummary?.bestAlert
    const activeCandidate = state.signalsSummary?.bestCandidate

    return (
        <main className="page">
            <header className="header">
                <div>
                    <div className="eyebrow">CEX-CEX PERP SCANNER</div>
                    <h1>Arbitrage Dashboard</h1>
                    <p>Live signal quality, market data health and 12h opportunity analytics.</p>
                </div>

                <div className="header-actions">
                    <button onClick={refresh} disabled={loading}>
                        {loading ? 'Loading...' : 'Refresh'}
                    </button>
                    <span>{lastRefreshAt ? `Updated ${lastRefreshAt.toLocaleTimeString()}` : 'Not loaded'}</span>
                </div>
            </header>

            {error && (
                <section className="error">
                    <strong>Request failed:</strong>
                    <span>{error}</span>
                </section>
            )}

            <section className="status-strip">
                <div className={`status-pill status-pill--${healthTone}`}>
                    <span className="status-dot" />
                    <span>{state.health?.status ?? 'Unknown'}</span>
                </div>

                <div className="status-meta">
                    <span>BBO {formatNumber(state.health?.bbo?.totalSnapshots)}</span>
                    <span>Depth {formatNumber(state.health?.depth?.totalCachedSnapshots)}</span>
                    <span>Valid {formatNumber(state.signalsSummary?.valid)}</span>
                    <span>Alerts {formatNumber(state.signalsSummary?.alerts)}</span>
                </div>
            </section>

            <section className="kpi-grid">
                <KpiCard
                    title="System status"
                    value={state.health?.status ?? '—'}
                    subtitle={`${state.health?.degradedConnectors?.length ?? 0} degraded connectors`}
                    tone={healthTone === 'good' ? 'good' : healthTone === 'warn' ? 'warn' : 'bad'}
                />

                <KpiCard
                    title="Active alerts"
                    value={state.signalsSummary?.alerts ?? '—'}
                    subtitle={`${state.signalsSummary?.candidates ?? 0} candidates · ${state.signalsSummary?.blocked ?? 0} blocked`}
                    tone={(state.signalsSummary?.alerts ?? 0) > 0 ? 'warn' : 'neutral'}
                />

                <KpiCard
                    title="12h conservative PnL"
                    value={formatUsd(state.analyticsSummary?.totalConservativeProfitUsd)}
                    subtitle={`${state.analyticsSummary?.tradesCount ?? 0} modelled trades`}
                    tone="good"
                />

                <KpiCard
                    title="12h avg entry edge"
                    value={formatPct(state.analyticsSummary?.avgEntryEdgePct)}
                    subtitle={`max ${formatPct(state.analyticsSummary?.maxEntryEdgePct)}`}
                    tone="info"
                />

                <KpiCard
                    title="BBO freshness"
                    value={formatNumber(state.health?.bbo?.freshCount)}
                    subtitle={`${formatNumber(state.health?.bbo?.totalSnapshots)} total snapshots`}
                    tone="neutral"
                />

                <KpiCard
                    title="Depth cache"
                    value={formatNumber(state.health?.depth?.totalCachedSnapshots)}
                    subtitle={`${formatNumber(state.health?.depth?.totalTargets)} active targets`}
                    tone="neutral"
                />
            </section>

            <section className="highlight-grid">
                <article className="highlight-card">
                    <div className="highlight-card__label">Best alert</div>
                    {activeAlert ? (
                        <>
                            <div className="highlight-card__title">{activeAlert.tradingPair}</div>
                            <div className="highlight-card__direction">{activeAlert.direction}</div>
                            <div className="highlight-card__metrics">
                                <span>{formatPct(activeAlert.netEdgePct)}</span>
                                <span>{formatUsd(activeAlert.estimatedProfitUsd)}</span>
                            </div>
                        </>
                    ) : (
                        <div className="highlight-card__empty">No active alert right now.</div>
                    )}
                </article>

                <article className="highlight-card">
                    <div className="highlight-card__label">Best candidate</div>
                    {activeCandidate ? (
                        <>
                            <div className="highlight-card__title">{activeCandidate.tradingPair}</div>
                            <div className="highlight-card__direction">{activeCandidate.direction}</div>
                            <div className="highlight-card__metrics">
                                <span>{formatPct(activeCandidate.netEdgePct)}</span>
                                <span>{formatUsd(activeCandidate.estimatedProfitUsd)}</span>
                            </div>
                        </>
                    ) : (
                        <div className="highlight-card__empty">No active candidate right now.</div>
                    )}
                </article>
            </section>

            <section className="panel">
                <div className="panel-header">
                    <div>
                        <h2>Active signals</h2>
                        <p>Live quality-filtered opportunities from current validation snapshot.</p>
                    </div>
                    <span>{state.activeSignals?.count ?? 0} rows</span>
                </div>

                <div className="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>Pair</th>
                                <th>Direction</th>
                                <th>Decision</th>
                                <th>Net edge</th>
                                <th>Profit</th>
                                <th>Spread / Fees</th>
                                <th>Slippage</th>
                                <th>Age</th>
                                <th>Reason</th>
                            </tr>
                        </thead>
                        <tbody>
                            {(state.activeSignals?.items ?? []).map((item) => (
                                <tr key={`${item.tradingPair}-${item.longConnector}-${item.shortConnector}-${item.decision}`}>
                                    <td className="strong">{item.tradingPair}</td>
                                    <td>
                                        <span className="connector">{item.longConnector}</span>
                                        <span className="arrow">→</span>
                                        <span className="connector">{item.shortConnector}</span>
                                    </td>
                                    <td>
                                        <span className={`badge badge--${getDecisionTone(item.decision)}`}>
                                            {item.decision}
                                        </span>
                                    </td>
                                    <td className="numeric positive">{formatPct(item.netEdgePct)}</td>
                                    <td className="numeric">{formatUsd(item.estimatedProfitUsd)}</td>
                                    <td className="numeric">
                                        {formatPct(item.grossSpreadPct)} / {formatPct(item.estimatedFeesPct)}
                                    </td>
                                    <td className="numeric">
                                        {formatPct(item.buySlippagePct)} / {formatPct(item.sellSlippagePct)}
                                    </td>
                                    <td>{formatAge(item.signalAgeMs)}</td>
                                    <td className="reason">{item.decisionReason}</td>
                                </tr>
                            ))}

                            {(state.activeSignals?.items.length ?? 0) === 0 && (
                                <EmptyRow columns={9} text="No active signal candidates right now." />
                            )}
                        </tbody>
                    </table>
                </div>
            </section>

            <section className="content-grid">
                <section className="panel">
                    <div className="panel-header">
                        <div>
                            <h2>Top opportunities, 12h</h2>
                            <p>Grouped by pair and direction, sorted by conservative PnL.</p>
                        </div>
                    </div>

                    <div className="table-wrap">
                        <table>
                            <thead>
                                <tr>
                                    <th>Pair</th>
                                    <th>Direction</th>
                                    <th>Trades</th>
                                    <th>Conservative PnL</th>
                                    <th>Avg edge</th>
                                    <th>Max edge</th>
                                    <th>Last seen</th>
                                </tr>
                            </thead>
                            <tbody>
                                {state.topOpportunities.map((item) => (
                                    <tr key={`${item.tradingPair}-${item.longConnector}-${item.shortConnector}`}>
                                        <td className="strong">{item.tradingPair}</td>
                                        <td>
                                            <span className="connector">{item.longConnector}</span>
                                            <span className="arrow">→</span>
                                            <span className="connector">{item.shortConnector}</span>
                                        </td>
                                        <td className="numeric">{item.trades}</td>
                                        <td className="numeric positive">{formatUsd(item.conservativeProfitUsd)}</td>
                                        <td className="numeric">{formatPct(item.avgEntryEdgePct)}</td>
                                        <td className="numeric">{formatPct(item.maxEntryEdgePct)}</td>
                                        <td>{formatTime(item.lastSeenAt)}</td>
                                    </tr>
                                ))}

                                {state.topOpportunities.length === 0 && (
                                    <EmptyRow columns={7} text="No historical opportunity episodes for selected window." />
                                )}
                            </tbody>
                        </table>
                    </div>
                </section>

                <aside className="side-stack">
                    <section className="panel">
                        <div className="panel-header">
                            <div>
                                <h2>Depth issues, 12h</h2>
                                <p>Missing/stale depth grouped by connector.</p>
                            </div>
                        </div>

                        <div className="table-wrap">
                            <table>
                                <thead>
                                    <tr>
                                        <th>Connector</th>
                                        <th>Status</th>
                                        <th>Rows</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {state.depthIssues.map((item) => (
                                        <tr key={`${item.problematicConnector}-${item.status}`}>
                                            <td className="connector">{item.problematicConnector}</td>
                                            <td>{item.status}</td>
                                            <td className="numeric">{formatNumber(item.rows)}</td>
                                        </tr>
                                    ))}

                                    {state.depthIssues.length === 0 && (
                                        <EmptyRow columns={3} text="No depth issues." />
                                    )}
                                </tbody>
                            </table>
                        </div>
                    </section>

                    <section className="panel">
                        <div className="panel-header">
                            <div>
                                <h2>Status distribution</h2>
                                <p>Validation results over the last 12h.</p>
                            </div>
                        </div>

                        <div className="status-list">
                            {state.statuses.map((item) => (
                                <div className="status-row" key={item.status}>
                                    <div>
                                        <div className="status-row__title">{item.status}</div>
                                        <div className="status-row__meta">{formatNumber(item.rows)} rows</div>
                                    </div>
                                    <div className="status-row__pct">{formatPct(item.pct, 2)}</div>
                                </div>
                            ))}

                            {state.statuses.length === 0 && (
                                <div className="empty-block">No status data.</div>
                            )}
                        </div>
                    </section>
                </aside>
            </section>
        </main>
    )
}

export default App