import { useEffect, useState } from 'react'
import ExecutionControls from './ExecutionControls'
import ManualTradingControls from './ManualTradingControls'
import { getJson } from './lib/http'
import { formatTime, formatUsd, formatPct, getDecisionTone } from './lib/format'

type CarryTradeItem = {
    id: string
    tradingPair: string
    longConnector: string
    shortConnector: string
    notionalUsd: number
    baseQuantity: number
    entryLongPrice: number
    entryShortPrice: number
    entryNetEdgePct: number
    openedAt: string
    status: string
    exitLongPrice: number | null
    exitShortPrice: number | null
    closeReason: string | null
    closedAt: string | null
    realizedPnlUsd: number | null
    error: string | null
}

type RealizedSummary = {
    tradesCount: number
    totalRealizedPnlUsd: number
    avgRealizedPnlUsd: number
    winRatePct: number
    avgHoldMinutes: number
    lastClosedAt: string | null
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

function TradingPage() {
    const [carryTrades, setCarryTrades] = useState<CarryTradeItem[]>([])
    const [realizedSummary, setRealizedSummary] = useState<RealizedSummary | null>(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)
    const [lastRefreshAt, setLastRefreshAt] = useState<Date | null>(null)

    async function refresh() {
        try {
            setError(null)

            const [trades, summary] = await Promise.all([
                getJson<CarryTradeItem[]>('/api/execution/trades?hours=168&limit=20'),
                getJson<RealizedSummary>('/api/analytics/opportunities/realized/summary?hours=168'),
            ])

            setCarryTrades(trades)
            setRealizedSummary(summary)
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

    return (
        <main className="page">
            <header className="header">
                <div>
                    <div className="eyebrow">REAL MONEY</div>
                    <h1>Trading</h1>
                    <p>Execution controls and actual carry-trade positions - real fills, any exchange pair, not modelled.</p>
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

            <ExecutionControls />

            <ManualTradingControls />

            <section className="kpi-grid">
                <KpiCard
                    title="Realized PnL"
                    value={formatUsd(realizedSummary?.totalRealizedPnlUsd)}
                    subtitle={`${realizedSummary?.tradesCount ?? 0} closed trades · ${formatPct(realizedSummary?.winRatePct, 0)} win rate`}
                    tone={(realizedSummary?.totalRealizedPnlUsd ?? 0) >= 0 ? 'good' : 'bad'}
                />

                <KpiCard
                    title="Avg realized PnL"
                    value={formatUsd(realizedSummary?.avgRealizedPnlUsd)}
                    subtitle="per closed trade"
                    tone="neutral"
                />

                <KpiCard
                    title="Avg hold time"
                    value={realizedSummary?.avgHoldMinutes ? `${realizedSummary.avgHoldMinutes.toFixed(0)} min` : '—'}
                    subtitle={`last closed ${formatTime(realizedSummary?.lastClosedAt)}`}
                    tone="info"
                />
            </section>

            <section className="panel">
                <div className="panel-header">
                    <div>
                        <h2>Real trades</h2>
                        <p>Actual carry-trade positions - real fills, any exchange pair, not modelled.</p>
                    </div>
                    <span>{carryTrades.length} rows</span>
                </div>

                <div className="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>Pair</th>
                                <th>Direction</th>
                                <th>Status</th>
                                <th>Qty</th>
                                <th>Entry</th>
                                <th>Exit</th>
                                <th>Realized PnL</th>
                                <th>Opened</th>
                                <th>Closed / reason</th>
                            </tr>
                        </thead>
                        <tbody>
                            {carryTrades.map((item) => (
                                <tr key={item.id}>
                                    <td className="strong">{item.tradingPair}</td>
                                    <td>
                                        <span className="connector">{item.longConnector}</span>
                                        <span className="arrow">→</span>
                                        <span className="connector">{item.shortConnector}</span>
                                    </td>
                                    <td>
                                        <span className={`badge badge--${getDecisionTone(item.status)}`}>
                                            {item.status}
                                        </span>
                                    </td>
                                    <td className="numeric">{item.baseQuantity}</td>
                                    <td className="numeric">
                                        {item.entryLongPrice} / {item.entryShortPrice}
                                    </td>
                                    <td className="numeric">
                                        {item.exitLongPrice ?? '—'} / {item.exitShortPrice ?? '—'}
                                    </td>
                                    <td className={`numeric ${(item.realizedPnlUsd ?? 0) >= 0 ? 'positive' : ''}`}>
                                        {formatUsd(item.realizedPnlUsd)}
                                    </td>
                                    <td>{formatTime(item.openedAt)}</td>
                                    <td className="reason">
                                        {item.closedAt ? `${formatTime(item.closedAt)} · ${item.closeReason ?? '—'}` : item.error ?? '—'}
                                    </td>
                                </tr>
                            ))}

                            {carryTrades.length === 0 && (
                                <EmptyRow columns={9} text="No real trades yet - waiting on the execution pipeline." />
                            )}
                        </tbody>
                    </table>
                </div>
            </section>
        </main>
    )
}

export default TradingPage
