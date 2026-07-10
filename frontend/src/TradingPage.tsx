import { Fragment, useEffect, useState } from 'react'
import ExecutionControls from './ExecutionControls'
import { getJson, postJson } from './lib/http'
import { formatTime, formatUsd, formatPct, formatDecimal, formatSignedUsd, getDecisionTone } from './lib/format'

type CarryTradeItem = {
    id: string
    tradingPair: string
    longConnector: string
    shortConnector: string
    notionalUsd: number
    baseQuantity: number
    entryLongPrice: number
    entryShortPrice: number
    entryFeesUsd: number
    entryNetEdgePct: number
    entryGrossSpreadPct: number | null
    entryEstimatedFeesPct: number | null
    entryReferencePrice: number | null
    openedAt: string
    status: string
    exitLongPrice: number | null
    exitShortPrice: number | null
    exitFeesUsd: number | null
    exitNetEdgePct: number | null
    closeReason: string | null
    closedAt: string | null
    realizedPnlUsd: number | null
    error: string | null
}

type Leg = {
    id: number
    phase: string
    connector: string
    role: string
    side: string
    reduceOnly: boolean
    requestedQuantity: number
    filledQuantity: number
    averageFillPrice: number
    feePaidUsd: number
    exchangeOrderId: string
    clientOrderId: string
    status: string
    filledAt: string
}

type TradeEvent = {
    id: number
    eventType: string
    reason: string
    detailsJson: string | null
    createdAt: string
}

type TradeDetail = {
    trade: CarryTradeItem
    legs: Leg[]
    events: TradeEvent[]
}

type RealizedSummary = {
    tradesCount: number
    totalRealizedPnlUsd: number
    avgRealizedPnlUsd: number
    winRatePct: number
    avgHoldMinutes: number
    lastClosedAt: string | null
}

function feesOf(t: CarryTradeItem) {
    return (t.entryFeesUsd || 0) + (t.exitFeesUsd || 0)
}

function KpiCard(props: {
    title: string
    value: string
    subtitle?: string
    tone?: 'neutral' | 'good' | 'warn' | 'bad' | 'info'
}) {
    const { title, value, subtitle, tone = 'neutral' } = props
    return (
        <article className={`kpi kpi--${tone}`}>
            <div className="kpi__topline">{title}</div>
            <div className="kpi__value">{value}</div>
            {subtitle && <div className="kpi__subtitle">{subtitle}</div>}
        </article>
    )
}

function OpenPositionCard({ trade }: { trade: CarryTradeItem }) {
    return (
        <section className="highlight-card" style={{ marginBottom: '1rem' }}>
            <div className="highlight-card__label">OPEN POSITION</div>
            <div className="highlight-card__title">{trade.tradingPair}</div>
            <div className="highlight-card__direction">
                Long <span className="connector">{trade.longConnector}</span> · Short{' '}
                <span className="connector">{trade.shortConnector}</span> · opened {formatTime(trade.openedAt)}
            </div>
            <div className="highlight-card__metrics">
                <span>Qty {formatDecimal(trade.baseQuantity)}</span>
                <span>Notional {formatUsd(trade.notionalUsd)}</span>
                <span>Entry edge {formatPct(trade.entryNetEdgePct, 3)}</span>
                <span>Entry fees {formatUsd(trade.entryFeesUsd)}</span>
                <span>{trade.status}</span>
            </div>
        </section>
    )
}

function DetailRow({ tradeId, columns }: { tradeId: string; columns: number }) {
    const [detail, setDetail] = useState<TradeDetail | null>(null)
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        let cancelled = false
        getJson<TradeDetail>(`/api/execution/trades/${tradeId}/detail`)
            .then((d) => !cancelled && setDetail(d))
            .catch((e) => !cancelled && setError(e instanceof Error ? e.message : 'Failed to load detail'))
        return () => {
            cancelled = true
        }
    }, [tradeId])

    return (
        <tr>
            <td colSpan={columns} style={{ background: 'rgba(8,17,31,0.5)' }}>
                {error && <div className="connector-card__error">{error}</div>}
                {!detail && !error && <div className="empty-block">Loading detail…</div>}

                {detail && (
                    <div style={{ display: 'grid', gap: '1rem' }}>
                        <div>
                            <div className="account-section-label">
                                <span>Legs (per-exchange orders)</span>
                                <span>{detail.legs.length}</span>
                            </div>
                            <div className="table-wrap">
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Phase</th>
                                            <th>Connector</th>
                                            <th>Role</th>
                                            <th>Side</th>
                                            <th>Requested</th>
                                            <th>Filled</th>
                                            <th>Avg price</th>
                                            <th>Fee</th>
                                            <th>Status</th>
                                            <th>Order id</th>
                                            <th>Filled at</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {detail.legs.map((l) => (
                                            <tr key={l.id}>
                                                <td>{l.phase}</td>
                                                <td className="connector">{l.connector}</td>
                                                <td>{l.role}</td>
                                                <td>{l.side}{l.reduceOnly ? ' (reduce)' : ''}</td>
                                                <td className="numeric">{formatDecimal(l.requestedQuantity)}</td>
                                                <td className="numeric">{formatDecimal(l.filledQuantity)}</td>
                                                <td className="numeric">{formatDecimal(l.averageFillPrice)}</td>
                                                <td className="numeric">{formatUsd(l.feePaidUsd, 4)}</td>
                                                <td>{l.status}</td>
                                                <td className="reason">{l.exchangeOrderId}</td>
                                                <td>{formatTime(l.filledAt)}</td>
                                            </tr>
                                        ))}
                                        {detail.legs.length === 0 && (
                                            <tr><td colSpan={11} className="empty-cell">No legs recorded.</td></tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        </div>

                        <div>
                            <div className="account-section-label">
                                <span>Decision / action log</span>
                                <span>{detail.events.length}</span>
                            </div>
                            <div className="table-wrap">
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Time</th>
                                            <th>Event</th>
                                            <th>Reason</th>
                                            <th>Details</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {detail.events.map((e) => (
                                            <tr key={e.id}>
                                                <td>{formatTime(e.createdAt)}</td>
                                                <td><span className="badge badge--neutral">{e.eventType}</span></td>
                                                <td className="reason" style={{ maxWidth: 420 }}>{e.reason}</td>
                                                <td>
                                                    {e.detailsJson && (
                                                        <details>
                                                            <summary style={{ cursor: 'pointer', color: '#93c5fd' }}>json</summary>
                                                            <pre style={{ margin: '0.4rem 0 0', fontSize: '0.75rem', whiteSpace: 'pre-wrap' }}>
                                                                {JSON.stringify(JSON.parse(e.detailsJson), null, 2)}
                                                            </pre>
                                                        </details>
                                                    )}
                                                </td>
                                            </tr>
                                        ))}
                                        {detail.events.length === 0 && (
                                            <tr><td colSpan={4} className="empty-cell">No events recorded.</td></tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                )}
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
    const [expandedId, setExpandedId] = useState<string | null>(null)
    const [closingId, setClosingId] = useState<string | null>(null)

    async function refresh() {
        try {
            setError(null)
            const [trades, summary] = await Promise.all([
                getJson<CarryTradeItem[]>('/api/execution/trades?hours=168&limit=50'),
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

    async function closePosition(id: string) {
        if (!window.confirm('Force-close this open position now? The monitor will close both legs on its next tick.')) return
        setClosingId(id)
        try {
            await postJson(`/api/execution/trades/${id}/close`)
            await refresh()
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Close request failed.')
        } finally {
            setClosingId(null)
        }
    }

    const openTrade = carryTrades.find((t) => t.status === 'Open' || t.status === 'Closing')
    const totalFees = carryTrades.reduce((sum, t) => sum + feesOf(t), 0)

    return (
        <main className="page">
            <header className="header">
                <div>
                    <div className="eyebrow">REAL MONEY · AUTOMATED</div>
                    <h1>Trading</h1>
                    <p>Fully automated carry-trade execution. Positions open on validated signals and close on take-profit or timeout — every decision and fill is logged to the database.</p>
                </div>
                <div className="header-actions">
                    <button onClick={refresh} disabled={loading}>{loading ? 'Loading...' : 'Refresh'}</button>
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

            <section className="kpi-grid">
                <KpiCard
                    title="Realized PnL"
                    value={formatUsd(realizedSummary?.totalRealizedPnlUsd)}
                    subtitle={`${realizedSummary?.tradesCount ?? 0} closed · ${formatPct(realizedSummary?.winRatePct, 0)} win rate`}
                    tone={(realizedSummary?.totalRealizedPnlUsd ?? 0) >= 0 ? 'good' : 'bad'}
                />
                <KpiCard
                    title="Avg realized PnL"
                    value={formatUsd(realizedSummary?.avgRealizedPnlUsd)}
                    subtitle="per closed trade"
                />
                <KpiCard
                    title="Total fees paid"
                    value={formatUsd(totalFees)}
                    subtitle="entry + exit, shown trades"
                    tone="warn"
                />
                <KpiCard
                    title="Avg hold time"
                    value={realizedSummary?.avgHoldMinutes ? `${realizedSummary.avgHoldMinutes.toFixed(0)} min` : '—'}
                    subtitle={`last closed ${formatTime(realizedSummary?.lastClosedAt)}`}
                    tone="info"
                />
                <KpiCard
                    title="Open position"
                    value={openTrade ? '1' : '0'}
                    subtitle={openTrade ? `${openTrade.tradingPair} · ${openTrade.status}` : 'flat'}
                    tone={openTrade ? 'info' : 'neutral'}
                />
            </section>

            {openTrade && <OpenPositionCard trade={openTrade} />}

            <section className="panel">
                <div className="panel-header">
                    <div>
                        <h2>Trade history</h2>
                        <p>Every real position with realized PnL. Click a row to see per-exchange legs and the full decision log.</p>
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
                                <th>Entry edge</th>
                                <th>Fees</th>
                                <th>Realized PnL</th>
                                <th>Opened</th>
                                <th>Closed / reason</th>
                                <th></th>
                            </tr>
                        </thead>
                        <tbody>
                            {carryTrades.map((item) => (
                                <Fragment key={item.id}>
                                    <tr
                                        onClick={() => setExpandedId(expandedId === item.id ? null : item.id)}
                                        style={{ cursor: 'pointer' }}
                                    >
                                        <td className="strong">{item.tradingPair}</td>
                                        <td>
                                            <span className="connector">{item.longConnector}</span>
                                            <span className="arrow">→</span>
                                            <span className="connector">{item.shortConnector}</span>
                                        </td>
                                        <td><span className={`badge badge--${getDecisionTone(item.status)}`}>{item.status}</span></td>
                                        <td className="numeric">{formatDecimal(item.baseQuantity)}</td>
                                        <td className="numeric">{formatPct(item.entryNetEdgePct, 3)}</td>
                                        <td className="numeric">{formatUsd(feesOf(item), 4)}</td>
                                        <td className={`numeric ${(item.realizedPnlUsd ?? 0) >= 0 ? 'positive' : 'negative'}`}>
                                            {item.realizedPnlUsd === null ? '—' : formatSignedUsd(item.realizedPnlUsd)}
                                        </td>
                                        <td>{formatTime(item.openedAt)}</td>
                                        <td className="reason">
                                            {item.closedAt ? `${formatTime(item.closedAt)} · ${item.closeReason ?? '—'}` : item.error ?? '—'}
                                        </td>
                                        <td>
                                            {(item.status === 'Open') && (
                                                <button
                                                    className="button--danger"
                                                    onClick={(ev) => { ev.stopPropagation(); closePosition(item.id) }}
                                                    disabled={closingId === item.id}
                                                >
                                                    {closingId === item.id ? '…' : 'Close'}
                                                </button>
                                            )}
                                        </td>
                                    </tr>
                                    {expandedId === item.id && <DetailRow tradeId={item.id} columns={10} />}
                                </Fragment>
                            ))}

                            {carryTrades.length === 0 && (
                                <tr>
                                    <td colSpan={10} className="empty-cell">No real trades yet — waiting on the execution pipeline.</td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>
            </section>
        </main>
    )
}

export default TradingPage
