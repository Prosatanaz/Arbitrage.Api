import { Fragment, useEffect, useState } from 'react'
import ExecutionControls from './ExecutionControls'
import { getJson, postJson } from './lib/http'
import { formatTime, formatUsd, formatPct, formatDecimal, formatSignedUsd } from './lib/format'

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

type Tone = 'good' | 'info' | 'warn' | 'bad' | 'neutral'

function feesOf(t: CarryTradeItem) {
    return (t.entryFeesUsd || 0) + (t.exitFeesUsd || 0)
}

function statusTone(status: string): Tone {
    switch (status) {
        case 'Open': return 'info'
        case 'Closing': return 'warn'
        case 'Closed': return 'neutral'
        case 'Failed': return 'bad'
        default: return 'neutral'
    }
}

function eventTone(eventType: string): Tone {
    switch (eventType) {
        case 'EntryFilled':
        case 'ExitFilled': return 'good'
        case 'EntrySignal':
        case 'ExitSignal': return 'info'
        case 'CloseRequested': return 'warn'
        case 'EntryFailed':
        case 'ExitFailed':
        case 'ManualInterventionRequired': return 'bad'
        default: return 'neutral'
    }
}

function legStatusTone(status: string): Tone {
    const s = status.toLowerCase()
    if (s === 'filled') return 'good'
    if (s === 'partiallyfilled') return 'warn'
    if (s === 'unfilled') return 'bad'
    return 'neutral'
}

function KpiCard(props: { title: string; value: string; subtitle?: string; tone?: Tone }) {
    const { title, value, subtitle, tone = 'neutral' } = props
    return (
        <article className={`kpi kpi--${tone === 'neutral' ? 'neutral' : tone}`}>
            <div className="kpi__topline">{title}</div>
            <div className="kpi__value">{value}</div>
            {subtitle && <div className="kpi__subtitle">{subtitle}</div>}
        </article>
    )
}

function OpenPositionCard({ trade, onClose, closing }: { trade: CarryTradeItem; onClose: () => void; closing: boolean }) {
    return (
        <section className="highlight-card" style={{ marginBottom: '1rem' }}>
            <div className="highlight-card__label">OPEN POSITION</div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: '1rem', flexWrap: 'wrap' }}>
                <div>
                    <div className="highlight-card__title">{trade.tradingPair}</div>
                    <div className="highlight-card__direction">
                        Long <span className="connector">{trade.longConnector}</span> · Short{' '}
                        <span className="connector">{trade.shortConnector}</span> · opened {formatTime(trade.openedAt)}
                    </div>
                </div>
                {trade.status === 'Open' && (
                    <button className="button--danger" onClick={onClose} disabled={closing}>
                        {closing ? 'Closing…' : 'Force-close now'}
                    </button>
                )}
            </div>
            <div className="highlight-card__metrics">
                <span>Qty {formatDecimal(trade.baseQuantity)}</span>
                <span>Notional {formatUsd(trade.notionalUsd)}</span>
                <span>Entry edge {formatPct(trade.entryNetEdgePct, 3)}</span>
                <span>Entry fees {formatUsd(trade.entryFeesUsd, 4)}</span>
                <span>{trade.status}</span>
            </div>
        </section>
    )
}

function LegCard({ leg }: { leg: Leg }) {
    return (
        <div className="leg-card">
            <span className="leg-card__phase">{leg.phase}</span>
            <div className="leg-card__main">
                <span className="connector">{leg.connector}</span>
                <span>{leg.role} · {leg.side}{leg.reduceOnly ? ' (reduce)' : ''}</span>
                <span className={`badge badge--${legStatusTone(leg.status)}`}>{leg.status}</span>
            </div>
            <div className="leg-card__nums">
                <span>filled <b>{formatDecimal(leg.filledQuantity)}/{formatDecimal(leg.requestedQuantity)}</b></span>
                <span>@ <b>{formatDecimal(leg.averageFillPrice)}</b></span>
                <span>fee <b>{formatUsd(leg.feePaidUsd, 4)}</b></span>
            </div>
            <div className="leg-card__sub">order {leg.exchangeOrderId || '—'} · {formatTime(leg.filledAt)}</div>
        </div>
    )
}

function TimelineEvent({ ev }: { ev: TradeEvent }) {
    const tone = eventTone(ev.eventType)
    let pretty: string | null = null
    if (ev.detailsJson) {
        try {
            pretty = JSON.stringify(JSON.parse(ev.detailsJson), null, 2)
        } catch {
            pretty = ev.detailsJson
        }
    }
    return (
        <li className="timeline__item">
            <div className="timeline__rail">
                <span className={`timeline__dot tone-${tone}`} />
                <span className="timeline__line" />
            </div>
            <div className="timeline__body">
                <div className="timeline__head">
                    <span className={`badge badge--${tone}`}>{ev.eventType}</span>
                    <span className="timeline__time">{formatTime(ev.createdAt)}</span>
                </div>
                <div className="timeline__reason">{ev.reason}</div>
                {pretty && (
                    <details className="timeline__json">
                        <summary>details</summary>
                        <pre>{pretty}</pre>
                    </details>
                )}
            </div>
        </li>
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
            <td colSpan={columns} className="trade-detail-cell">
                {error && <div className="connector-card__error" style={{ margin: '1rem' }}>{error}</div>}
                {!detail && !error && <div className="trade-detail__loading">Loading detail…</div>}

                {detail && (
                    <div className="trade-detail">
                        <div>
                            <div className="trade-detail__title">
                                Legs — per-exchange orders <span className="count">{detail.legs.length}</span>
                            </div>
                            <div className="leg-list">
                                {detail.legs.map((l) => <LegCard key={l.id} leg={l} />)}
                                {detail.legs.length === 0 && <div className="empty-block">No legs recorded.</div>}
                            </div>
                        </div>

                        <div>
                            <div className="trade-detail__title">
                                Decision &amp; action log <span className="count">{detail.events.length}</span>
                            </div>
                            <ol className="timeline">
                                {detail.events.map((e) => <TimelineEvent key={e.id} ev={e} />)}
                            </ol>
                            {detail.events.length === 0 && <div className="empty-block">No events recorded.</div>}
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

            <section className="account-kpis">
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
                    tone="neutral"
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

            {openTrade && (
                <OpenPositionCard
                    trade={openTrade}
                    closing={closingId === openTrade.id}
                    onClose={() => closePosition(openTrade.id)}
                />
            )}

            <section className="panel">
                <div className="panel-header">
                    <div>
                        <h2>Trade history</h2>
                        <p>Every real position with realized PnL. Click a row to expand per-exchange legs and the full decision log.</p>
                    </div>
                    <span>{carryTrades.length} rows</span>
                </div>

                <div className="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th style={{ width: '1.5rem' }}></th>
                                <th>Pair</th>
                                <th>Direction</th>
                                <th>Status</th>
                                <th>Entry edge</th>
                                <th>Realized PnL</th>
                                <th>Fees</th>
                                <th>Opened</th>
                                <th>Closed / reason</th>
                                <th></th>
                            </tr>
                        </thead>
                        <tbody>
                            {carryTrades.map((item) => {
                                const expanded = expandedId === item.id
                                return (
                                    <Fragment key={item.id}>
                                        <tr
                                            className={`trade-row ${expanded ? 'trade-row--expanded' : ''}`}
                                            onClick={() => setExpandedId(expanded ? null : item.id)}
                                        >
                                            <td><span className="trade-caret">▶</span></td>
                                            <td className="strong">{item.tradingPair}</td>
                                            <td>
                                                <span className="connector">{item.longConnector}</span>
                                                <span className="arrow">→</span>
                                                <span className="connector">{item.shortConnector}</span>
                                            </td>
                                            <td><span className={`badge badge--${statusTone(item.status)}`}>{item.status}</span></td>
                                            <td className="numeric">{formatPct(item.entryNetEdgePct, 3)}</td>
                                            <td className={`numeric ${(item.realizedPnlUsd ?? 0) >= 0 ? 'positive' : 'negative'}`}>
                                                {item.realizedPnlUsd === null ? '—' : formatSignedUsd(item.realizedPnlUsd)}
                                            </td>
                                            <td className="numeric">{formatUsd(feesOf(item), 4)}</td>
                                            <td>{formatTime(item.openedAt)}</td>
                                            <td className="reason">
                                                {item.closedAt ? `${formatTime(item.closedAt)} · ${item.closeReason ?? '—'}` : item.error ?? '—'}
                                            </td>
                                            <td>
                                                {item.status === 'Open' && (
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
                                        {expanded && <DetailRow tradeId={item.id} columns={10} />}
                                    </Fragment>
                                )
                            })}

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
