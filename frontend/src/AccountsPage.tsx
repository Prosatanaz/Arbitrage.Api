import { useEffect, useState } from 'react'
import { getJson } from './lib/http'
import {
    formatConnectorLabel,
    formatDecimal,
    formatSignedUsd,
    formatTime,
    formatUsd,
} from './lib/format'

type Balance = {
    connectorName: string
    asset: string
    walletBalance: number
    availableBalance: number
    equity: number
    usdValue: number
    receivedAt: string
}

type Position = {
    connectorName: string
    tradingPair: string
    size: number
    entryPrice: number
    markPrice: number
    unrealizedPnl: number
    side: string
    receivedAt: string
}

type OpenOrder = {
    connectorName: string
    exchangeOrderId: string
    tradingPair: string
    side: string
    orderType: string
    price: number
    quantity: number
    filledQuantity: number
    reduceOnly: boolean
    status: string
    createdAt: string
}

type AccountView = {
    connectorName: string
    ok: boolean
    error: string | null
    balances: Balance[]
    positions: Position[]
    openOrders: OpenOrder[]
    fetchedAt: string
}

function usdtOf(view: AccountView): Balance | null {
    return view.balances.find((b) => b.asset.toUpperCase() === 'USDT') ?? null
}

function equityOf(view: AccountView): number {
    const usdt = usdtOf(view)
    if (usdt) return usdt.equity || usdt.walletBalance || usdt.usdValue
    return view.balances.reduce((sum, b) => sum + (b.usdValue || 0), 0)
}

function availableOf(view: AccountView): number {
    const usdt = usdtOf(view)
    return usdt ? usdt.availableBalance : 0
}

function deployedOf(view: AccountView): number {
    return view.positions.reduce((sum, p) => sum + Math.abs(p.size * p.markPrice), 0)
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

function AccountCard({ view, totalEquity }: { view: AccountView; totalEquity: number }) {
    const equity = equityOf(view)
    const available = availableOf(view)
    const deployed = deployedOf(view)
    const share = totalEquity > 0 ? (equity / totalEquity) * 100 : 0

    return (
        <article className={`panel connector-card ${view.ok ? 'connector-card--configured' : ''}`}>
            <div className="account-card__top">
                <div className="connector-card__identity">
                    <span className="connector-card__avatar">
                        {view.connectorName.slice(0, 2).toUpperCase()}
                    </span>
                    <div>
                        <div className="account-name-row">
                            <span className={`health-dot ${view.ok ? 'health--ok' : 'health--bad'}`} />
                            <span className="connector-card__name">
                                {formatConnectorLabel(view.connectorName)}
                            </span>
                        </div>
                        <div className="connector-card__meta">
                            Updated {formatTime(view.fetchedAt)}
                        </div>
                    </div>
                </div>

                {view.ok && (
                    <div className="account-card__equity">
                        <div className="account-card__equity-value">{formatUsd(equity)}</div>
                        <div className="account-card__equity-label">Equity</div>
                    </div>
                )}
            </div>

            {view.error && <div className="connector-card__error">{view.error}</div>}

            {view.ok && (
                <>
                    <div className="account-card__metrics">
                        <span>
                            Available <b>{formatUsd(available)}</b>
                        </span>
                        <span>
                            Deployed <b>{formatUsd(deployed)}</b>
                        </span>
                        <span>
                            Share <b>{share.toFixed(1)}%</b>
                        </span>
                    </div>
                    <div className="account-alloc">
                        <div
                            className="account-alloc__fill"
                            style={{ width: `${Math.min(share, 100)}%` }}
                        />
                    </div>

                    {/* Positions */}
                    {view.positions.length > 0 ? (
                        <>
                            <div className="account-section-label">
                                <span>Open positions</span>
                                <span>{view.positions.length}</span>
                            </div>
                            <div className="table-wrap">
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Pair</th>
                                            <th>Side</th>
                                            <th>Size</th>
                                            <th>Entry</th>
                                            <th>Mark</th>
                                            <th>uPnL</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {view.positions.map((p, i) => (
                                            <tr key={`${p.tradingPair}-${i}`}>
                                                <td className="strong">{p.tradingPair}</td>
                                                <td>{p.side}</td>
                                                <td className="numeric">{formatDecimal(p.size)}</td>
                                                <td className="numeric">{formatDecimal(p.entryPrice)}</td>
                                                <td className="numeric">{formatDecimal(p.markPrice)}</td>
                                                <td className={`numeric ${p.unrealizedPnl >= 0 ? 'positive' : 'negative'}`}>
                                                    {formatSignedUsd(p.unrealizedPnl)}
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </>
                    ) : null}

                    {/* Orders */}
                    {view.openOrders.length > 0 ? (
                        <>
                            <div className="account-section-label">
                                <span>Active orders</span>
                                <span>{view.openOrders.length}</span>
                            </div>
                            <div className="table-wrap">
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Pair</th>
                                            <th>Side</th>
                                            <th>Type</th>
                                            <th>Price</th>
                                            <th>Qty (filled)</th>
                                            <th>Status</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {view.openOrders.map((o) => (
                                            <tr key={o.exchangeOrderId}>
                                                <td className="strong">{o.tradingPair}</td>
                                                <td>
                                                    {o.side}
                                                    {o.reduceOnly && <span className="badge badge--reduce">reduce</span>}
                                                </td>
                                                <td>{o.orderType}</td>
                                                <td className="numeric">{formatDecimal(o.price)}</td>
                                                <td className="numeric">
                                                    {formatDecimal(o.quantity)} ({formatDecimal(o.filledQuantity)})
                                                </td>
                                                <td>{o.status}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </>
                    ) : null}

                    {view.positions.length === 0 && view.openOrders.length === 0 && (
                        <div className="account-empty-line">
                            <span>No open positions</span>
                            <span>No active orders</span>
                        </div>
                    )}
                </>
            )}
        </article>
    )
}

function AccountsPage() {
    const [views, setViews] = useState<AccountView[]>([])
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)
    const [lastRefreshAt, setLastRefreshAt] = useState<Date | null>(null)

    async function refresh() {
        try {
            setError(null)
            const data = await getJson<AccountView[]>('/api/execution/accounts')
            setViews(data)
            setLastRefreshAt(new Date())
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Unknown error')
        } finally {
            setLoading(false)
        }
    }

    useEffect(() => {
        refresh()

        const timer = window.setInterval(refresh, 10000)

        return () => window.clearInterval(timer)
    }, [])

    // Summary across all exchanges.
    const okViews = views.filter((v) => v.ok)
    const totalEquity = okViews.reduce((sum, v) => sum + equityOf(v), 0)
    const totalAvailable = okViews.reduce((sum, v) => sum + availableOf(v), 0)
    const totalDeployed = okViews.reduce((sum, v) => sum + deployedOf(v), 0)
    const positionCount = views.reduce((sum, v) => sum + v.positions.length, 0)
    const orderCount = views.reduce((sum, v) => sum + v.openOrders.length, 0)
    const errorCount = views.length - okViews.length

    // Biggest accounts first; errored connectors sink to the bottom.
    const sortedViews = [...views].sort((a, b) => {
        if (a.ok !== b.ok) return a.ok ? -1 : 1
        return equityOf(b) - equityOf(a)
    })

    return (
        <main className="page">
            <header className="header">
                <div>
                    <div className="eyebrow">EXCHANGE ACCOUNTS</div>
                    <h1>Balances &amp; active orders</h1>
                    <p>
                        Live per-exchange balances, open positions and active orders. Read-only — no
                        orders are placed here. Only enabled connectors are queried.
                    </p>
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

            {views.length > 0 && (
                <section className="account-kpis">
                    <KpiCard
                        title="Total equity"
                        value={formatUsd(totalEquity)}
                        subtitle={`across ${okViews.length} exchange${okViews.length === 1 ? '' : 's'}`}
                        tone="info"
                    />
                    <KpiCard
                        title="Available"
                        value={formatUsd(totalAvailable)}
                        subtitle="free to trade"
                        tone="good"
                    />
                    <KpiCard
                        title="Deployed"
                        value={formatUsd(totalDeployed)}
                        subtitle={`in ${positionCount} position${positionCount === 1 ? '' : 's'}`}
                        tone="neutral"
                    />
                    <KpiCard
                        title="Active orders"
                        value={String(orderCount)}
                        subtitle="resting on exchanges"
                        tone="neutral"
                    />
                    <KpiCard
                        title="Online"
                        value={`${okViews.length} / ${views.length}`}
                        subtitle={errorCount > 0 ? `${errorCount} with errors` : 'all healthy'}
                        tone={errorCount > 0 ? 'bad' : 'good'}
                    />
                </section>
            )}

            <section className="connectors-grid">
                {sortedViews.map((view) => (
                    <AccountCard key={view.connectorName} view={view} totalEquity={totalEquity} />
                ))}

                {!loading && views.length === 0 && (
                    <div className="empty-block">
                        No enabled connectors. Configure and enable API keys on the Connectors tab.
                    </div>
                )}
            </section>
        </main>
    )
}

export default AccountsPage
