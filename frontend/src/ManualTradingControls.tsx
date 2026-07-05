import { useEffect, useState } from 'react'
import { getJson, postJson } from './lib/http'
import { formatPct } from './lib/format'

type ExecutionState = {
    mode: string
    runtimeStatus: string
    killSwitchEnabled: boolean
    manualTradingEnabled: boolean
    maxNotionalUsd: number | null
    lastStatusReason: string | null
}

type CurrentOpportunity = {
    available: boolean
    tradingPair?: string
    longConnector?: string
    shortConnector?: string
    netEdgePct?: number | null
    referencePrice?: number | null
}

type OpenResult = {
    success: boolean
    reason: string
}

function ManualTradingControls() {
    const [state, setState] = useState<ExecutionState | null>(null)
    const [opportunity, setOpportunity] = useState<CurrentOpportunity | null>(null)
    const [busy, setBusy] = useState(false)
    const [message, setMessage] = useState<string | null>(null)
    const [error, setError] = useState<string | null>(null)

    async function loadState() {
        try {
            setState(await getJson<ExecutionState>('/api/execution/state'))
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Failed to load execution state.')
        }
    }

    async function loadOpportunity() {
        try {
            setOpportunity(await getJson<CurrentOpportunity>('/api/execution/opportunity/current'))
        } catch {
            // Non-fatal: the opportunity panel simply shows nothing.
        }
    }

    useEffect(() => {
        loadState()
        loadOpportunity()

        const timer = window.setInterval(() => {
            loadState()
            loadOpportunity()
        }, 5000)

        return () => window.clearInterval(timer)
    }, [])

    async function toggleManualTrading() {
        if (!state) return

        const enabling = !state.manualTradingEnabled

        if (enabling && !window.confirm(
            'Enable manual trading? While on, the "Open position now" button can place a REAL two-leg position with real money (still requires arming and the kill switch off).',
        )) {
            return
        }

        setBusy(true)
        setError(null)
        setMessage(null)

        try {
            const path = enabling ? 'manual/enable' : 'manual/disable'
            const result = await postJson<{ reason: string }>(`/api/execution/${path}`)
            setMessage(result.reason)
            await loadState()
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Manual trading toggle failed.')
        } finally {
            setBusy(false)
        }
    }

    async function openPosition() {
        if (!opportunity?.available) {
            setError('No qualifying validated opportunity is available right now.')
            return
        }

        const notional = state?.maxNotionalUsd ?? 0

        if (!window.confirm(
            `Open a REAL position on ${opportunity.tradingPair} `
            + `(long ${opportunity.longConnector} / short ${opportunity.shortConnector}) `
            + `for ${notional} USDT of real money now?`,
        )) {
            return
        }

        setBusy(true)
        setError(null)
        setMessage(null)

        try {
            const result = await postJson<OpenResult>('/api/execution/trades/open')
            setMessage(result.reason)
            await loadState()
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Open failed.')
        } finally {
            setBusy(false)
        }
    }

    const manualOn = state?.manualTradingEnabled ?? false
    const killed = state?.killSwitchEnabled ?? true
    const armed = (state?.maxNotionalUsd ?? 0) > 0
    const canOpen = manualOn && !killed && armed && (opportunity?.available ?? false) && !busy

    return (
        <section className="panel execution-controls">
            <div className="panel-header">
                <div>
                    <h2>Manual trading</h2>
                    <p>
                        Opens the current best validated opportunity on demand. Still gated by arming and the
                        kill switch - the manual toggle is an additional switch on top of them.
                    </p>
                </div>
                <span className={`badge badge--${manualOn ? 'alert' : 'neutral'}`}>
                    Manual: {manualOn ? 'ON' : 'OFF'}
                </span>
            </div>

            {error && <div className="connector-card__error">{error}</div>}

            <div className="execution-controls__status">
                <div className="execution-controls__stat">
                    <span>Kill switch</span>
                    <span className={`badge badge--${killed ? 'candidate' : 'alert'}`}>
                        {killed ? 'ON (blocking)' : 'OFF (live)'}
                    </span>
                </div>
                <div className="execution-controls__stat">
                    <span>Armed notional</span>
                    <strong>{state?.maxNotionalUsd ?? '—'} USDT</strong>
                </div>
                <div className="execution-controls__stat execution-controls__stat--wide">
                    <span>Current opportunity</span>
                    <strong className="reason">
                        {opportunity?.available
                            ? `${opportunity.tradingPair} · ${opportunity.longConnector} → ${opportunity.shortConnector} · net edge ${formatPct(opportunity.netEdgePct, 3)}`
                            : 'None qualifying right now'}
                    </strong>
                </div>
            </div>

            <div className="connector-card__actions">
                <button
                    onClick={toggleManualTrading}
                    disabled={busy}
                    className={manualOn ? undefined : 'button--danger'}
                >
                    {manualOn ? 'Disable manual trading' : 'Enable manual trading'}
                </button>
                <button onClick={openPosition} disabled={!canOpen} className="button--danger">
                    Open position now
                </button>
            </div>

            {message && <div className="connector-card__message">{message}</div>}

            {!armed && manualOn && (
                <div className="connector-card__error">
                    Manual trading is on but the gate is not armed. Arm a notional in Execution controls above first.
                </div>
            )}
        </section>
    )
}

export default ManualTradingControls
