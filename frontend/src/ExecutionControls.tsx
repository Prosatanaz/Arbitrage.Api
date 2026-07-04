import { useEffect, useState } from 'react'

type ExecutionConfig = {
    mode: string
    maxLiveNotionalUsd: number
    maxAttemptsPerArm: number
    armExpiresAfterSeconds: number
    maxLegDelayMs: number
    maxAllowedSlippagePct: number
    killSwitchEnabledOnStartup: boolean
    requireOneShotArm: boolean
    enabledConnectors: string[]
}

type ExecutionState = {
    mode: string
    runtimeStatus: string
    killSwitchEnabled: boolean
    remainingAttempts: number
    maxNotionalUsd: number | null
    armedUntil: string | null
    lastAttemptId: string | null
    lastStatusReason: string | null
    createdAt: string
    updatedAt: string
}

type GateResult = {
    isAllowed: boolean
    reason: string
}

async function getJson<T>(url: string): Promise<T> {
    const response = await fetch(url)

    if (!response.ok) {
        const text = await response.text()
        throw new Error(`${response.status} ${response.statusText}: ${text}`)
    }

    return response.json() as Promise<T>
}

async function postJson<T>(url: string, body?: unknown): Promise<T> {
    const response = await fetch(url, {
        method: 'POST',
        headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
    })

    const text = await response.text()
    const parsed = text ? (JSON.parse(text) as T) : ({} as T)

    if (!response.ok) {
        const reason = (parsed as { reason?: string; error?: string })?.reason
            ?? (parsed as { reason?: string; error?: string })?.error
            ?? `${response.status} ${response.statusText}`
        throw new Error(reason)
    }

    return parsed
}

function formatTime(value: string | null | undefined) {
    if (!value) return '—'

    const date = new Date(value)

    if (Number.isNaN(date.getTime())) return '—'

    return date.toLocaleString()
}

function runtimeStatusTone(status: string | undefined) {
    switch (status) {
        case 'Armed':
        case 'Executing':
            return 'alert'
        case 'LockedByError':
        case 'KillSwitch':
            return 'blocked'
        default:
            return 'neutral'
    }
}

function ExecutionControls() {
    const [config, setConfig] = useState<ExecutionConfig | null>(null)
    const [state, setState] = useState<ExecutionState | null>(null)
    const [notional, setNotional] = useState('')
    const [expiresSeconds, setExpiresSeconds] = useState('')
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

    useEffect(() => {
        getJson<ExecutionConfig>('/api/execution/config')
            .then((data) => {
                setConfig(data)
                setNotional((current) => current || String(data.maxLiveNotionalUsd))
            })
            .catch((err) => setError(err instanceof Error ? err.message : 'Failed to load execution config.'))

        loadState()

        const timer = window.setInterval(loadState, 5000)

        return () => window.clearInterval(timer)
    }, [])

    async function armOnce() {
        const notionalValue = Number(notional)

        if (!Number.isFinite(notionalValue) || notionalValue <= 0) {
            setError('Enter a positive notional before arming.')
            return
        }

        if (!window.confirm(
            `Arm live execution for up to ${notionalValue} USDT? The next qualifying opportunity may open a REAL position with real money.`,
        )) {
            return
        }

        setBusy(true)
        setError(null)
        setMessage(null)

        try {
            const body: { maxNotionalUsd: number; expiresAfterSeconds?: number } = {
                maxNotionalUsd: notionalValue,
            }

            if (expiresSeconds)
                body.expiresAfterSeconds = Number(expiresSeconds)

            const result = await postJson<GateResult>('/api/execution/arm-once', body)
            setMessage(result.reason)
            await loadState()
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Arm failed.')
        } finally {
            setBusy(false)
        }
    }

    async function disarm() {
        setBusy(true)
        setError(null)
        setMessage(null)

        try {
            const result = await postJson<GateResult>('/api/execution/disarm')
            setMessage(result.reason)
            await loadState()
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Disarm failed.')
        } finally {
            setBusy(false)
        }
    }

    async function toggleKillSwitch() {
        if (!state) return

        const enabling = !state.killSwitchEnabled

        if (!enabling && !window.confirm(
            'Disable the kill switch? This allows real orders to be placed again as soon as the gate is armed.',
        )) {
            return
        }

        setBusy(true)
        setError(null)
        setMessage(null)

        try {
            const path = enabling ? 'kill-switch/enable' : 'kill-switch/disable'
            const result = await postJson<GateResult>(`/api/execution/${path}`)
            setMessage(result.reason)
            await loadState()
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Kill switch toggle failed.')
        } finally {
            setBusy(false)
        }
    }

    const canArm = config?.mode === 'ArmedOneShot'

    return (
        <section className="panel execution-controls">
            <div className="panel-header">
                <div>
                    <h2>Execution controls</h2>
                    <p>
                        Arms/disarms real order placement across all enabled connectors. Server mode is fixed at{' '}
                        <strong>{config?.mode ?? '—'}</strong> (change <code>Execution:Mode</code> in appsettings to alter).
                    </p>
                </div>
                <span className={`badge badge--${state?.killSwitchEnabled ? 'candidate' : 'alert'}`}>
                    Kill switch: {state?.killSwitchEnabled ? 'ON (blocking)' : 'OFF (live)'}
                </span>
            </div>

            {error && <div className="connector-card__error">{error}</div>}

            <div className="execution-controls__status">
                <div className="execution-controls__stat">
                    <span>Runtime status</span>
                    <span className={`badge badge--${runtimeStatusTone(state?.runtimeStatus)}`}>
                        {state?.runtimeStatus ?? '—'}
                    </span>
                </div>
                <div className="execution-controls__stat">
                    <span>Remaining attempts</span>
                    <strong>{state?.remainingAttempts ?? '—'}</strong>
                </div>
                <div className="execution-controls__stat">
                    <span>Armed notional</span>
                    <strong>{state?.maxNotionalUsd ?? '—'} USDT</strong>
                </div>
                <div className="execution-controls__stat">
                    <span>Armed until</span>
                    <strong>{formatTime(state?.armedUntil)}</strong>
                </div>
                <div className="execution-controls__stat execution-controls__stat--wide">
                    <span>Last reason</span>
                    <strong className="reason">{state?.lastStatusReason ?? '—'}</strong>
                </div>
            </div>

            <div className="field-grid">
                <label className="field">
                    <span>Arm notional, USDT (max {config?.maxLiveNotionalUsd ?? '—'})</span>
                    <input
                        type="number"
                        value={notional}
                        onChange={(event) => setNotional(event.target.value)}
                        min={0}
                        step="0.01"
                    />
                </label>

                <label className="field">
                    <span>Expires after, seconds (default {config?.armExpiresAfterSeconds ?? '—'})</span>
                    <input
                        type="number"
                        value={expiresSeconds}
                        onChange={(event) => setExpiresSeconds(event.target.value)}
                        placeholder="default"
                        min={0}
                    />
                </label>
            </div>

            <div className="connector-card__actions">
                <button onClick={armOnce} disabled={busy || !canArm}>
                    Arm once
                </button>
                <button onClick={disarm} disabled={busy}>
                    Disarm
                </button>
                <button
                    onClick={toggleKillSwitch}
                    disabled={busy}
                    className={state?.killSwitchEnabled ? undefined : 'button--danger'}
                >
                    {state?.killSwitchEnabled ? 'Disable kill switch' : 'Enable kill switch'}
                </button>
            </div>

            {message && <div className="connector-card__message">{message}</div>}

            {!canArm && (
                <div className="connector-card__error">
                    Server Execution:Mode is "{config?.mode ?? '—'}" - arming is disabled until it is set to
                    ArmedOneShot in appsettings.
                </div>
            )}
        </section>
    )
}

export default ExecutionControls
