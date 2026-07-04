import { useEffect, useState } from 'react'

type ExchangeCredentialSummary = {
    connectorName: string
    isConfigured: boolean
    maskedApiKey: string | null
    isEnabled: boolean
    lastCheckedAt: string | null
    lastCheckStatus: string | null
    lastCheckError: string | null
    createdAt: string | null
    updatedAt: string | null
}

type CheckResult = {
    isConnected?: boolean
    status?: string
    error?: string | null
}

async function getJson<T>(url: string): Promise<T> {
    const response = await fetch(url)

    if (!response.ok) {
        const text = await response.text()
        throw new Error(`${response.status} ${response.statusText}: ${text}`)
    }

    return response.json() as Promise<T>
}

async function readErrorMessage(response: Response) {
    try {
        const body = await response.json()
        return body?.error ?? `${response.status} ${response.statusText}`
    } catch {
        return `${response.status} ${response.statusText}`
    }
}

function formatTime(value: string | null | undefined) {
    if (!value) return '—'

    const date = new Date(value)

    if (Number.isNaN(date.getTime())) return '—'

    return date.toLocaleString()
}

function ConnectorCard({
    summary,
    onChanged,
}: {
    summary: ExchangeCredentialSummary
    onChanged: () => void
}) {
    const [apiKey, setApiKey] = useState('')
    const [apiSecret, setApiSecret] = useState('')
    const [passphrase, setPassphrase] = useState('')
    // Default to enabled for a not-yet-configured connector - entering keys for the first time
    // should activate them, not silently save a disabled credential. Already-configured
    // connectors keep reflecting their actual saved state.
    const [isEnabled, setIsEnabled] = useState(summary.isConfigured ? summary.isEnabled : true)
    const [busy, setBusy] = useState(false)
    const [message, setMessage] = useState<string | null>(null)

    async function save() {
        setBusy(true)
        setMessage(null)

        try {
            const response = await fetch(`/api/execution/credentials/${summary.connectorName}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    apiKey,
                    apiSecret,
                    passphrase: passphrase || null,
                    isEnabled,
                }),
            })

            if (!response.ok) {
                throw new Error(await readErrorMessage(response))
            }

            setApiKey('')
            setApiSecret('')
            setPassphrase('')
            setMessage('Saved.')
            onChanged()
        } catch (err) {
            setMessage(err instanceof Error ? err.message : 'Save failed.')
        } finally {
            setBusy(false)
        }
    }

    async function check() {
        setBusy(true)
        setMessage(null)

        try {
            const response = await fetch(`/api/execution/credentials/${summary.connectorName}/check`, {
                method: 'POST',
            })

            const body = (await response.json()) as CheckResult
            setMessage(`${body.status ?? 'Unknown'}${body.error ? ` — ${body.error}` : ''}`)
            onChanged()
        } catch (err) {
            setMessage(err instanceof Error ? err.message : 'Check failed.')
        } finally {
            setBusy(false)
        }
    }

    async function remove() {
        if (!window.confirm(`Delete stored credentials for ${summary.connectorName}?`)) return

        setBusy(true)
        setMessage(null)

        try {
            const response = await fetch(`/api/execution/credentials/${summary.connectorName}`, {
                method: 'DELETE',
            })

            if (!response.ok) {
                throw new Error(await readErrorMessage(response))
            }

            onChanged()
        } catch (err) {
            setMessage(err instanceof Error ? err.message : 'Delete failed.')
        } finally {
            setBusy(false)
        }
    }

    return (
        <article className="panel connector-card">
            <div className="connector-card__header">
                <div>
                    <div className="connector-card__name">{summary.connectorName}</div>
                    <div className="connector-card__meta">
                        {summary.isConfigured ? `Key ${summary.maskedApiKey}` : 'Not configured'}
                    </div>
                </div>

                <div className="connector-card__badges">
                    <span className={`badge badge--${summary.isConfigured ? 'candidate' : 'neutral'}`}>
                        {summary.isConfigured ? 'Configured' : 'Empty'}
                    </span>
                    <span className={`badge badge--${summary.isEnabled ? 'candidate' : 'ignored'}`}>
                        {summary.isEnabled ? 'Enabled' : 'Disabled'}
                    </span>
                </div>
            </div>

            {summary.isConfigured && (
                <div className="connector-card__status">
                    Last check: {summary.lastCheckStatus ?? '—'} ({formatTime(summary.lastCheckedAt)})
                    {summary.lastCheckError && (
                        <div className="connector-card__error">{summary.lastCheckError}</div>
                    )}
                </div>
            )}

            <div className="field-grid">
                <label className="field">
                    <span>API key</span>
                    <input
                        type="text"
                        value={apiKey}
                        onChange={(event) => setApiKey(event.target.value)}
                        placeholder={summary.isConfigured ? 'Leave blank to keep unchanged' : 'API key'}
                        autoComplete="off"
                    />
                </label>

                <label className="field">
                    <span>API secret</span>
                    <input
                        type="password"
                        value={apiSecret}
                        onChange={(event) => setApiSecret(event.target.value)}
                        placeholder={summary.isConfigured ? 'Leave blank to keep unchanged' : 'API secret'}
                        autoComplete="new-password"
                    />
                </label>

                <label className="field">
                    <span>Passphrase (if required)</span>
                    <input
                        type="password"
                        value={passphrase}
                        onChange={(event) => setPassphrase(event.target.value)}
                        placeholder="Optional"
                        autoComplete="new-password"
                    />
                </label>

                <label className="field field--checkbox">
                    <input
                        type="checkbox"
                        checked={isEnabled}
                        onChange={(event) => setIsEnabled(event.target.checked)}
                    />
                    <span>Enabled</span>
                </label>
            </div>

            <div className="connector-card__actions">
                <button onClick={save} disabled={busy || !apiKey || !apiSecret}>
                    {busy ? 'Working...' : 'Save'}
                </button>
                <button onClick={check} disabled={busy || !summary.isConfigured}>
                    Check connection
                </button>
                <button onClick={remove} disabled={busy || !summary.isConfigured} className="button--danger">
                    Delete
                </button>
            </div>

            {message && <div className="connector-card__message">{message}</div>}
        </article>
    )
}

function ConnectorsPage() {
    const [summaries, setSummaries] = useState<ExchangeCredentialSummary[]>([])
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)

    async function load() {
        try {
            setError(null)
            const data = await getJson<ExchangeCredentialSummary[]>('/api/execution/credentials')
            setSummaries(data)
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Unknown error')
        } finally {
            setLoading(false)
        }
    }

    useEffect(() => {
        load()
    }, [])

    return (
        <main className="page">
            <header className="header">
                <div>
                    <div className="eyebrow">EXCHANGE CONNECTORS</div>
                    <h1>Connector credentials</h1>
                    <p>
                        API keys used for real balance/position checks and order placement. Secrets are
                        encrypted at rest and never redisplayed once saved.
                    </p>
                </div>

                <div className="header-actions">
                    <button onClick={load} disabled={loading}>
                        {loading ? 'Loading...' : 'Refresh'}
                    </button>
                </div>
            </header>

            {error && (
                <section className="error">
                    <strong>Request failed:</strong>
                    <span>{error}</span>
                </section>
            )}

            <section className="connectors-grid">
                {summaries.map((summary) => (
                    <ConnectorCard key={summary.connectorName} summary={summary} onChanged={load} />
                ))}

                {!loading && summaries.length === 0 && (
                    <div className="empty-block">No connectors found.</div>
                )}
            </section>
        </main>
    )
}

export default ConnectorsPage
