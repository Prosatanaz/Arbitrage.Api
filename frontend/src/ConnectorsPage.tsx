import { useEffect, useState } from 'react'
import { getJson, postJson, putJson, deleteJson } from './lib/http'
import { formatConnectorLabel, formatDateTime } from './lib/format'

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

function checkStatusTone(summary: ExchangeCredentialSummary) {
    if (!summary.isConfigured) return 'neutral'
    if (!summary.lastCheckStatus) return 'neutral'
    return summary.lastCheckStatus.toLowerCase() === 'connected' ? 'candidate' : 'blocked'
}

function ConnectorCard({
    summary,
    onChanged,
}: {
    summary: ExchangeCredentialSummary
    onChanged: () => void
}) {
    const [editing, setEditing] = useState(!summary.isConfigured)
    const [apiKey, setApiKey] = useState('')
    const [apiSecret, setApiSecret] = useState('')
    const [passphrase, setPassphrase] = useState('')
    // Default to enabled for a not-yet-configured connector - entering keys for the first time
    // should activate them, not silently save a disabled credential. Already-configured
    // connectors keep reflecting their actual saved state.
    const [isEnabled, setIsEnabled] = useState(summary.isConfigured ? summary.isEnabled : true)
    const [busy, setBusy] = useState(false)
    const [message, setMessage] = useState<string | null>(null)

    function resetForm() {
        setApiKey('')
        setApiSecret('')
        setPassphrase('')
        setIsEnabled(summary.isConfigured ? summary.isEnabled : true)
    }

    async function save() {
        setBusy(true)
        setMessage(null)

        try {
            await putJson(`/api/execution/credentials/${summary.connectorName}`, {
                apiKey,
                apiSecret,
                passphrase: passphrase || null,
                isEnabled,
            })

            resetForm()
            setEditing(false)
            setMessage('Saved.')
            onChanged()
        } catch (err) {
            setMessage(err instanceof Error ? err.message : 'Save failed.')
        } finally {
            setBusy(false)
        }
    }

    async function toggleEnabled() {
        setBusy(true)
        setMessage(null)

        try {
            const path = summary.isEnabled ? 'disable' : 'enable'
            await postJson(`/api/execution/credentials/${summary.connectorName}/${path}`)
            onChanged()
        } catch (err) {
            setMessage(err instanceof Error ? err.message : 'Toggle failed.')
        } finally {
            setBusy(false)
        }
    }

    async function check() {
        setBusy(true)
        setMessage(null)

        try {
            const body = await postJson<CheckResult>(`/api/execution/credentials/${summary.connectorName}/check`)
            setMessage(`${body.status ?? 'Unknown'}${body.error ? ` — ${body.error}` : ''}`)
            onChanged()
        } catch (err) {
            setMessage(err instanceof Error ? err.message : 'Check failed.')
        } finally {
            setBusy(false)
        }
    }

    async function remove() {
        if (!window.confirm(`Delete stored credentials for ${formatConnectorLabel(summary.connectorName)}?`)) return

        setBusy(true)
        setMessage(null)

        try {
            await deleteJson(`/api/execution/credentials/${summary.connectorName}`)
            onChanged()
        } catch (err) {
            setMessage(err instanceof Error ? err.message : 'Delete failed.')
        } finally {
            setBusy(false)
        }
    }

    const initials = summary.connectorName.slice(0, 2).toUpperCase()

    return (
        <article className={`panel connector-card ${summary.isConfigured ? 'connector-card--configured' : ''}`}>
            <div className="connector-card__header">
                <div className="connector-card__identity">
                    <span className="connector-card__avatar">{initials}</span>
                    <div>
                        <div className="connector-card__name">{formatConnectorLabel(summary.connectorName)}</div>
                        <div className="connector-card__meta">
                            {summary.isConfigured ? `Key ${summary.maskedApiKey}` : 'Not configured'}
                        </div>
                    </div>
                </div>

                <span className={`badge badge--${checkStatusTone(summary)}`}>
                    {summary.isConfigured ? (summary.lastCheckStatus ?? 'Not checked') : 'Empty'}
                </span>
            </div>

            {summary.isConfigured && (
                <>
                    <div className="connector-card__status">
                        <span>Last check {formatDateTime(summary.lastCheckedAt)}</span>
                        {summary.lastCheckError && (
                            <div className="connector-card__error">{summary.lastCheckError}</div>
                        )}
                    </div>

                    <label className="toggle-switch">
                        <input
                            type="checkbox"
                            checked={summary.isEnabled}
                            onChange={toggleEnabled}
                            disabled={busy}
                        />
                        <span className="toggle-switch__track" />
                        <span className="toggle-switch__label">
                            {summary.isEnabled ? 'Enabled' : 'Disabled'}
                        </span>
                    </label>
                </>
            )}

            {editing ? (
                <>
                    <div className="field-grid">
                        <label className="field">
                            <span>API key</span>
                            <input
                                type="text"
                                value={apiKey}
                                onChange={(event) => setApiKey(event.target.value)}
                                placeholder="API key"
                                autoComplete="off"
                            />
                        </label>

                        <label className="field">
                            <span>API secret</span>
                            <input
                                type="password"
                                value={apiSecret}
                                onChange={(event) => setApiSecret(event.target.value)}
                                placeholder="API secret"
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
                        {summary.isConfigured && (
                            <button
                                onClick={() => {
                                    resetForm()
                                    setEditing(false)
                                    setMessage(null)
                                }}
                                disabled={busy}
                            >
                                Cancel
                            </button>
                        )}
                    </div>
                </>
            ) : (
                <div className="connector-card__actions">
                    <button onClick={check} disabled={busy}>
                        Check connection
                    </button>
                    <button onClick={() => setEditing(true)} disabled={busy}>
                        Rewrite key
                    </button>
                    <button onClick={remove} disabled={busy} className="button--danger">
                        Delete
                    </button>
                </div>
            )}

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

    const configuredCount = summaries.filter((x) => x.isConfigured).length

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
                    <span>{configuredCount} / {summaries.length} configured</span>
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
