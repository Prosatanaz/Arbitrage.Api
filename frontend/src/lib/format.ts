export function formatUsd(value: number | null | undefined, digits = 2) {
    if (value === null || value === undefined || Number.isNaN(value)) return '—'
    return `${value.toFixed(digits)} USDT`
}

export function formatPct(value: number | null | undefined, digits = 3) {
    if (value === null || value === undefined || Number.isNaN(value)) return '—'
    return `${value.toFixed(digits)}%`
}

export function formatNumber(value: number | null | undefined) {
    if (value === null || value === undefined || Number.isNaN(value)) return '—'
    return value.toLocaleString('en-US')
}

export function formatDecimal(value: number | null | undefined, maxDigits = 4) {
    if (value === null || value === undefined || Number.isNaN(value)) return '—'
    return value.toLocaleString('en-US', { maximumFractionDigits: maxDigits })
}

export function formatSignedUsd(value: number | null | undefined, digits = 2) {
    if (value === null || value === undefined || Number.isNaN(value)) return '—'
    const sign = value > 0 ? '+' : ''
    return `${sign}${value.toFixed(digits)} USDT`
}

export function formatAge(ms: number | null | undefined) {
    if (ms === null || ms === undefined || Number.isNaN(ms)) return '—'
    if (ms < 1000) return `${Math.round(ms)} ms`
    if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`
    return `${(ms / 60_000).toFixed(1)} min`
}

export function formatTime(value: string | null | undefined) {
    if (!value) return '—'

    const date = new Date(value)

    if (Number.isNaN(date.getTime())) return '—'

    return date.toLocaleTimeString()
}

export function formatDateTime(value: string | null | undefined) {
    if (!value) return '—'

    const date = new Date(value)

    if (Number.isNaN(date.getTime())) return '—'

    return date.toLocaleString()
}

export function formatConnectorLabel(connectorName: string) {
    return connectorName
        .split('_')
        .map((part) => (part.length > 0 ? part[0].toUpperCase() + part.slice(1) : part))
        .join(' ')
}

export function getDecisionTone(decision: string | undefined) {
    const normalized = decision?.toLowerCase()

    if (normalized === 'alert') return 'alert'
    if (normalized === 'candidate') return 'candidate'
    if (normalized === 'blocked') return 'blocked'
    if (normalized === 'ignored') return 'ignored'

    return 'neutral'
}
