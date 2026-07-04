export async function getJson<T>(url: string): Promise<T> {
    const response = await fetch(url)

    if (!response.ok) {
        const text = await response.text()
        throw new Error(`${response.status} ${response.statusText}: ${text}`)
    }

    return response.json() as Promise<T>
}

export async function postJson<T>(url: string, body?: unknown): Promise<T> {
    const response = await fetch(url, {
        method: 'POST',
        headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
    })

    const text = await response.text()
    const parsed = text ? (JSON.parse(text) as T) : ({} as T)

    if (!response.ok) {
        const reason = (parsed as { reason?: string; error?: string; Error?: string })?.reason
            ?? (parsed as { reason?: string; error?: string; Error?: string })?.error
            ?? (parsed as { reason?: string; error?: string; Error?: string })?.Error
            ?? `${response.status} ${response.statusText}`
        throw new Error(reason)
    }

    return parsed
}

export async function putJson<T>(url: string, body: unknown): Promise<T> {
    const response = await fetch(url, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    })

    if (!response.ok) {
        const text = await response.text()
        throw new Error(await extractErrorMessage(text, response))
    }

    return response.json() as Promise<T>
}

export async function deleteJson<T>(url: string): Promise<T> {
    const response = await fetch(url, { method: 'DELETE' })

    if (!response.ok) {
        const text = await response.text()
        throw new Error(await extractErrorMessage(text, response))
    }

    return response.json() as Promise<T>
}

async function extractErrorMessage(text: string, response: Response): Promise<string> {
    try {
        const body = JSON.parse(text) as { error?: string; Error?: string }
        return body?.error ?? body?.Error ?? `${response.status} ${response.statusText}`
    } catch {
        return `${response.status} ${response.statusText}`
    }
}
