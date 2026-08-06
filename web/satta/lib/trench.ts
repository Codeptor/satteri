import net from "node:net"

// Private paper daemon Unix socket adapter.
// Never exposes socket path to client, never uses NEXT_PUBLIC_*,
// bounded timeout + body limit + schema_version validation.
// Phase 1: read-only status over length-prefixed Unix socket
//          as defined in crates/trenchd/src/admin.rs

const ADMIN_SCHEMA_VERSION = 1
const MAX_FRAME_BYTES = 64 * 1024
const SOCKET_TIMEOUT_MS = 2000

// Candidate socket locations. Production uses /run/trenchbot/admin.sock,
// paper.example.toml declares /run/trench/trenchd.sock, GIFGOBLIN
// dev uses /home/esoteric/trenchbot-data/run/admin.sock .
function socketCandidates(): string[] {
  const env = process.env.TRENCH_ADMIN_SOCKET?.trim()
  const candidates = [
    env ?? "",
    "/run/trenchbot/admin.sock",
    "/run/trench/trenchd.sock",
    "/home/esoteric/trenchbot-data/run/admin.sock",
  ]
  return [...new Set(candidates.filter(Boolean))]
}

export type MarketReadiness = {
  market: string
  entry_blockers: string[]
  rules_entry_ready: boolean
  mandatory_exit_ready: boolean
}

export type ReadinessSnapshot = {
  global_blockers: string[]
  rules_blockers: string[]
  markets: MarketReadiness[]
}

export type DaemonStatus = {
  run_id: string
  reconciled: boolean
  mode: string // "collection_only"
  execution_enabled: boolean
  readiness: ReadinessSnapshot
}

export type AdminResponse = {
  schema_version: number
  ok: boolean
  status?: DaemonStatus
  error?: string
}

export type StatusResult =
  | { ok: true; status: DaemonStatus; schema_version: number }
  | { ok: false; error: string }

function writeFrame(body: Buffer): Buffer {
  if (body.length === 0 || body.length > MAX_FRAME_BYTES) {
    throw new Error("frame bounds violated")
  }
  const header = Buffer.allocUnsafe(4)
  header.writeUInt32BE(body.length, 0)
  return Buffer.concat([header, body])
}

function frameError(op: string, err: unknown): Error {
  const msg = err instanceof Error ? err.message : String(err)
  return new Error(`frame ${op}: ${msg}`)
}

async function readStatusFromSocket(socketPath: string): Promise<AdminResponse> {
  const payload = Buffer.from(
    JSON.stringify({ schema_version: ADMIN_SCHEMA_VERSION, type: "status" }),
  )

  return await new Promise<AdminResponse>((resolve, reject) => {
    const chunks: Buffer[] = []
    let buffered = Buffer.alloc(0)
    let expected: number | null = null
    let done = false
    let timer: ReturnType<typeof setTimeout> | null = null

    const socket = net.createConnection({ path: socketPath })

    const fail = (err: Error) => {
      if (done) return
      done = true
      if (timer) clearTimeout(timer)
      try {
        socket.destroy()
      } catch {
        // ignore
      }
      reject(err)
    }

    const succeed = (value: AdminResponse) => {
      if (done) return
      done = true
      if (timer) clearTimeout(timer)
      try {
        socket.end()
        socket.destroy()
      } catch {
        // ignore
      }
      resolve(value)
    }

    timer = setTimeout(() => fail(new Error("timeout")), SOCKET_TIMEOUT_MS)
    socket.setTimeout(SOCKET_TIMEOUT_MS, () => fail(new Error("timeout")))

    socket.on("error", (err) => fail(frameError("connect", err)))

    socket.on("connect", () => {
      try {
        socket.write(writeFrame(payload))
      } catch (err) {
        fail(frameError("write", err))
      }
    })

    socket.on("data", (data: Buffer) => {
      if (done) return
      buffered = Buffer.concat([buffered, data])

      // drain frames (expect exactly one response frame)
      while (true) {
        if (expected === null) {
          if (buffered.length < 4) return
          expected = buffered.readUInt32BE(0)
          if (expected === 0 || expected > MAX_FRAME_BYTES) {
            fail(new Error("frame too large"))
            return
          }
          buffered = buffered.subarray(4)
        }
        if (buffered.length < expected) return
        const body = buffered.subarray(0, expected)
        // consume this frame; extra bytes are ignored (protocol is 1-req-1-resp)
        chunks.push(body)
        try {
          const parsed = JSON.parse(body.toString("utf8")) as AdminResponse
          succeed(parsed)
        } catch (err) {
          fail(new Error(`invalid JSON: ${String(err)}`))
        }
        return
      }
    })

    socket.on("close", () => {
      if (!done) fail(new Error("closed before frame"))
    })
  })
}

// Server-only: reads versioned status from private Unix socket or remote HTTP proxy.
// If TRENCH_STATUS_URL is set (Vercel → GIFGOBLIN proxy), fetch via HTTP with Bearer token.
// Otherwise try Unix sockets in order. Never leaks path/token in error.
export async function getDaemonStatus(): Promise<StatusResult> {
  // Remote HTTP proxy for Vercel (e.g. http://167.86.115.1:8787/status)
  const remoteUrl = process.env.TRENCH_STATUS_URL?.trim()
  if (remoteUrl) {
    try {
      const token = process.env.TRENCH_PROXY_TOKEN?.trim() || "satta-readonly-2026"
      const res = await fetch(remoteUrl, {
        headers: { Authorization: `Bearer ${token}` },
        cache: "no-store",
        signal: AbortSignal.timeout(SOCKET_TIMEOUT_MS),
      })
      if (!res.ok) throw new Error(`http ${res.status}`)
      const raw = (await res.json()) as AdminResponse
      if (typeof raw.schema_version !== "number" || raw.schema_version !== ADMIN_SCHEMA_VERSION) {
        return { ok: false, error: "unsupported_schema" }
      }
      if (raw.ok !== true || !raw.status) {
        return { ok: false, error: raw.error ?? "unavailable" }
      }
      const s = raw.status
      if (
        typeof s.run_id !== "string" ||
        typeof s.reconciled !== "boolean" ||
        typeof s.mode !== "string" ||
        typeof s.execution_enabled !== "boolean" ||
        !s.readiness ||
        !Array.isArray(s.readiness.global_blockers) ||
        !Array.isArray(s.readiness.rules_blockers) ||
        !Array.isArray(s.readiness.markets)
      ) {
        return { ok: false, error: "invalid_response" }
      }
      return { ok: true, status: s, schema_version: raw.schema_version }
    } catch {
      return { ok: false, error: "unavailable" }
    }
  }

  const candidates = socketCandidates()
  let lastErr: unknown = null

  for (const candidate of candidates) {
    try {
      const raw = await readStatusFromSocket(candidate)

      if (typeof raw.schema_version !== "number" || raw.schema_version !== ADMIN_SCHEMA_VERSION) {
        return { ok: false, error: "unsupported_schema" }
      }
      if (raw.ok !== true || !raw.status) {
        // daemon reported not ok (error field) — fail closed, no readiness claim
        return { ok: false, error: raw.error ?? "unavailable" }
      }

      // validate required readiness shape minimally — otherwise stale
      const s = raw.status
      if (
        typeof s.run_id !== "string" ||
        typeof s.reconciled !== "boolean" ||
        typeof s.mode !== "string" ||
        typeof s.execution_enabled !== "boolean" ||
        !s.readiness ||
        !Array.isArray(s.readiness.global_blockers) ||
        !Array.isArray(s.readiness.rules_blockers) ||
        !Array.isArray(s.readiness.markets)
      ) {
        return { ok: false, error: "invalid_response" }
      }

      return { ok: true, status: s, schema_version: raw.schema_version }
    } catch (err) {
      lastErr = err
      // try next candidate; keep lastErr for fallback
      continue
    }
  }

  // Never expose path or raw error
  void lastErr
  return { ok: false, error: "unavailable" }
}

// Helper for API route / pages: never throws, always returns {ok:false} on failure.
export async function getStatusSafe(): Promise<StatusResult> {
  try {
    return await getDaemonStatus()
  } catch {
    return { ok: false, error: "unavailable" }
  }
}
