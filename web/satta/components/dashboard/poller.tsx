"use client"

import { useEffect, useState } from "react"

type PollState = "live" | "stale" | "checking"

export function Poller({ initialOk, className }: { initialOk: boolean; className?: string }) {
  const [state, setState] = useState<PollState>(initialOk ? "live" : "stale")

  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      setState("checking")
      try {
        const res = await fetch("/api/status", { cache: "no-store" })
        const json = (await res.json()) as { ok: boolean }
        if (cancelled) return
        setState(json.ok ? "live" : "stale")
        // If the server has recovered from stale, soft-refresh to get fresh SSR props
        if (json.ok && state === "stale") {
          window.location.reload()
        }
      } catch {
        if (!cancelled) setState("stale")
      }
    }
    const id = window.setInterval(tick, 5000)
    return () => {
      cancelled = true
      window.clearInterval(id)
    }
  }, [state])

  return (
    <span
      className={`inline-flex items-center justify-center gap-1.5 border px-2 py-1 text-[9px] font-bold tracking-[0.12em] ${
        state === "live"
          ? "border-[#b6e875]/25 bg-[#b6e875]/[0.08] text-[#b6e875]"
          : state === "checking"
            ? "border-white/[0.12] text-[#91a09f]"
            : "border-[#f28b8b]/25 bg-[#f28b8b]/[0.08] text-[#f28b8b]"
      } ${className ?? ""}`}
    >
      <span
        className={`size-1.5 rounded-full ${state === "live" ? "bg-[#b6e875] animate-pulse" : state === "checking" ? "bg-[#91a09f]" : "bg-[#f28b8b]"}`}
      />
      {state === "live" ? "LIVE" : state === "checking" ? "CHECKING" : "STALE"}
    </span>
  )
}
