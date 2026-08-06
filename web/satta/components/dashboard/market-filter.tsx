"use client"

import { useMemo, useState } from "react"

import { Badge } from "@/components/ui/badge"

import type { MarketReadiness } from "@/lib/trench"

type Props = {
  markets: MarketReadiness[]
}

const toneForMarket = (symbol: string) => {
  const hash = symbol.charCodeAt(0) % 3
  if (hash === 0) return "cyan"
  if (hash === 1) return "lime"
  return "amber"
}

const toneClasses: Record<string, string> = {
  amber: "text-[#f2b56b] border-[#f2b56b]/25 bg-[#f2b56b]/[0.08]",
  cyan: "text-[#71e4df] border-[#71e4df]/25 bg-[#71e4df]/[0.08]",
  lime: "text-[#b6e875] border-[#b6e875]/25 bg-[#b6e875]/[0.08]",
  slate: "text-[#9ba9ae] border-white/[0.12] bg-white/[0.04]",
}

export function MarketFilter({ markets }: Props) {
  const [filter, setFilter] = useState("all")
  const visible = useMemo(
    () => (filter === "all" ? markets : markets.filter((m) => m.market.startsWith(filter))),
    [filter, markets],
  )

  if (markets.length === 0) {
    return <div className="text-xs text-[#778682]">No markets in current universe. Awaiting verified snapshot.</div>
  }

  return (
    <div>
      <div className="mb-4 flex items-center gap-2">
        <label htmlFor="market-filter" className="text-[10px] font-bold tracking-[0.16em] text-[#71807c]">
          MARKET FILTER
        </label>
        <select
          id="market-filter"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="border border-white/[0.13] bg-[#0b1715] px-3 py-2 text-[11px] tracking-[0.06em] text-[#d9e2df] outline-none focus-visible:outline focus-visible:outline-2 focus-visible:outline-[#b6e875]"
        >
          <option value="all">ALL MARKETS</option>
          {markets.map((m) => (
            <option key={m.market} value={m.market}>
              {m.market}
            </option>
          ))}
        </select>
        <span className="text-[10px] tracking-[0.16em] text-[#667570]">
          {visible.length} / {markets.length}
        </span>
      </div>

      <div className="grid gap-2 lg:grid-cols-3">
        {visible.map((m) => {
          const tone = toneForMarket(m.market)
          return (
            <div
              key={m.market}
              className="group flex items-center justify-between border border-white/[0.08] bg-black/15 p-4 transition-colors hover:border-white/[0.18]"
            >
              <div className="flex items-center gap-3">
                <div className={`grid size-9 place-items-center border text-[10px] font-bold ${toneClasses[tone]}`}>
                  {m.market.slice(0, 3)}
                </div>
                <div>
                  <div className="text-sm font-semibold text-[#d9e2df]">{m.market}</div>
                  <div className="mt-1 text-[10px] text-[#667570]">
                    entry {m.rules_entry_ready ? "ready" : "blocked"} · exit{" "}
                    {m.mandatory_exit_ready ? "ready" : "sealed"}
                  </div>
                  {m.entry_blockers.length > 0 && (
                    <div className="mt-1 flex flex-wrap gap-1">
                      {m.entry_blockers.map((b) => (
                        <Badge
                          key={b}
                          variant="secondary"
                          className="border-white/[0.08] bg-white/[0.06] px-1 py-0 text-[9px] tracking-[0.08em] text-[#9ba9ae]"
                        >
                          {b}
                        </Badge>
                      ))}
                    </div>
                  )}
                </div>
              </div>
              <div className="flex flex-col items-end gap-1.5">
                <span
                  className={`border px-2 py-1 text-[9px] font-bold tracking-[0.12em] ${m.rules_entry_ready ? "border-[#b6e875]/25 bg-[#b6e875]/[0.08] text-[#b6e875]" : "border-[#f2b56b]/25 bg-[#f2b56b]/[0.08] text-[#f2b56b]"}`}
                >
                  {m.rules_entry_ready ? "ENTRY READY" : "ENTRY BLOCKED"}
                </span>
                <span
                  className={`border px-2 py-1 text-[9px] font-bold tracking-[0.12em] ${m.mandatory_exit_ready ? "border-[#71e4df]/25 bg-[#71e4df]/[0.08] text-[#71e4df]" : "border-white/[0.12] text-[#778682]"}`}
                >
                  {m.mandatory_exit_ready ? "EXIT READY" : "EXIT SEALED"}
                </span>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
