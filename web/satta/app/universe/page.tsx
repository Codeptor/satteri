import Link from "next/link"

import { Check, Pulse, ShieldCheck, SquaresFour } from "@phosphor-icons/react/dist/ssr"

import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { MarketFilter } from "@/components/dashboard/market-filter"
import { Poller } from "@/components/dashboard/poller"
import { getStatusSafe } from "@/lib/trench"

export const dynamic = "force-dynamic"
export const revalidate = 0

const TRADEABLE_COUNT = 20
const WARM_BUFFER_COUNT = 10
const UNIVERSE_TOTAL = TRADEABLE_COUNT + WARM_BUFFER_COUNT

export default async function UniversePage() {
  const result = await getStatusSafe()
  const ok = result.ok
  const markets = ok ? (result.status.readiness.markets ?? []) : []

  return (
    <main className="min-h-screen overflow-hidden bg-[#07100f] text-[#d9e2df] selection:bg-[#b6e875]/20 selection:text-[#eaffcc]">
      <div className="pointer-events-none fixed inset-0 bg-[radial-gradient(circle_at_78%_8%,rgba(91,209,174,0.09),transparent_30%),radial-gradient(circle_at_15%_80%,rgba(21,95,104,0.10),transparent_34%)]" />
      <div className="relative mx-auto max-w-[1480px] px-5 py-5 sm:px-8 lg:px-12 lg:py-8">
        <header className="flex flex-col gap-5 border-b border-white/[0.09] pb-5 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-center gap-4">
            <div className="grid size-10 place-items-center border border-[#b6e875]/35 bg-[#b6e875]/[0.08] text-[#b6e875]">
              <Pulse weight="bold" size={21} />
            </div>
            <div>
              <div className="flex items-center gap-2 text-[11px] font-bold tracking-[0.28em] text-[#b6e875]">
                SATTA <span className="text-white/25">/</span> UNIVERSE
              </div>
              <p className="mt-1 text-[10px] tracking-[0.16em] text-[#7d8d8c]">DYNAMIC SELECTION · SOURCE-OWNED · BUILD 0.1.0</p>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2 text-[10px] font-semibold tracking-[0.14em]">
            <Badge
              variant="secondary"
              className={`rounded-none border px-3 py-2 ${ok ? "border-[#b6e875]/25 bg-[#b6e875]/[0.06] text-[#b6e875]" : "border-[#f2b56b]/25 bg-[#f2b56b]/[0.07] text-[#f2b56b]"}`}
            >
              {ok ? "STATUS LIVE" : "STATUS UNAVAILABLE"}
            </Badge>
            <Badge variant="secondary" className="rounded-none border border-white/[0.12] px-3 py-2 text-[#91a09f]">
              {TRADEABLE_COUNT} TRADEABLE
            </Badge>
            <Badge variant="secondary" className="rounded-none border border-white/[0.12] px-3 py-2 text-[#91a09f]">
              {WARM_BUFFER_COUNT} WARM BUFFER
            </Badge>
            <Poller initialOk={ok} />
          </div>
        </header>

        <nav className="mt-5 flex gap-2 text-[10px] font-bold tracking-[0.16em]">
          <Link href="/" className="border border-white/[0.10] px-3 py-2 text-[#82918d] hover:bg-white/[0.06] hover:text-[#d9e2df]">
            OVERVIEW
          </Link>
          <Link href="/readiness" className="border border-white/[0.10] px-3 py-2 text-[#82918d] hover:bg-white/[0.06] hover:text-[#d9e2df]">
            READINESS
          </Link>
          <Link href="/universe" className="bg-[#b6e875] px-3 py-2 text-[#07100f]">
            UNIVERSE
          </Link>
          <Link href="/ledger" className="border border-white/[0.10] px-3 py-2 text-[#82918d] hover:bg-white/[0.06] hover:text-[#d9e2df]">
            LEDGER
          </Link>
        </nav>

        <div className="mt-7 grid gap-6 lg:grid-cols-[1.2fr_0.8fr]">
          <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-[10px] font-bold tracking-[0.18em] text-[#71e4df]">
                <SquaresFour size={15} /> UNIVERSE SHAPE — ADMISSION IS SOURCE-BOUND
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-3 gap-3 text-center">
                <div className="border border-[#b6e875]/20 bg-[#b6e875]/[0.06] p-4">
                  <div className="text-2xl font-semibold text-[#b6e875]">{TRADEABLE_COUNT}</div>
                  <div className="mt-1 text-[10px] tracking-[0.12em] text-[#7d8d8c]">TRADEABLE</div>
                </div>
                <div className="border border-white/[0.08] bg-white/[0.04] p-4">
                  <div className="text-2xl font-semibold text-[#d9e2df]">{WARM_BUFFER_COUNT}</div>
                  <div className="mt-1 text-[10px] tracking-[0.12em] text-[#7d8d8c]">WARM BUFFER</div>
                </div>
                <div className="border border-white/[0.08] bg-black/15 p-4">
                  <div className="text-2xl font-semibold text-[#edf5ef]">{UNIVERSE_TOTAL}</div>
                  <div className="mt-1 text-[10px] tracking-[0.12em] text-[#7d8d8c]">TOTAL</div>
                </div>
              </div>
              <p className="text-xs leading-6 text-[#8f9f9c]">
                The dashboard never selects markets. The daemon&apos;s public-context capture proposes a complete normalized
                batch on the frozen universe cadence; readiness then gates entries per-market. This view shows the live
                universe projection when available, otherwise the configured shape.
              </p>
              <div className="flex flex-wrap gap-2 text-[10px] tracking-[0.12em] text-[#667570]">
                <span>FEEDS.universe_refresh_seconds = 3600</span>
                <span className="text-white/15">·</span>
                <span>max_entries_per_day = 6</span>
                <span className="text-white/15">·</span>
                <span>max_open_positions = 1</span>
              </div>
            </CardContent>
          </Card>

          <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-[10px] font-bold tracking-[0.18em] text-[#b6e875]">
                <ShieldCheck size={15} /> COVERAGE GATES
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-xs leading-6 text-[#8f9f9c]">
              <div className="flex items-center justify-between border border-white/[0.08] bg-black/15 px-3 py-2">
                <span>required_history_days</span>
                <span className="font-mono text-[#d9e2df]">30</span>
              </div>
              <div className="flex items-center justify-between border border-white/[0.08] bg-black/15 px-3 py-2">
                <span>required_bar_coverage</span>
                <span className="font-mono text-[#d9e2df]">0.995</span>
              </div>
              <div className="flex items-center justify-between border border-white/[0.08] bg-black/15 px-3 py-2">
                <span>max_effective_spread_bps</span>
                <span className="font-mono text-[#d9e2df]">15</span>
              </div>
              <div className="flex items-center justify-between border border-white/[0.08] bg-black/15 px-3 py-2">
                <span>minimum_daily_notional_usdc</span>
                <span className="font-mono text-[#d9e2df]">5,000,000</span>
              </div>
              <div className="text-[10px] tracking-[0.08em] text-[#5f6d69]">
                Deprecated linkage is forbidden — the UI displays only what the daemon has persisted atomically before
                routing.
              </div>
            </CardContent>
          </Card>
        </div>

        <Tabs defaultValue="markets" className="mt-6">
          <TabsList variant="line" className="border-b border-white/[0.09] bg-transparent p-0">
            <TabsTrigger value="markets" className="rounded-none data-[state=active]:bg-[#b6e875] data-[state=active]:text-[#07100f]">
              CURRENT MARKETS
            </TabsTrigger>
            <TabsTrigger value="table" className="rounded-none">
              TABLE
            </TabsTrigger>
          </TabsList>

          <TabsContent value="markets" className="mt-6">
            <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
              <CardHeader>
                <CardTitle className="text-[10px] font-bold tracking-[0.16em] text-[#71807c]">
                  {ok ? `${markets.length} MARKETS IN LATEST READINESS SNAPSHOT` : "AWAITING VERIFIED SNAPSHOT"}
                </CardTitle>
              </CardHeader>
              <CardContent>
                {!ok ? (
                  <div className="border border-white/[0.08] bg-black/15 p-6 text-xs leading-6 text-[#778682]">
                    No verified snapshot is available. The universe view shows the configured {TRADEABLE_COUNT} tradeable +{" "}
                    {WARM_BUFFER_COUNT} warm shape, and will populate marks (SOL, BTC, ETH perps etc.) once the daemon
                    streams live readiness. No synthetic equity is invented here.
                  </div>
                ) : (
                  <MarketFilter markets={markets} />
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="table" className="mt-6">
            <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
              <CardHeader>
                <CardTitle className="text-[10px] font-bold tracking-[0.18em] text-[#b6e875]">SNAPSHOT TABLE</CardTitle>
              </CardHeader>
              <CardContent className="p-0">
                {!ok ? (
                  <div className="p-6 text-xs text-[#778682]">Status unavailable — table hidden, failing closed.</div>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow className="border-white/[0.10] hover:bg-transparent">
                        <TableHead className="px-6 text-[10px] tracking-[0.12em] text-[#667570]">MARKET</TableHead>
                        <TableHead className="px-6 text-[10px] tracking-[0.12em] text-[#667570]">ENTRY</TableHead>
                        <TableHead className="px-6 text-[10px] tracking-[0.12em] text-[#667570]">EXIT</TableHead>
                        <TableHead className="px-6 text-[10px] tracking-[0.12em] text-[#667570]">BLOCKERS</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {markets.map((m) => (
                        <TableRow key={m.market} className="border-white/[0.06]">
                          <TableCell className="px-6 font-semibold text-[#d9e2df]">{m.market}</TableCell>
                          <TableCell className="px-6">
                            <span className={m.rules_entry_ready ? "text-[#b6e875]" : "text-[#f2b56b]"}>
                              {m.rules_entry_ready ? "READY" : "BLOCKED"}
                            </span>
                          </TableCell>
                          <TableCell className="px-6">
                            <span className={m.mandatory_exit_ready ? "text-[#71e4df]" : "text-[#778682]"}>
                              {m.mandatory_exit_ready ? "READY" : "SEALED"}
                            </span>
                          </TableCell>
                          <TableCell className="px-6 text-[#778682]">{m.entry_blockers.length ? m.entry_blockers.join(", ") : <span className="inline-flex items-center gap-1 text-[#b6e875]"><Check size={12} /> NONE</span>}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>

        <footer className="mt-8 border-t border-white/[0.09] pt-5 text-[10px] tracking-[0.12em] text-[#5f6d69]">
          UNIVERSE VIEW IS READ-ONLY — IT DOES NOT MUTATE SELECTION OR STRATEGY STATE
        </footer>
      </div>
    </main>
  )
}
