import Link from "next/link"

import { ActivityIcon, LockKey, Pulse, ShieldCheck, TrendUp } from "@phosphor-icons/react/dist/ssr"

import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Separator } from "@/components/ui/separator"
import { Skeleton } from "@/components/ui/skeleton"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { EquityChart } from "@/components/dashboard/equity-chart"
import { Poller } from "@/components/dashboard/poller"
import { getStatusSafe } from "@/lib/trench"

export const dynamic = "force-dynamic"
export const revalidate = 0



export default async function LedgerPage() {
  const result = await getStatusSafe()
  const ok = result.ok
  const status = ok ? result.status : null
  const executionEnabled = status?.execution_enabled ?? false

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
                SATTA <span className="text-white/25">/</span> LEDGER
              </div>
              <p className="mt-1 text-[10px] tracking-[0.16em] text-[#7d8d8c]">RULES_ONLY · 100 USDC · ISOLATED MARGIN · BUILD 0.1.0</p>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2 text-[10px] font-semibold tracking-[0.14em]">
            <Badge variant="secondary" className={`rounded-none border px-3 py-2 ${ok ? "border-[#b6e875]/25 bg-[#b6e875]/[0.06] text-[#b6e875]" : "border-[#f2b56b]/25 bg-[#f2b56b]/[0.07] text-[#f2b56b]"}`}>
              {ok ? "STATUS LIVE" : "STATUS UNAVAILABLE"}
            </Badge>
            <Badge variant="secondary" className="rounded-none border border-white/[0.12] px-3 py-2 text-[#91a09f]">
              COLLECT_ONLY
            </Badge>
            <Badge variant={executionEnabled ? "destructive" : "secondary"} className="rounded-none border px-3 py-2">
              {executionEnabled ? "EXECUTION ENABLED" : "EXECUTION DISABLED"}
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
          <Link href="/universe" className="border border-white/[0.10] px-3 py-2 text-[#82918d] hover:bg-white/[0.06] hover:text-[#d9e2df]">
            UNIVERSE
          </Link>
          <Link href="/ledger" className="bg-[#b6e875] px-3 py-2 text-[#07100f]">
            LEDGER
          </Link>
        </nav>

        <div className="mt-7 grid gap-3 sm:grid-cols-3">
          <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2 text-[10px] font-bold tracking-[0.18em] text-[#71807c]">
                <ActivityIcon size={14} /> EQUITY (SYNTHETIC)
              </CardTitle>
              <CardDescription className="text-[11px] text-[#778682]">rules_only ledger · isolated margin 5–20x</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-semibold tracking-[-0.04em] text-[#edf5ef]">100.00 USDC</div>
              <div className="mt-1 text-[11px] text-[#778682]">initial_equity_usdc · no wallet · paper only</div>
            </CardContent>
          </Card>

          <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2 text-[10px] font-bold tracking-[0.18em] text-[#71807c]">
                <TrendUp size={14} /> POSITIONS
              </CardTitle>
              <CardDescription className="text-[11px] text-[#778682]">max_open_positions = 1 · max_entries_per_day = 6</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-semibold tracking-[-0.04em] text-[#edf5ef]">0 OPEN</div>
              <div className="mt-1 text-[11px] text-[#778682]">Placeholder until ledger read model — no synthetic state rendered</div>
            </CardContent>
          </Card>

          <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2 text-[10px] font-bold tracking-[0.18em] text-[#71807c]">
                <ShieldCheck size={14} /> RISK
              </CardTitle>
              <CardDescription className="text-[11px] text-[#778682]">daily 1.5% · weekly 4% · hard 8% · cooldown 12h</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-semibold tracking-[-0.04em] text-[#edf5ef]">FLAT</div>
              <div className="mt-1 text-[11px] text-[#778682]">No breach — collected only, execution sealed</div>
            </CardContent>
          </Card>
        </div>

        <Tabs defaultValue="equity" className="mt-6">
          <TabsList variant="line" className="border-b border-white/[0.09] bg-transparent p-0">
            <TabsTrigger value="equity" className="rounded-none data-[state=active]:bg-[#b6e875] data-[state=active]:text-[#07100f]">
              EQUITY SPARKLINE
            </TabsTrigger>
            <TabsTrigger value="trades" className="rounded-none">
              TRADES
            </TabsTrigger>
            <TabsTrigger value="about" className="rounded-none">
              ABOUT
            </TabsTrigger>
          </TabsList>

          <TabsContent value="equity" className="mt-6">
            <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-[10px] font-bold tracking-[0.18em] text-[#b6e875]">
                  <ActivityIcon size={15} /> EQUITY · rules_only (MOCK UNTIL READ MODEL)
                </CardTitle>
                <CardDescription className="text-[11px] text-[#778682]">
                  Server will serve versioned, content-addressed read models. This sparkline is a visual placeholder using the default chart component — it is not a ledger claim.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <EquityChart />
                <Separator className="my-4 bg-white/[0.08]" />
                <div className="flex flex-wrap gap-2 text-[10px] tracking-[0.08em] text-[#5f6d69]">
                  <span>FIXED FEE 7.5 bps/side</span>
                  <span className="text-white/15">·</span>
                  <span>margin mode isolated · 5–20x</span>
                  <span className="text-white/15">·</span>
                  <span>max_margin_fraction 0.25</span>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="trades" className="mt-6">
            <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
              <CardHeader>
                <CardTitle className="text-[10px] font-bold tracking-[0.16em] text-[#71807c]">TRADES — NONE RENDERED</CardTitle>
                <CardDescription className="text-[11px] text-[#778682]">
                  The dashboard must not open SQLite or Parquet. A future read model will populate this table.
                </CardDescription>
              </CardHeader>
              <CardContent className="p-0">
                <Table>
                  <TableHeader>
                    <TableRow className="border-white/[0.10] hover:bg-transparent">
                      <TableHead className="px-6 text-[10px] tracking-[0.12em] text-[#667570]">TIME</TableHead>
                      <TableHead className="px-6 text-[10px] tracking-[0.12em] text-[#667570]">MARKET</TableHead>
                      <TableHead className="px-6 text-[10px] tracking-[0.12em] text-[#667570]">SIDE</TableHead>
                      <TableHead className="px-6 text-[10px] tracking-[0.12em] text-[#667570]">SIZE</TableHead>
                      <TableHead className="px-6 text-[10px] tracking-[0.12em] text-[#667570]">PRICE</TableHead>
                      <TableHead className="px-6 text-[10px] tracking-[0.12em] text-[#667570]">FEE</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    <TableRow className="border-white/[0.06]">
                      <TableCell colSpan={6} className="px-6 py-12 text-center">
                        <div className="flex flex-col items-center gap-3">
                          <Skeleton className="h-4 w-32 bg-white/[0.06]" />
                          <Skeleton className="h-3 w-64 bg-white/[0.04]" />
                          <div className="text-xs text-[#778682]">No trades — ledger read model not yet wired. Showing skeleton placeholder, not synthesized rows.</div>
                        </div>
                      </TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="about" className="mt-6">
            <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
              <CardHeader>
                <CardTitle className="text-[10px] font-bold tracking-[0.18em] text-[#71e4df]">LEDGER INVARIANT</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3 text-xs leading-6 text-[#8f9f9c]">
                <p>
                  <span className="font-semibold text-[#d9e2df]">rules_only</span> and <span className="font-semibold text-[#d9e2df]">ml_champion</span> are independently accounted. The UI must not synthesize equity, positions, PnL, alpha, or trade state from the status payload — that payload carries only lifecycle, reconciliation, and readiness blockers.
                </p>
                <p>
                  When ledger read models exist they will be separately versioned, content-addressed artifacts. The dashboard process will never open <span className="font-mono text-[#b6e875]">state/trench.sqlite</span> or{" "}
                  <span className="font-mono text-[#b6e875]">data/parquet</span> directly.
                </p>
                <Separator className="bg-white/[0.08]" />
                <div className="grid gap-2 sm:grid-cols-2">
                  <div className="border border-white/[0.08] bg-black/15 p-3">
                    <div className="text-[10px] tracking-[0.12em] text-[#667570]">INITIAL EQUITY</div>
                    <div className="mt-1 font-mono text-[#d9e2df]">100.00 USDC (isolated)</div>
                  </div>
                  <div className="border border-white/[0.08] bg-black/15 p-3">
                    <div className="text-[10px] tracking-[0.12em] text-[#667570]">LEVERAGE</div>
                    <div className="mt-1 font-mono text-[#d9e2df]">5 — 20× · max margin 25%</div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>

        <footer className="mt-8 flex flex-col gap-3 border-t border-white/[0.09] pt-5 text-[10px] tracking-[0.12em] text-[#5f6d69] sm:flex-row sm:items-center sm:justify-between">
          <span>NO ORDERS · NO WALLET · NO TELEGRAM · SYNTHETIC PAPER SCOPE — EQUITY SHOWN IS MOCK UNTIL READ MODEL</span>
          <span className="flex items-center gap-2">
            <LockKey size={13} /> PRIVATE · SERVER-SIDE ONLY
          </span>
        </footer>
      </div>
    </main>
  )
}
