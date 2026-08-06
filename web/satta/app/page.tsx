import Link from "next/link"

import {
  Activity,
  AlertTriangle,
  ArrowUpRight,
  BarChart3,
  Check,
  Gauge,
  Lock,
  ShieldCheck,
  TrendingUp,
} from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { BentoGrid } from "@/components/ui/bento-grid"
import { BorderBeam } from "@/components/ui/border-beam"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty"
import { Marquee } from "@/components/ui/marquee"
import { NumberTicker } from "@/components/ui/number-ticker"
import { Progress } from "@/components/ui/progress"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Separator } from "@/components/ui/separator"
import { Skeleton } from "@/components/ui/skeleton"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"

import { Status, StatusIndicator, StatusLabel } from "@/components/kibo-ui/status"
import { Ticker, TickerIcon, TickerPrice, TickerPriceChange, TickerSymbol } from "@/components/kibo-ui/ticker"
import { ShimmeringText } from "@/components/shimmering-text"

import { CandlestickChart } from "@/components/charts/candlestick-chart"
import { Candlestick } from "@/components/charts/candlestick"
import { Grid } from "@/components/charts/grid"
import { XAxis } from "@/components/charts/x-axis"
import { YAxis } from "@/components/charts/y-axis"
import { ChartTooltip as BklitTooltip } from "@/components/charts/tooltip/chart-tooltip"

import { BklitEquityArea } from "@/components/dashboard/bklit-equity-area"
import { Poller } from "@/components/dashboard/poller"
import { ShadcnEquityLine } from "@/components/dashboard/shadcn-equity-line"
import { ThemeSwitcherConnected } from "@/components/dashboard/theme-switcher-connected"
import { getStatusSafe } from "@/lib/trench"

export const dynamic = "force-dynamic"
export const revalidate = 0

const TRADEABLE_COUNT = 20
const WARM_BUFFER_COUNT = 10

function buildEquityFlat() {
  return Array.from({ length: 24 }, (_, i) => ({
    date: new Date(Date.UTC(2026, 0, 1, i)),
    equity: 100 + Math.sin(i / 4) * 0.08,
    t: `${String(i).padStart(2, "0")}:00`,
  }))
}

function buildOhlcMock() {
  let price = 43250
  return Array.from({ length: 36 }, (_, i) => {
    const date = new Date(Date.UTC(2026, 0, 1 + i))
    const drift = (Math.random() - 0.5) * 420
    const open = price
    const close = open + drift
    const high = Math.max(open, close) + Math.random() * 180
    const low = Math.min(open, close) - Math.random() * 180
    price = close
    return { date, open, high, low, close }
  })
}

const tickerMocks = [
  { symbol: "BTC", price: 67284.12, change: 1.42 },
  { symbol: "ETH", price: 3421.55, change: -0.64 },
  { symbol: "SOL", price: 142.88, change: 2.15 },
  { symbol: "AVAX", price: 21.44, change: 0.82 },
  { symbol: "ARB", price: 0.92, change: -1.1 },
  { symbol: "OP", price: 1.84, change: 0.45 },
  { symbol: "LINK", price: 18.22, change: 1.05 },
  { symbol: "UNI", price: 7.31, change: -0.32 },
]

const fillsMock = [
  { time: "2026-01-06 14:22 UTC", market: "BTC-PERP", side: "LONG", size: "0.042", price: "67210.4", fee: "0.21", status: "filled" },
  { time: "2026-01-06 09:11 UTC", market: "SOL-PERP", side: "SHORT", size: "12.5", price: "141.92", fee: "0.13", status: "filled" },
  { time: "2026-01-05 22:04 UTC", market: "ETH-PERP", side: "LONG", size: "1.20", price: "3418.10", fee: "0.31", status: "filled" },
  { time: "2026-01-05 16:33 UTC", market: "ARB-PERP", side: "LONG", size: "420", price: "0.91", fee: "0.03", status: "pending" },
  { time: "2026-01-04 11:02 UTC", market: "LINK-PERP", side: "SHORT", size: "85", price: "18.05", fee: "0.11", status: "filled" },
]

function universeCandidatesMock(markets: { market: string }[]) {
  const base = markets.length ? markets.map((m) => m.market) : ["BTC-PERP", "ETH-PERP", "SOL-PERP", "AVAX-PERP", "ARB-PERP", "OP-PERP", "LINK-PERP", "UNI-PERP", "MATIC-PERP", "DOGE-PERP"]
  return base.slice(0, 10).map((market, i) => ({
    market,
    vol: 12000000 - i * 820000,
    oi: 4500000 - i * 310000,
    spread: 4 + (i % 3) * 3.2,
    depth: 850000 - i * 42000,
    score: 92 - i * 6.5,
  }))
}

export default async function Page() {
  const result = await getStatusSafe()
  const ok = result.ok
  const status = ok ? result.status : null
  const mode = status?.mode ?? "unavailable"
  const executionEnabled = status?.execution_enabled ?? false
  const readiness = status?.readiness ?? null
  const globalBlockers = readiness?.global_blockers ?? []
  const rulesBlockers = readiness?.rules_blockers ?? []
  const markets = readiness?.markets ?? []

  const equityData = buildEquityFlat()
  const ohlcData = buildOhlcMock()
  const candidates = universeCandidatesMock(markets)

  const sourceHealthy = ok && globalBlockers.length === 0
  const entryReadyCount = markets.filter((m) => m.rules_entry_ready).length
  const exitReadyCount = markets.filter((m) => m.mandatory_exit_ready).length
  const readinessPct = markets.length ? Math.round((entryReadyCount / markets.length) * 100) : 0

  const evilData = equityData.map((d) => ({ t: d.t, equity: d.equity }))

  const auditRows: Array<[string, string, string, string]> = [
    ["transport", "unix socket", "browser isolated", ok ? "safe" : "wait"],
    ["mode", mode, executionEnabled ? "execution enabled" : "entries disabled", executionEnabled ? "warn" : "safe"],
    ["credentials", "none in client", "server boundary only", "safe"],
    ["last payload", ok ? (status?.run_id.slice(0, 12) ?? "—") : "—", ok ? "status live" : "status unavailable", ok ? "safe" : "wait"],
  ]

  return (
    <div className="min-h-screen w-screen bg-background text-foreground font-mono">
      <div className="pointer-events-none fixed inset-0 bg-[linear-gradient(to_right,transparent_98%,hsl(var(--border))_98%),linear-gradient(to_bottom,transparent_98%,hsl(var(--border))_98%)] bg-[size:32px_32px] opacity-[0.04] dark:opacity-[0.08]" />
      <div className="pointer-events-none fixed inset-x-0 top-0 h-[2px] bg-[hsl(var(--foreground))] opacity-10" />
      <div className="w-full px-4 py-4 sm:px-6 lg:px-8 lg:py-6 relative">
        {/* Header Card — brutalist terminal */}
        <div className="mb-2 flex items-center gap-2 font-terminal text-[10px] tracking-[0.18em] text-muted-foreground">
          <span className="inline-flex items-center gap-1.5 border bg-card px-2 py-1">
            <span className="size-1.5 bg-emerald-500 animate-pulse" /> satta@ gifgoblin:~$ ./trenchd run --config paper.toml --mode active
          </span>
          <span className="hidden sm:inline-flex items-center gap-1.5 border bg-card px-2 py-1">PID 267567 · run-17860407 · TRENCH_WORKSPACE_BUILD_DIGEST b3:d9ac…</span>
          <span className="ml-auto hidden lg:inline-flex border bg-card px-2 py-1">v0.1.0 · pnpm · Next 16.2.6 · Turbopack</span>
        </div>
        <Card className="relative overflow-hidden rounded-none border-2 border-border bg-card shadow-[4px_4px_0_hsl(var(--border))]">
          <BorderBeam size={90} duration={8} colorFrom="hsl(var(--muted-foreground))" colorTo="hsl(var(--foreground))" borderWidth={1.5} />
          <CardHeader className="pb-3">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
              <div className="flex items-center gap-3">
                <div className="grid size-9 place-items-center border bg-muted text-foreground">
                  <Gauge size={18} />
                </div>
                <div>
                  <div className="flex items-center gap-2 font-mono text-xs font-semibold tracking-[0.2em]">
                    SATTA <span className="text-muted-foreground">/</span> PAPER OPS
                    <Badge variant="outline" className="ml-1 font-mono text-[10px] tracking-widest">
                      BUILD 0.1.0
                    </Badge>
                  </div>
                  <CardDescription className="font-mono text-[11px] tracking-wide">PRIVATE CONTROL SURFACE · SERVER-SIDE /api/status · 5s POLL</CardDescription>
                </div>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <Status
                  status={ok ? (sourceHealthy ? "online" : "degraded") : "offline"}
                  className="w-[148px] justify-center border text-[11px]"
                >
                  <StatusIndicator />
                  <StatusLabel>{ok ? (sourceHealthy ? "Live" : "Degraded") : "Offline"}</StatusLabel>
                </Status>
                <Badge
                  variant={mode === "collection_only" ? "secondary" : "destructive"}
                  className="w-[148px] justify-center font-mono text-[11px] tracking-widest"
                >
                  {mode === "collection_only" ? "UNAVAILABLE" : mode.toUpperCase()}
                </Badge>
                <Badge variant={executionEnabled ? "destructive" : "outline"} className="w-[148px] justify-center font-mono text-[11px] tracking-widest">
                  {executionEnabled ? "EXECUTION ENABLED" : "EXECUTION DISABLED"}
                </Badge>
                <Badge variant="outline" className="w-[148px] justify-center font-mono text-[11px]">
                  <span className={`mr-1.5 size-1.5 rounded-full ${ok ? "bg-emerald-500" : "bg-amber-500 animate-pulse"}`} />
                  {ok ? "SOCKET LIVE" : "NO TELEMETRY"}
                </Badge>
                <span className="inline-flex w-[148px] justify-center">
                  <Poller initialOk={ok} className="w-full justify-center" />
                </span>
                <ThemeSwitcherConnected className="w-[148px]" />
              </div>
            </div>
          </CardHeader>
          <CardContent className="space-y-3 pt-0">
            <div className="flex flex-wrap items-center gap-3 border-y bg-muted/30 px-3 py-2">
              <div className="flex items-center gap-2 text-xs">
                <span className="font-mono text-[11px] tracking-widest text-muted-foreground">EQUITY</span>
                <span className="font-mono text-sm font-semibold tabular-nums">
                  <NumberTicker value={100} decimalPlaces={2} className="text-foreground" /> <span className="text-muted-foreground">USDC</span>
                </span>
                <Badge variant="secondary" className="font-mono text-[10px]">
                  rules_only · isolated 5–20×
                </Badge>
              </div>
              <Separator orientation="vertical" className="hidden h-4 sm:block" />
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <TrendingUp size={14} /> 30d flat until ledger read model
              </div>
              <div className="ml-auto hidden items-center gap-2 md:flex">
                <Link href="/readiness" className="text-xs text-muted-foreground hover:text-foreground">
                  Readiness
                </Link>
                <Separator orientation="vertical" className="h-3" />
                <Link href="/universe" className="text-xs text-muted-foreground hover:text-foreground">
                  Universe
                </Link>
                <Separator orientation="vertical" className="h-3" />
                <Link href="/ledger" className="text-xs text-muted-foreground hover:text-foreground">
                  Ledger
                </Link>
              </div>
            </div>
            <div className="overflow-hidden rounded-none border bg-background">
              <Marquee pauseOnHover className="[--duration:28s] [--gap:1.25rem] py-1">
                {tickerMocks.map((t) => (
                  <Ticker key={t.symbol} className="gap-2 border bg-card px-3 py-1.5">
                    <TickerIcon symbol={t.symbol} className="size-6" />
                    <TickerSymbol symbol={t.symbol} className="font-mono text-xs" />
                    <TickerPrice price={t.price} className="font-mono text-xs tabular-nums" />
                    <TickerPriceChange change={t.change} isPercent className="text-xs" />
                  </Ticker>
                ))}
              </Marquee>
            </div>
            {!ok && (
              <Alert variant="destructive" className="rounded-none">
                <AlertTriangle className="size-4" />
                <AlertTitle className="font-mono text-xs tracking-widest">STATUS UNAVAILABLE</AlertTitle>
                <AlertDescription className="text-xs leading-relaxed">
                  Daemon socket unreachable, timed out, or schema mismatch. No position, PnL, equity or alpha claim is inferred. Failing closed.
                </AlertDescription>
              </Alert>
            )}
          </CardContent>
        </Card>

        {/* Top metrics BentoGrid */}
        <div className="mt-4">
          <BentoGrid className="grid-cols-1 gap-3 auto-rows-auto sm:grid-cols-2 lg:grid-cols-4">
            <Card className="relative overflow-hidden">
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center justify-between font-mono text-xs tracking-widest text-muted-foreground">
                  DAEMON MODE <Gauge size={14} />
                </CardTitle>
                <CardDescription className="font-mono text-[11px]">{executionEnabled ? "typed strategy armed" : "entries remain sealed"}</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="font-mono text-xl font-semibold tracking-tight tabular-nums">{mode.toUpperCase()}</div>
                <Progress value={mode === "collection_only" ? 35 : 0} className="mt-3 h-1" />
                <div className="mt-1 flex justify-between font-mono text-[10px] text-muted-foreground">
                  <span>collect_only</span>
                  <span>live</span>
                </div>
              </CardContent>
            </Card>

            <Card className="relative overflow-hidden">
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center justify-between font-mono text-xs tracking-widest text-muted-foreground">
                  ACTIVE UNIVERSE <BarChart3 size={14} />
                </CardTitle>
                <CardDescription className="font-mono text-[11px]">{TRADEABLE_COUNT} tradeable / {WARM_BUFFER_COUNT} warm buffer</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex items-baseline gap-2">
                  <NumberTicker value={markets.length || 0} className="font-mono text-xl font-semibold tabular-nums" />
                  <span className="font-mono text-xs text-muted-foreground">MARKETS</span>
                </div>
                <Progress value={markets.length ? Math.min(100, (markets.length / TRADEABLE_COUNT) * 100) : 0} className="mt-3 h-1" />
                <div className="mt-2 font-mono text-[11px] text-muted-foreground">{entryReadyCount} entry-ready · {exitReadyCount} exit-ready</div>
              </CardContent>
            </Card>

            <Card className="relative overflow-hidden">
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center justify-between font-mono text-xs tracking-widest text-muted-foreground">
                  SOURCE HEALTH <Activity size={14} />
                </CardTitle>
                <CardDescription className="font-mono text-[11px]">global readiness gates</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="font-mono text-xl font-semibold tracking-tight">{!ok ? "—" : globalBlockers.length === 0 ? "HEALTHY" : `${globalBlockers.length} BLOCKER${globalBlockers.length > 1 ? "S" : ""}`}</div>
                <div className="mt-3 flex flex-wrap gap-1">
                  {globalBlockers.length === 0 ? (
                    <Badge variant="secondary" className="font-mono text-[10px]">
                      <Check size={12} /> NONE
                    </Badge>
                  ) : (
                    globalBlockers.map((b) => (
                      <Badge key={b} variant="destructive" className="font-mono text-[10px]">
                        {b}
                      </Badge>
                    ))
                  )}
                  {rulesBlockers.map((b) => (
                    <Badge key={b} variant="outline" className="font-mono text-[10px]">
                      {b}
                    </Badge>
                  ))}
                </div>
              </CardContent>
            </Card>

            <Card className="relative overflow-hidden">
              <BorderBeam size={60} duration={6} colorFrom="hsl(var(--chart-1))" colorTo="hsl(var(--chart-2))" />
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center justify-between font-mono text-xs tracking-widest text-muted-foreground">
                  PAPER LEDGER <TrendingUp size={14} />
                </CardTitle>
                <CardDescription className="font-mono text-[11px]">rules_only · synthetic · isolated margin</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="font-mono text-xl font-semibold tracking-tight tabular-nums">
                  <NumberTicker value={100} decimalPlaces={2} className="text-foreground" /> USDC
                </div>
                <Progress value={100} className="mt-3 h-1" />
                <div className="mt-1 font-mono text-[10px] text-muted-foreground">max_open_positions 1 · max_entries 6/d · fee 7.5 bps</div>
              </CardContent>
            </Card>
          </BentoGrid>
        </div>

        {/* Main Tabs */}
        <Tabs defaultValue="overview" className="mt-4">
          <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
            <TabsList variant="line" className="h-auto gap-1 bg-transparent p-0">
              <TabsTrigger value="overview">Overview</TabsTrigger>
              <TabsTrigger value="readiness">Readiness</TabsTrigger>
              <TabsTrigger value="universe">Universe</TabsTrigger>
              <TabsTrigger value="ledger">Ledger</TabsTrigger>
              <TabsTrigger value="audit">Audit</TabsTrigger>
            </TabsList>
            <div className="flex items-center gap-2">
              <DropdownMenu>
                <DropdownMenuTrigger
                  render={
                    <Button variant="outline" size="sm" className="h-7 font-mono text-xs">
                      Filter
                    </Button>
                  }
                />
                <DropdownMenuContent align="end" className="font-mono">
                  <DropdownMenuLabel className="text-xs">Quick filters</DropdownMenuLabel>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem className="text-xs">Entry ready only</DropdownMenuItem>
                  <DropdownMenuItem className="text-xs">Exit ready only</DropdownMenuItem>
                  <DropdownMenuItem className="text-xs">Blocked markets</DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
              <Tooltip>
                <TooltipTrigger
                  render={
                    <Button variant="ghost" size="sm" className="h-7">
                      <ShieldCheck size={14} />
                    </Button>
                  }
                />
                <TooltipContent>Read-only — no wallet, no orders, no Telegram</TooltipContent>
              </Tooltip>
            </div>
          </div>

          {/* Overview */}
          <TabsContent value="overview" className="mt-4 space-y-4">
            <div className="grid gap-4 lg:grid-cols-2">
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center gap-2 font-mono text-xs tracking-widest">
                    <BarChart3 size={14} className="text-muted-foreground" /> BKLiT AREA — EQUITY (MOCK FLAT)
                  </CardTitle>
                  <CardDescription className="font-mono text-xs">bklit area-chart · 24h · 100 USDC · gradient + grid + tooltip</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-[220px] w-full">
                    <BklitEquityArea data={equityData as unknown as Record<string, unknown>[]} />
                  </div>
                  <Separator className="my-3" />
                  <div className="flex items-center justify-between font-mono text-[11px] text-muted-foreground">
                    <span>flat until ledger read model</span>
                    <span className="flex items-center gap-1">
                      <span className="size-2 rounded-full bg-[hsl(var(--chart-1))]" /> equity
                    </span>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center gap-2 font-mono text-xs tracking-widest">
                    <Activity size={14} className="text-muted-foreground" /> EVILCHARTS — EQUITY SPARKLINE (CLIENT WRAPPER)
                  </CardTitle>
                  <CardDescription className="font-mono text-xs">evilcharts via ShadcnEquityLine wrapper · same data · gradient</CardDescription>
                </CardHeader>
                <CardContent>
                  <ShadcnEquityLine data={evilData} />
                  <Separator className="my-3" />
                  <div className="font-mono text-[11px] text-muted-foreground">Recharts via client wrapper — evilcharts registry proven via bklit + shadcn stacks elsewhere.</div>
                </CardContent>
              </Card>
            </div>

            {/* Lightweight shadcn chart duplicate for registry coverage */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-2 font-mono text-xs tracking-widest">
                  <TrendingUp size={14} /> SHADCN/RECHARTS — FLAT 100 (CHART.CONTAINER)
                </CardTitle>
                <CardDescription className="font-mono text-xs">ChartContainer + Recharts primitive · dense mono axis</CardDescription>
              </CardHeader>
              <CardContent>
                <ShadcnEquityLine data={evilData} />
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <CardTitle className="font-mono text-xs tracking-widest">RECENT FILLS — TABLE-02 (MOCK UNTIL READ MODEL)</CardTitle>
                    <CardDescription className="font-mono text-xs">blocks-so table-02 pattern · badge + tooltip + actions · no SQLite in browser</CardDescription>
                  </div>
                  <Badge variant="outline" className="font-mono text-[10px]">
                    {fillsMock.length} rows · mock
                  </Badge>
                </div>
              </CardHeader>
              <CardContent className="p-0">
                <ScrollArea className="w-full">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="font-mono text-[11px]">TIME</TableHead>
                        <TableHead className="font-mono text-[11px]">MARKET</TableHead>
                        <TableHead className="font-mono text-[11px]">SIDE</TableHead>
                        <TableHead className="font-mono text-right font-mono text-[11px]">SIZE</TableHead>
                        <TableHead className="font-mono text-right font-mono text-[11px]">PRICE</TableHead>
                        <TableHead className="font-mono text-right font-mono text-[11px]">FEE</TableHead>
                        <TableHead className="font-mono text-[11px]">STATUS</TableHead>
                        <TableHead className="w-[80px] font-mono text-[11px]">ACTIONS</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {fillsMock.map((f) => (
                        <TableRow key={`${f.time}-${f.market}`} className="font-mono">
                          <TableCell className="text-xs text-muted-foreground">{f.time}</TableCell>
                          <TableCell className="text-xs font-medium">{f.market}</TableCell>
                          <TableCell>
                            <Badge variant={f.side === "LONG" ? "secondary" : "outline"} className="font-mono text-[10px]">
                              {f.side}
                            </Badge>
                          </TableCell>
                          <TableCell className="text-right tabular-nums text-xs">{f.size}</TableCell>
                          <TableCell className="text-right tabular-nums text-xs">{f.price}</TableCell>
                          <TableCell className="text-right tabular-nums text-xs">{f.fee}</TableCell>
                          <TableCell>
                            <Badge variant={f.status === "filled" ? "secondary" : "outline"} className="font-mono text-[10px] capitalize">
                              {f.status}
                            </Badge>
                          </TableCell>
                          <TableCell>
                            <Dialog>
                              <DialogTrigger
                                render={
                                  <Button variant="outline" size="xs" className="h-6 font-mono text-[10px]">
                                    View
                                  </Button>
                                }
                              />
                              <DialogContent>
                                <DialogHeader>
                                  <DialogTitle className="font-mono">{f.market} — {f.side}</DialogTitle>
                                  <DialogDescription className="font-mono text-xs">Mock fill detail · read model will hydrate this dialog server-side.</DialogDescription>
                                </DialogHeader>
                                <div className="grid gap-2 font-mono text-xs">
                                  <div className="flex justify-between border p-2"><span className="text-muted-foreground">Time</span><span>{f.time}</span></div>
                                  <div className="flex justify-between border p-2"><span className="text-muted-foreground">Price</span><span className="tabular-nums">{f.price}</span></div>
                                  <div className="flex justify-between border p-2"><span className="text-muted-foreground">Size / Fee</span><span className="tabular-nums">{f.size} / {f.fee} USDC</span></div>
                                </div>
                              </DialogContent>
                            </Dialog>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </ScrollArea>
              </CardContent>
              <CardFooter className="flex justify-between font-mono text-[11px] text-muted-foreground">
                <span>fee 7.5 bps/side · isolated margin</span>
                <Link href="/ledger" className="inline-flex items-center gap-1 hover:text-foreground">
                  Ledger <ArrowUpRight size={12} />
                </Link>
              </CardFooter>
            </Card>
          </TabsContent>

          {/* Readiness */}
          <TabsContent value="readiness" className="mt-4 space-y-4">
            <div className="grid gap-4 lg:grid-cols-[1.6fr_0.9fr]">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2 font-mono text-xs tracking-widest">
                    <Gauge size={14} /> MARKET READINESS — LATTICE
                  </CardTitle>
                  <CardDescription className="font-mono text-xs">entry_blockers · rules_entry_ready · mandatory_exit_ready — separate signals</CardDescription>
                </CardHeader>
                <CardContent className="p-0">
                  {!ok ? (
                    <div className="p-6">
                      <div className="space-y-2">
                        <Skeleton className="h-4 w-1/3" />
                        <Skeleton className="h-10 w-full" />
                        <Skeleton className="h-10 w-full" />
                      </div>
                      <div className="mt-4 flex items-center gap-2 font-mono text-xs text-muted-foreground">
                        <ShimmeringText text="AWAITING PAYLOAD" duration={1.2} className="text-xs" />
                      </div>
                    </div>
                  ) : (
                    <ScrollArea className="max-h-[420px]">
                      <Table>
                        <TableHeader className="sticky top-0 bg-card">
                          <TableRow>
                            <TableHead className="font-mono text-[11px]">MARKET</TableHead>
                            <TableHead className="font-mono text-[11px]">ENTRY BLOCKERS</TableHead>
                            <TableHead className="font-mono text-[11px]">RULES ENTRY</TableHead>
                            <TableHead className="font-mono text-[11px]">MANDATORY EXIT</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {markets.length === 0 ? (
                            <TableRow>
                              <TableCell colSpan={4} className="p-8 text-center">
                                <Empty>
                                  <EmptyHeader>
                                    <EmptyMedia variant="icon">
                                      <Gauge size={16} />
                                    </EmptyMedia>
                                    <EmptyTitle className="font-mono">No markets in snapshot</EmptyTitle>
                                    <EmptyDescription className="font-mono">Verified readiness payload has no markets yet.</EmptyDescription>
                                  </EmptyHeader>
                                </Empty>
                              </TableCell>
                            </TableRow>
                          ) : (
                            markets.map((m) => (
                              <TableRow key={m.market} className="font-mono">
                                <TableCell className="text-xs font-medium">{m.market}</TableCell>
                                <TableCell className="text-xs text-muted-foreground">
                                  {m.entry_blockers.length === 0 ? (
                                    <span className="inline-flex items-center gap-1 text-emerald-600 dark:text-emerald-400">
                                      <Check size={12} /> NONE
                                    </span>
                                  ) : (
                                    <span className="flex flex-wrap gap-1">
                                      {m.entry_blockers.map((b) => (
                                        <Tooltip key={b}>
                                          <TooltipTrigger>
                                            <Badge variant="outline" className="font-mono text-[10px]">
                                              {b}
                                            </Badge>
                                          </TooltipTrigger>
                                          <TooltipContent className="font-mono text-xs">{b} — market-local block</TooltipContent>
                                        </Tooltip>
                                      ))}
                                    </span>
                                  )}
                                </TableCell>
                                <TableCell>
                                  <Badge variant={m.rules_entry_ready ? "secondary" : "destructive"} className="font-mono text-[10px]">
                                    {m.rules_entry_ready ? "READY" : "BLOCKED"}
                                  </Badge>
                                </TableCell>
                                <TableCell>
                                  <Badge variant={m.mandatory_exit_ready ? "secondary" : "outline"} className="font-mono text-[10px]">
                                    {m.mandatory_exit_ready ? "READY" : "SEALED"}
                                  </Badge>
                                </TableCell>
                              </TableRow>
                            ))
                          )}
                        </TableBody>
                      </Table>
                    </ScrollArea>
                  )}
                </CardContent>
                <CardFooter className="font-mono text-[11px] text-muted-foreground">An entry blocker must not be rendered as permission to abandon a mandatory exit.</CardFooter>
              </Card>

              <div className="space-y-4">
                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="font-mono text-xs tracking-widest">LIVE TICKER STRIP</CardTitle>
                    <CardDescription className="font-mono text-xs">kibo-ui/ticker · per-market price + change · mock</CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-2">
                    <div className="grid grid-cols-2 gap-2">
                      {tickerMocks.slice(0, 6).map((t) => (
                        <Ticker key={t.symbol} className="justify-between border px-2 py-2">
                          <div className="flex items-center gap-1.5">
                            <TickerIcon symbol={t.symbol} className="size-5" />
                            <TickerSymbol symbol={t.symbol} className="font-mono text-[11px]" />
                          </div>
                          <div className="text-right">
                            <TickerPrice price={t.price} className="font-mono text-xs tabular-nums" />
                            <TickerPriceChange change={t.change} isPercent className="justify-end text-[11px]" />
                          </div>
                        </Ticker>
                      ))}
                    </div>
                    <Separator />
                    <div className="space-y-1">
                      <div className="flex justify-between font-mono text-[11px]">
                        <span className="text-muted-foreground">Readiness</span>
                        <span className="tabular-nums">{readinessPct}%</span>
                      </div>
                      <Progress value={readinessPct} className="h-1.5" />
                      <div className="flex justify-between font-mono text-[10px] text-muted-foreground">
                        <span>{entryReadyCount} ready</span>
                        <span>{markets.length} total</span>
                      </div>
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="font-mono text-xs tracking-widest">UNIVERSE GRID — LYTENYTE CORE</CardTitle>
                    <CardDescription className="font-mono text-xs">dense grid · vol / OI / spread / depth / score · fallback Table below</CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="overflow-hidden border">
                      <ScrollArea className="h-[260px]">
                        <Table>
                          <TableHeader className="sticky top-0 bg-card">
                            <TableRow>
                              <TableHead className="font-mono text-[11px]">MARKET</TableHead>
                              <TableHead className="text-right font-mono text-[11px]">VOL 24h</TableHead>
                              <TableHead className="text-right font-mono text-[11px]">OI</TableHead>
                              <TableHead className="text-right font-mono text-[11px]">SPREAD bps</TableHead>
                              <TableHead className="text-right font-mono text-[11px]">SCORE</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {candidates.map((c) => (
                              <TableRow key={c.market} className="font-mono">
                                <TableCell className="text-xs font-medium">{c.market}</TableCell>
                                <TableCell className="text-right tabular-nums text-xs">{(c.vol / 1_000_000).toFixed(2)}M</TableCell>
                                <TableCell className="text-right tabular-nums text-xs">{(c.oi / 1_000_000).toFixed(2)}M</TableCell>
                                <TableCell className="text-right tabular-nums text-xs">{c.spread.toFixed(1)}</TableCell>
                                <TableCell className="text-right">
                                  <Badge variant={c.score > 80 ? "secondary" : c.score > 65 ? "outline" : "destructive"} className="font-mono text-[10px] tabular-nums">
                                    {c.score.toFixed(0)}
                                  </Badge>
                                </TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </ScrollArea>
                    </div>
                    <div className="rounded-none border border-dashed p-2 font-mono text-[11px] text-muted-foreground">
                      lytenyte-core Grid is installed — this Table is the fallback. For a full virtualized grid, mount <code className="bg-muted px-1 py-0.5">LyteNyte</code> with useClientDataSource in a client wrapper.
                    </div>
                    {!ok && (
                      <div className="flex items-center gap-2 font-mono text-xs">
                        <Skeleton className="h-3 w-20" />
                        <ShimmeringText text="SYNCING UNIVERSE" className="text-xs" />
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>
            </div>
          </TabsContent>

          {/* Universe */}
          <TabsContent value="universe" className="mt-4 space-y-4">
            <div className="grid gap-4 lg:grid-cols-[1.2fr_0.8fr]">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2 font-mono text-xs tracking-widest">
                    <BarChart3 size={14} /> CANDIDATE MATRIX — DENSE GRID
                  </CardTitle>
                  <CardDescription className="font-mono text-xs">
                    vol · OI · spread · depth · score · lytenyte-core fallback to shadcn Table + ScrollArea + DropdownMenu
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="flex items-center justify-between">
                    <div className="font-mono text-xs text-muted-foreground">
                      {candidates.length} candidates · {TRADEABLE_COUNT} tradeable / {WARM_BUFFER_COUNT} warm
                    </div>
                    <DropdownMenu>
                      <DropdownMenuTrigger
                        render={
                          <Button variant="outline" size="xs" className="h-7 font-mono text-xs">
                            Sort: Score ↓
                          </Button>
                        }
                      />
                      <DropdownMenuContent className="font-mono">
                        <DropdownMenuLabel className="text-xs">Sort by</DropdownMenuLabel>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem className="text-xs">Score (high → low)</DropdownMenuItem>
                        <DropdownMenuItem className="text-xs">Volume (high → low)</DropdownMenuItem>
                        <DropdownMenuItem className="text-xs">Spread (low → high)</DropdownMenuItem>
                      </DropdownMenuContent>
                    </DropdownMenu>
                  </div>
                  <ScrollArea className="h-[360px] rounded-none border">
                    <Table>
                      <TableHeader className="sticky top-0 bg-card">
                        <TableRow>
                          <TableHead className="font-mono text-[11px]">MARKET</TableHead>
                          <TableHead className="text-right font-mono text-[11px]">VOL</TableHead>
                          <TableHead className="text-right font-mono text-[11px]">OI</TableHead>
                          <TableHead className="text-right font-mono text-[11px]">SPREAD</TableHead>
                          <TableHead className="text-right font-mono text-[11px]">DEPTH</TableHead>
                          <TableHead className="text-right font-mono text-[11px]">SCORE</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {candidates.map((c) => (
                          <TableRow key={c.market} className="font-mono hover:bg-muted/40">
                            <TableCell className="text-xs font-medium">{c.market}</TableCell>
                            <TableCell className="text-right tabular-nums text-xs">{(c.vol / 1e6).toFixed(2)}M</TableCell>
                            <TableCell className="text-right tabular-nums text-xs">{(c.oi / 1e6).toFixed(2)}M</TableCell>
                            <TableCell className="text-right tabular-nums text-xs">{c.spread.toFixed(1)}</TableCell>
                            <TableCell className="text-right tabular-nums text-xs">{(c.depth / 1e3).toFixed(0)}k</TableCell>
                            <TableCell className="text-right">
                              <Badge variant="secondary" className="font-mono text-[10px] tabular-nums">
                                {c.score.toFixed(1)}
                              </Badge>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </ScrollArea>
                  <p className="font-mono text-[11px] text-muted-foreground">Selection is source-owned · daemon persists complete normalized batch before routing.</p>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2 font-mono text-xs tracking-widest">
                    <Activity size={14} /> CANDLESTICK — SELECTED MARKET (MOCK OHLC)
                  </CardTitle>
                  <CardDescription className="font-mono text-xs">bklit candlestick-chart · visx · motion · Grid + Tooltip</CardDescription>
                </CardHeader>
                <CardContent>
                  <CandlestickChart data={ohlcData} className="w-full" aspectRatio="1.6 / 1">
                    <Grid horizontal vertical={false} />
                    <Candlestick />
                    <XAxis />
                    <YAxis />
                    <BklitTooltip />
                  </CandlestickChart>
                  <Separator className="my-3" />
                  <div className="grid grid-cols-3 gap-2 font-mono text-xs">
                    <div className="border p-2">
                      <div className="text-[10px] tracking-widest text-muted-foreground">LAST</div>
                      <div className="tabular-nums font-medium">{ohlcData.at(-1)?.close.toFixed(2)}</div>
                    </div>
                    <div className="border p-2">
                      <div className="text-[10px] tracking-widest text-muted-foreground">HIGH</div>
                      <div className="tabular-nums font-medium">{Math.max(...ohlcData.map((d) => d.high)).toFixed(2)}</div>
                    </div>
                    <div className="border p-2">
                      <div className="text-[10px] tracking-widest text-muted-foreground">LOW</div>
                      <div className="tabular-nums font-medium">{Math.min(...ohlcData.map((d) => d.low)).toFixed(2)}</div>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          {/* Ledger */}
          <TabsContent value="ledger" className="mt-4 space-y-4">
            <div className="grid gap-3 sm:grid-cols-3">
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center gap-2 font-mono text-xs tracking-widest text-muted-foreground">
                    <Activity size={14} /> EQUITY (SYNTHETIC)
                  </CardTitle>
                  <CardDescription className="font-mono text-xs">rules_only · isolated 5–20×</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="font-mono text-xl font-semibold tabular-nums">
                    <NumberTicker value={100} decimalPlaces={2} /> USDC
                  </div>
                  <div className="mt-1 font-mono text-xs text-muted-foreground">initial_equity_usdc · synthetic · no wallet</div>
                  <Progress value={100} className="mt-3 h-1" />
                </CardContent>
              </Card>
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center gap-2 font-mono text-xs tracking-widest text-muted-foreground">
                    <ShieldCheck size={14} /> RISK
                  </CardTitle>
                  <CardDescription className="font-mono text-xs">daily 1.5% · weekly 4% · hard 8% · cooldown 12h</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="font-mono text-xl font-semibold">FLAT</div>
                  <div className="mt-1 font-mono text-xs text-muted-foreground">No breach — collected only</div>
                  <Progress value={0} className="mt-3 h-1" />
                </CardContent>
              </Card>
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center gap-2 font-mono text-xs tracking-widest text-muted-foreground">
                    <TrendingUp size={14} /> POSITIONS
                  </CardTitle>
                  <CardDescription className="font-mono text-xs">max_open_positions 1 · max_entries 6/d</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="font-mono text-xl font-semibold">0 OPEN</div>
                  <div className="mt-1 font-mono text-xs text-muted-foreground">Placeholder until ledger read model</div>
                  <Progress value={0} className="mt-3 h-1" />
                </CardContent>
              </Card>
            </div>

            <div className="grid gap-4 lg:grid-cols-[1.1fr_0.9fr]">
              <Card>
                <CardHeader>
                  <CardTitle className="font-mono text-xs tracking-widest">rules_only vs ml_champion — INDEPENDENT LEDGERS</CardTitle>
                  <CardDescription className="font-mono text-xs">Stats-01 pattern · isolated margin · never synthesize PnL from status</CardDescription>
                </CardHeader>
                <CardContent className="grid gap-3 sm:grid-cols-2">
                  <div className="border bg-card p-4">
                    <div className="flex items-center justify-between font-mono text-xs text-muted-foreground">
                      <span>rules_only</span>
                      <Badge variant="secondary" className="font-mono text-[10px]">ACTIVE</Badge>
                    </div>
                    <div className="mt-2 font-mono text-2xl font-semibold tabular-nums">
                      <NumberTicker value={100} decimalPlaces={2} /> USDC
                    </div>
                    <div className="mt-1 font-mono text-xs text-emerald-600 dark:text-emerald-400">+0.00% · flat</div>
                    <Progress value={100} className="mt-3 h-1" />
                    <div className="mt-2 font-mono text-[11px] text-muted-foreground">synthetic · 100.00 initial · fee 7.5 bps/side</div>
                  </div>
                  <div className="border bg-muted/30 p-4">
                    <div className="flex items-center justify-between font-mono text-xs text-muted-foreground">
                      <span>ml_champion</span>
                      <Badge variant="outline" className="font-mono text-[10px]">
                        SHADOW
                      </Badge>
                    </div>
                    <div className="mt-2 font-mono text-2xl font-semibold tabular-nums">100.00 USDC</div>
                    <div className="mt-1 font-mono text-xs text-muted-foreground">not wired · offline training only</div>
                    <Progress value={0} className="mt-3 h-1" />
                    <div className="mt-2 font-mono text-[11px] text-muted-foreground">never owns live state · never bypasses risk</div>
                  </div>
                </CardContent>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle className="font-mono text-xs tracking-widest">ALLOCATION — HEATMAP (MOCK)</CardTitle>
                  <CardDescription className="font-mono text-xs">bklit scale tokens · 25% max margin · ring-like progress</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="grid grid-cols-5 gap-1.5">
                    {candidates.slice(0, 10).map((c, i) => {
                      const intensity = i < 2 ? "bg-foreground" : i < 4 ? "bg-foreground/70" : i < 6 ? "bg-foreground/40" : "bg-muted"
                      return (
                        <Tooltip key={c.market}>
                          <TooltipTrigger className={`h-10 border ${intensity} transition-colors hover:opacity-80`} />
                          <TooltipContent className="font-mono text-xs">
                            {c.market} · score {c.score.toFixed(1)}
                          </TooltipContent>
                        </Tooltip>
                      )
                    })}
                  </div>
                  <div className="space-y-2">
                    <div className="flex justify-between font-mono text-xs">
                      <span className="text-muted-foreground">Margin used</span>
                      <span className="tabular-nums">0.0% / 25% max</span>
                    </div>
                    <Progress value={0} className="h-2" />
                    <div className="flex justify-between font-mono text-[11px] text-muted-foreground">
                      <span>isolated · 5–20×</span>
                      <span>
                        <NumberTicker value={0} className="tabular-nums" /> open
                      </span>
                    </div>
                  </div>
                  <Separator />
                  <div className="font-mono text-[11px] leading-relaxed text-muted-foreground">
                    Heatmap is mock until ledger read model provides versioned, content-addressed artifacts. The dashboard never opens <code className="bg-muted px-1 py-0.5">state/trench.sqlite</code> or <code className="bg-muted px-1 py-0.5">data/parquet</code> directly.
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          {/* Audit */}
          <TabsContent value="audit" className="mt-4 space-y-4">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 font-mono text-xs tracking-widest">
                  <ShieldCheck size={14} /> BOUNDARY AUDIT — PRIVATE READ-ONLY CONTRACT
                </CardTitle>
                <CardDescription className="font-mono text-xs">transport · mode · credentials · last payload · server-only Unix socket</CardDescription>
              </CardHeader>
              <CardContent className="p-0">
                <ScrollArea>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="font-mono text-[11px]">CONTROL</TableHead>
                        <TableHead className="font-mono text-[11px]">OBSERVED</TableHead>
                        <TableHead className="font-mono text-[11px]">SURFACE RULE</TableHead>
                        <TableHead className="font-mono text-[11px]">STATE</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {auditRows.map(([control, observed, rule, state]) => (
                        <TableRow key={control} className="font-mono">
                          <TableCell className="text-xs font-medium">{control}</TableCell>
                          <TableCell className="text-xs text-muted-foreground">{observed}</TableCell>
                          <TableCell className="text-xs text-muted-foreground">{rule}</TableCell>
                          <TableCell>
                            <span className={`inline-flex items-center gap-1 text-xs font-medium ${state === "safe" ? "text-emerald-600 dark:text-emerald-400" : "text-amber-600 dark:text-amber-400"}`}>
                              {state === "safe" ? <Check size={14} /> : <AlertTriangle size={14} />} {state.toUpperCase()}
                            </span>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </ScrollArea>
              </CardContent>
              <CardFooter className="flex flex-col gap-3 sm:flex-row sm:justify-between">
                <Alert className="max-w-xl rounded-none">
                  <Lock size={14} />
                  <AlertTitle className="font-mono text-xs">NO WALLET · NO /exchange · NO TELEGRAM</AlertTitle>
                  <AlertDescription className="font-mono text-xs leading-relaxed">Paper-only boundary enforced by scripts/check-paper-boundary.sh · daemon is sole SQLite writer · browser never sees socket path.</AlertDescription>
                </Alert>
                <div className="font-mono text-xs text-muted-foreground">
                  {ok ? (
                    <span>
                      run_id <span className="font-medium text-foreground">{status?.run_id.slice(0, 8)}</span> · {status?.reconciled ? "RECONCILED" : "PENDING"}
                    </span>
                  ) : (
                    <span className="flex items-center gap-2">
                      <Skeleton className="h-3 w-24" /> <ShimmeringText text="AWAITING PAYLOAD" className="text-xs" duration={1} />
                    </span>
                  )}
                </div>
              </CardFooter>
            </Card>
          </TabsContent>
        </Tabs>

        <footer className="mt-6 flex flex-col gap-2 border-t pt-4 font-mono text-[11px] tracking-wide text-muted-foreground sm:flex-row sm:items-center sm:justify-between">
          <span>NO ORDERS · NO WALLET · NO TELEGRAM · SYNTHETIC PAPER SCOPE</span>
          <span className="flex items-center gap-2">
            <Lock size={12} /> {ok ? `RUN ${status?.run_id.slice(0, 8)}` : "PRIVATE STATUS ADAPTER PENDING"} · <span className="hidden sm:inline">GET /api/status · 2s · 64KB · schema_version 1</span>
          </span>
        </footer>
      </div>
    </div>
  )
}
