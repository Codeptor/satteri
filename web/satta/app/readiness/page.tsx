import Link from "next/link"

import { Check, Gauge, LockKey, Pulse, ShieldCheck, Warning } from "@phosphor-icons/react/dist/ssr"

import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Separator } from "@/components/ui/separator"
import { Skeleton } from "@/components/ui/skeleton"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Poller } from "@/components/dashboard/poller"
import { getStatusSafe } from "@/lib/trench"

export const dynamic = "force-dynamic"
export const revalidate = 0

function BlockerBadges({ blockers, emptyLabel }: { blockers: string[]; emptyLabel: string }) {
  if (blockers.length === 0) {
    return <span className="text-xs text-[#778682]">{emptyLabel}</span>
  }
  return (
    <div className="flex flex-wrap gap-1.5">
      {blockers.map((b) => (
        <Badge key={b} variant="secondary" className="rounded-none border-[#f2b56b]/25 bg-[#f2b56b]/[0.08] text-[#f2b56b]">
          {b}
        </Badge>
      ))}
    </div>
  )
}

export default async function ReadinessPage() {
  const result = await getStatusSafe()
  const ok = result.ok
  const status = ok ? result.status : null
  const readiness = status?.readiness ?? null
  const globalBlockers = readiness?.global_blockers ?? []
  const rulesBlockers = readiness?.rules_blockers ?? []
  const markets = readiness?.markets ?? []
  const executionEnabled = status?.execution_enabled ?? false
  const mode = status?.mode ?? "unavailable"

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
                SATTA <span className="text-white/25">/</span> READINESS
              </div>
              <p className="mt-1 text-[10px] tracking-[0.16em] text-[#7d8d8c]">LIFECYCLE · LATTICE · SEALED UNTIL PROVEN · BUILD 0.1.0</p>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2 text-[10px] font-semibold tracking-[0.14em]">
            <Badge variant="secondary" className={`rounded-none border px-3 py-2 ${ok ? "border-[#b6e875]/25 bg-[#b6e875]/[0.06] text-[#b6e875]" : "border-[#f2b56b]/25 bg-[#f2b56b]/[0.07] text-[#f2b56b]"}`}>
              {ok ? "STATUS LIVE" : "STATUS UNAVAILABLE"}
            </Badge>
            <Badge variant="secondary" className="rounded-none border border-white/[0.12] px-3 py-2 text-[#91a09f]">
              {mode.toUpperCase()}
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
          <Link href="/readiness" className="bg-[#b6e875] px-3 py-2 text-[#07100f]">
            READINESS
          </Link>
          <Link href="/universe" className="border border-white/[0.10] px-3 py-2 text-[#82918d] hover:bg-white/[0.06] hover:text-[#d9e2df]">
            UNIVERSE
          </Link>
          <Link href="/ledger" className="border border-white/[0.10] px-3 py-2 text-[#82918d] hover:bg-white/[0.06] hover:text-[#d9e2df]">
            LEDGER
          </Link>
        </nav>

        {!ok ? (
          <div className="mt-7 space-y-4">
            <Card className="rounded-none border-[#f2b56b]/25 bg-[#f2b56b]/[0.06]">
              <CardContent className="flex items-start gap-3 p-5 text-[11px] leading-5 text-[#b9a98f]">
                <Warning size={16} className="mt-0.5 shrink-0 text-[#f2b56b]" />
                <span>
                  Status unavailable — daemon socket unreachable, timed out, or schema mismatch. No readiness claim is
                  inferred. Failing closed. The readiness lattice below is intentionally empty until a validated status
                  response arrives server-side.
                </span>
              </CardContent>
            </Card>
            <div className="grid gap-3 sm:grid-cols-3">
              {[1, 2, 3].map((i) => (
                <Card key={i} className="rounded-none border-white/[0.10] bg-[#0b1715]/75 p-5">
                  <Skeleton className="h-4 w-24 bg-white/[0.06]" />
                  <Skeleton className="mt-4 h-8 w-32 bg-white/[0.06]" />
                  <Skeleton className="mt-3 h-3 w-full bg-white/[0.06]" />
                </Card>
              ))}
            </div>
          </div>
        ) : (
          <div className="mt-7 space-y-6">
            <div className="grid gap-3 sm:grid-cols-3">
              <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center gap-2 text-[10px] font-bold tracking-[0.18em] text-[#b6e875]">
                    <Gauge size={14} /> GLOBAL BLOCKERS
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="text-2xl font-semibold text-[#edf5ef]">{globalBlockers.length}</div>
                  <BlockerBadges blockers={globalBlockers} emptyLabel="NONE — all global gates open" />
                </CardContent>
              </Card>
              <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center gap-2 text-[10px] font-bold tracking-[0.18em] text-[#b6e875]">
                    <ShieldCheck size={14} /> RULES BLOCKERS
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="text-2xl font-semibold text-[#edf5ef]">{rulesBlockers.length}</div>
                  <BlockerBadges blockers={rulesBlockers} emptyLabel="NONE — rules sleeve warm" />
                </CardContent>
              </Card>
              <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center gap-2 text-[10px] font-bold tracking-[0.18em] text-[#71e4df]">
                    <LockKey size={14} /> EXIT vs ENTRY
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-sm leading-6 text-[#778682]">
                    <span className="font-semibold text-[#71e4df]">{markets.filter((m) => m.mandatory_exit_ready).length}</span> markets can perform a
                    mandatory exit using a recovered executable book, independent of entry gates.
                  </div>
                  <Separator className="my-3 bg-white/[0.08]" />
                  <div className="text-sm leading-6 text-[#778682]">
                    <span className="font-semibold text-[#b6e875]">{markets.filter((m) => m.rules_entry_ready).length}</span> markets are entry-ready
                    (global + market + rules gates all open).
                  </div>
                </CardContent>
              </Card>
            </div>

            <Tabs defaultValue="markets" className="w-full">
              <TabsList variant="line" className="border-b border-white/[0.09] bg-transparent p-0">
                <TabsTrigger value="markets" className="rounded-none data-[state=active]:bg-[#b6e875] data-[state=active]:text-[#07100f]">
                  PER-MARKET LATTICE
                </TabsTrigger>
                <TabsTrigger value="blockers" className="rounded-none">
                  BLOCKER GLOSSARY
                </TabsTrigger>
              </TabsList>

              <TabsContent value="markets" className="mt-6">
                <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
                  <CardHeader>
                    <CardTitle className="text-[10px] font-bold tracking-[0.16em] text-[#71807c]">
                      {markets.length} MARKETS IN CURRENT UNIVERSE · ENTRY AND MANDATORY EXIT ARE SEPARATE SIGNALS
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="p-0">
                    <Table>
                      <TableHeader>
                        <TableRow className="border-white/[0.10] hover:bg-transparent">
                          <TableHead className="px-6 text-[10px] tracking-[0.12em] text-[#667570]">MARKET</TableHead>
                          <TableHead className="px-6 text-[10px] tracking-[0.12em] text-[#667570]">ENTRY BLOCKERS</TableHead>
                          <TableHead className="px-6 text-[10px] tracking-[0.12em] text-[#667570]">RULES ENTRY</TableHead>
                          <TableHead className="px-6 text-[10px] tracking-[0.12em] text-[#667570]">MANDATORY EXIT</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {markets.length === 0 ? (
                          <TableRow className="border-white/[0.06]">
                            <TableCell colSpan={4} className="px-6 py-8 text-center text-xs text-[#778682]">
                              No markets registered in this readiness snapshot.
                            </TableCell>
                          </TableRow>
                        ) : (
                          markets.map((m) => (
                            <TableRow key={m.market} className="border-white/[0.06] hover:bg-white/[0.03]">
                              <TableCell className="px-6 font-semibold text-[#d9e2df]">{m.market}</TableCell>
                              <TableCell className="px-6 text-[#778682]">
                                {m.entry_blockers.length === 0 ? (
                                  <span className="inline-flex items-center gap-1 text-[#b6e875]">
                                    <Check size={12} /> NONE
                                  </span>
                                ) : (
                                  <span className="flex flex-wrap gap-1">
                                    {m.entry_blockers.map((b) => (
                                      <Badge key={b} variant="secondary" className="rounded-none border-white/[0.08] bg-white/[0.06] text-[#9ba9ae]">
                                        {b}
                                      </Badge>
                                    ))}
                                  </span>
                                )}
                              </TableCell>
                              <TableCell className="px-6">
                                <Badge
                                  variant="secondary"
                                  className={`rounded-none border text-[9px] ${m.rules_entry_ready ? "border-[#b6e875]/25 bg-[#b6e875]/[0.08] text-[#b6e875]" : "border-[#f2b56b]/25 bg-[#f2b56b]/[0.08] text-[#f2b56b]"}`}
                                >
                                  {m.rules_entry_ready ? "READY" : "BLOCKED"}
                                </Badge>
                              </TableCell>
                              <TableCell className="px-6">
                                <Badge
                                  variant="secondary"
                                  className={`rounded-none border text-[9px] ${m.mandatory_exit_ready ? "border-[#71e4df]/25 bg-[#71e4df]/[0.08] text-[#71e4df]" : "border-white/[0.12] text-[#778682]"}`}
                                >
                                  {m.mandatory_exit_ready ? "READY" : "SEALED"}
                                </Badge>
                              </TableCell>
                            </TableRow>
                          ))
                        )}
                      </TableBody>
                    </Table>
                  </CardContent>
                </Card>
                <p className="mt-3 text-[10px] leading-5 tracking-[0.06em] text-[#5f6d69]">
                  An entry blocker must not be rendered as permission to abandon a mandatory exit. Execution readiness uses only
                  recovered + executable-book gates.
                </p>
              </TabsContent>

              <TabsContent value="blockers" className="mt-6">
                <div className="grid gap-4 lg:grid-cols-2">
                  <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
                    <CardHeader>
                      <CardTitle className="text-[10px] font-bold tracking-[0.18em] text-[#b6e875]">GLOBAL</CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-2 text-xs leading-6 text-[#8f9f9c]">
                      <div>
                        <span className="font-semibold text-[#d9e2df]">ntp</span> — no positive NTP-health assertion
                      </div>
                      <div>
                        <span className="font-semibold text-[#d9e2df]">sqlite_reconciliation</span> — recovery incomplete
                      </div>
                      <div>
                        <span className="font-semibold text-[#d9e2df]">storage</span> — atomic store not writable
                      </div>
                      <div>
                        <span className="font-semibold text-[#d9e2df]">stream</span> — public market-data disconnected
                      </div>
                      <div>
                        <span className="font-semibold text-[#d9e2df]">metadata</span> — universe metadata absent/stale
                      </div>
                      <div>
                        <span className="font-semibold text-[#d9e2df]">context_capture</span> — bounded public-context batch not persisted
                      </div>
                      <div>
                        <span className="font-semibold text-[#d9e2df]">fresh_books</span> — required executable books missing
                      </div>
                    </CardContent>
                  </Card>
                  <Card className="rounded-none border-white/[0.10] bg-[#0b1715]/75">
                    <CardHeader>
                      <CardTitle className="text-[10px] font-bold tracking-[0.18em] text-[#b6e875]">RULES & MARKET</CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-2 text-xs leading-6 text-[#8f9f9c]">
                      <div>
                        <span className="font-semibold text-[#d9e2df]">configuration</span> — frozen rules artifact not validated
                      </div>
                      <div>
                        <span className="font-semibold text-[#d9e2df]">sleeve_warmup</span> — rules sleeve history not warmed
                      </div>
                      <div>
                        <span className="font-semibold text-[#d9e2df]">universe_witness / risk_witness</span> — source-bound witnesses absent
                      </div>
                      <Separator className="bg-white/[0.08]" />
                      <div>
                        <span className="font-semibold text-[#d9e2df]">recovery / executable_book / common_features / data_quality / stale_bbo / stale_all_mids</span> — market-local; quarantine is per-market
                      </div>
                    </CardContent>
                  </Card>
                </div>
              </TabsContent>
            </Tabs>
          </div>
        )}

        <footer className="mt-8 flex flex-col gap-3 border-t border-white/[0.09] pt-5 text-[10px] tracking-[0.12em] text-[#5f6d69] sm:flex-row sm:items-center sm:justify-between">
          <span>READINESS PROJECTION ONLY — DO NOT SYNTHESIZE EQUITY OR POSITIONS FROM THIS PAYLOAD</span>
          <span className="flex items-center gap-2">
            <LockKey size={13} /> SERVER-SIDE /api/status · FAIL CLOSED
          </span>
        </footer>
      </div>
    </main>
  )
}
