"use client"

import { AreaChart } from "@/components/charts/area-chart"
import { Area } from "@/components/charts/area"
import { Grid } from "@/components/charts/grid"
import { XAxis } from "@/components/charts/x-axis"
import { YAxis } from "@/components/charts/y-axis"
import { ChartTooltip as BklitTooltip } from "@/components/charts/tooltip/chart-tooltip"

export function BklitEquityArea({ data }: { data: Record<string, unknown>[] }) {
  return (
    <AreaChart data={data} className="h-full w-full">
      <Grid horizontal vertical={false} strokeDasharray="3 3" />
      <Area dataKey="equity" fill="hsl(var(--chart-1))" stroke="hsl(var(--chart-1))" fillOpacity={0.18} strokeWidth={1.8} />
      <XAxis />
      <YAxis formatValue={(v: number) => v.toFixed(1)} />
      <BklitTooltip />
    </AreaChart>
  )
}
