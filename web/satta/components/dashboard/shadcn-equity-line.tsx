"use client"

import { CartesianGrid, Line, LineChart, XAxis as RechartsXAxis, YAxis as RechartsYAxis } from "recharts"

import { ChartContainer, ChartTooltip as ShadChartTooltip, ChartTooltipContent } from "@/components/ui/chart"

export function ShadcnEquityLine({ data }: { data: Array<{ t: string; equity: number }> }) {
  return (
    <ChartContainer config={{ equity: { label: "Equity", color: "hsl(var(--chart-2))" } }} className="h-[180px] w-full">
      <LineChart data={data} margin={{ left: 12, right: 12, top: 8, bottom: 8 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
        <RechartsXAxis dataKey="t" tick={{ fontSize: 10 }} axisLine={false} tickLine={false} interval={5} />
        <RechartsYAxis domain={[99.5, 100.5]} tick={{ fontSize: 10 }} axisLine={false} tickLine={false} width={36} />
        <ShadChartTooltip content={<ChartTooltipContent />} />
        <Line type="monotone" dataKey="equity" stroke="var(--color-equity)" strokeWidth={1.5} dot={false} />
      </LineChart>
    </ChartContainer>
  )
}
