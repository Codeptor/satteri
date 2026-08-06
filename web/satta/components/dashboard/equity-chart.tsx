"use client"

import { CartesianGrid, Line, LineChart, XAxis, YAxis } from "recharts"

import { ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart"

const mockEquity = Array.from({ length: 24 }, (_, i) => ({
  t: `${String(i).padStart(2, "0")}:00`,
  equity: 100 + Math.sin(i / 3) * 0.3 + (i % 7 === 0 ? 0.12 : 0),
}))

const chartConfig = {
  equity: {
    label: "Equity (USDC)",
    color: "#b6e875",
  },
}

export function EquityChart() {
  return (
    <ChartContainer config={chartConfig} className="h-[220px] w-full">
      <LineChart data={mockEquity} margin={{ left: 12, right: 12, top: 8, bottom: 8 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
        <XAxis dataKey="t" tick={{ fill: "#667570", fontSize: 10 }} axisLine={false} tickLine={false} interval={5} />
        <YAxis domain={[99, 101]} tick={{ fill: "#667570", fontSize: 10 }} axisLine={false} tickLine={false} width={36} />
        <ChartTooltip content={<ChartTooltipContent />} />
        <Line type="monotone" dataKey="equity" stroke="var(--color-equity)" strokeWidth={1.5} dot={false} />
      </LineChart>
    </ChartContainer>
  )
}
