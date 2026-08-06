"use client"

import { useTheme } from "next-themes"
import { ThemeSwitcher } from "@/components/kibo-ui/theme-switcher"

export function ThemeSwitcherConnected({ className }: { className?: string }) {
  const { setTheme, theme } = useTheme()

  const current = (theme as "light" | "dark" | "system") ?? "system"

  return <ThemeSwitcher value={current} onChange={(v) => setTheme(v)} className={className} />
}
