"use client"

import { useTheme } from "next-themes"
import { ThemeSwitcher } from "@/components/kibo-ui/theme-switcher"

export function ThemeSwitcherConnected({ className }: { className?: string }) {
  const { resolvedTheme, setTheme, theme } = useTheme()

  // Use `theme` (explicit) if set, otherwise fallback to resolvedTheme for display
  // next-themes `theme` can be "light" | "dark" | "system", while `resolvedTheme` is "light" | "dark"
  const current = (theme as "light" | "dark" | "system") ?? "system"

  return <ThemeSwitcher value={current} onChange={(v) => setTheme(v)} className={className} />
}
