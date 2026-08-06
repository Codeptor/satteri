import { Fragment_Mono, Geist, JetBrains_Mono, Space_Grotesk } from "next/font/google"

import "./globals.css"
import { ThemeProvider } from "@/components/theme-provider"
import { TooltipProvider } from "@/components/ui/tooltip"
import { cn } from "@/lib/utils";

const fontSans = Geist({
  subsets: ["latin"],
  variable: "--font-sans",
})

const fontDisplay = Space_Grotesk({
  subsets: ["latin"],
  variable: "--font-display",
})

const fragmentMono = Fragment_Mono({
  subsets: ["latin"],
  variable: "--font-terminal",
  weight: "400",
})

const jetbrainsMono = JetBrains_Mono({subsets:['latin'],variable:'--font-mono'})

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html
      lang="en"
      suppressHydrationWarning
      className={cn("antialiased", fontSans.variable, fontDisplay.variable, fragmentMono.variable, "font-mono", jetbrainsMono.variable)}
    >
      <body suppressHydrationWarning>
        <ThemeProvider>
          <TooltipProvider delay={150}>{children}</TooltipProvider>
        </ThemeProvider>
      </body>
    </html>
  )
}
