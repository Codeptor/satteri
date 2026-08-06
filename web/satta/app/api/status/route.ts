import { NextResponse } from "next/server"

import { getStatusSafe } from "@/lib/trench"

export const runtime = "nodejs"
export const dynamic = "force-dynamic"
export const revalidate = 0

// GET /api/status — server-only Unix-socket adapter.
// - Only versioned status request; no mutations.
// - Short timeout + 64KB bound, schema_version validation via lib/trench.
// - Never exposes socket path or host filesystem.
// - Returns { ok, status } on success or { ok:false } on stale/unavailable.
export async function GET() {
  const result = await getStatusSafe()

  if (!result.ok) {
    return NextResponse.json(
      { ok: false, error: result.error },
      {
        status: 503,
        headers: {
          "Cache-Control": "no-store, no-cache, must-revalidate",
          "Pragma": "no-cache",
        },
      },
    )
  }

  return NextResponse.json(
    {
      schema_version: result.schema_version,
      ok: true,
      status: result.status,
    },
    {
      headers: {
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "Pragma": "no-cache",
      },
    },
  )
}

function methodNotAllowed() {
  return NextResponse.json({ ok: false, error: "method_not_allowed" }, { status: 405 })
}

export async function POST() {
  return methodNotAllowed()
}
export async function PUT() {
  return methodNotAllowed()
}
export async function PATCH() {
  return methodNotAllowed()
}
export async function DELETE() {
  return methodNotAllowed()
}
