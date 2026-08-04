# Recovery outcomes are descriptor-bound source members

Status: approved for the paper-research evidence pipeline.

A reconciled recovery result is system evidence, not a raw venue fact. It must
not be added to `MarketEventKind`, and a compiler caller must not supply an
unbound `completed_at` timestamp. Either path would allow historical research
to present a recovery as available before the result was actually captured.

Each recovery outcome is therefore published as its own immutable,
descriptor-bound companion source member. Its canonical payload commits the
request identity and cursors, status/source, completed-through boundary,
anchor and backfill references, official-candle references, result digest, and
one `availability_anchor` raw-event reference. A raw reference carries the
selected member manifest digest and the full `(received_at, event_time,
event_id)` coordinate; it never carries a path.

The companion manifest and payload use the same private-directory, regular
file, no-symlink, staged/no-replace publication rules as other research source
evidence. Opening a source plan revalidates every companion member against the
plan's provenance and exact selected raw members before exposing a verified
outcome to the compiler.

Recovery becomes available only when the final-run cursor reaches the exact
`availability_anchor` key. `completed_through` is historical event time and
never a readiness clock. A post-recovery executable book must have a strictly
later full availability key, not merely a later timestamp. Only reconciled
captured-trade outcomes can release a market; unavailable or archive-only
outcomes remain quarantined.

This adds a separate evidence format and source-plan binding, but preserves the
meaning and schema provenance of the raw venue-event stream. It also makes
late/out-of-order equal-time ordering auditable and fail-closed.
