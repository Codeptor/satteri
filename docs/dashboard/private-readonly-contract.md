# Private dashboard read-only contract

`web/satta` is a private operator view over the paper daemon. It is not an
execution surface. The dashboard must never contain a wallet, signer, exchange
action, Telegram session, private key, account credential, or order-submission
route.

## Transport boundary

The daemon's phase-1 status protocol is an authenticated Unix socket at
`/run/trenchbot/admin.sock`. A browser cannot open this socket. A future
server-side adapter may read it on the VPS and expose one authenticated,
read-only dashboard route. That adapter is outside the current deployment
scaffold; do not replace the socket with a public TCP listener or a permissive
proxy. A remotely hosted dashboard must use an approved private access path
(for example, an operator VPN or SSH tunnel) to that adapter.

The adapter must:

- allow only the versioned `status` request;
- keep the socket path and host filesystem out of the response;
- reject every `POST`, `PUT`, `PATCH`, and `DELETE` operation;
- apply a short timeout and bounded response body;
- return an unavailable/degraded result when the socket cannot be reached;
- keep all credentials in the server-side deployment, never in browser code or
  `NEXT_PUBLIC_*` variables.

## Status payload

The adapter's initial endpoint is `GET /api/status`. It projects the daemon's
status response without renaming readiness fields:

```json
{
  "schema_version": 1,
  "ok": true,
  "status": {
    "run_id": "opaque-run-id",
    "reconciled": true,
    "mode": "collection_only",
    "execution_enabled": false,
    "readiness": {
      "global_blockers": ["context_capture"],
      "rules_blockers": ["configuration", "sleeve_warmup"],
      "markets": [
        {
          "market": "SOL",
          "entry_blockers": ["common_features"],
          "rules_entry_ready": false,
          "mandatory_exit_ready": true
        }
      ]
    }
  }
}
```

`schema_version` is required and unknown fields must be ignored by the UI
projection only after the adapter has validated the response. The dashboard
must show `mode = collection_only` and `execution_enabled = false` as a
prominent, persistent paper-only state. `rules_entry_ready` and
`mandatory_exit_ready` are separate signals: an entry blocker must not be
rendered as permission to abandon a mandatory exit.

If `ok` is false, the response is missing, or the last successful response is
stale, the UI displays **status unavailable** and no readiness claim. It must
not synthesize equity, positions, PnL, alpha, or trade state from this payload.

## UI and API rules

- Poll `GET /api/status` from a server component or a same-origin read-only
  route; never fetch the daemon socket from a client component.
- Keep the first view to lifecycle, reconciliation, readiness blockers, and
  market-local entry/exit flags.
- Any future ledger/research views must consume separately versioned,
  content-addressed read models. They may not open SQLite or Parquet from the
  dashboard process.
- Do not add mutation endpoints, action buttons, order forms, leverage inputs,
  wallet fields, or “resume trading” controls to the scaffold.

The current dashboard repository is intentionally a UI scaffold. Building the
adapter, authentication, and visual monitoring views is a separate task from
the paper daemon and does not authorize live execution.
