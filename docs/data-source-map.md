# Rules-only data source map

**Status:** acquisition map for the paper-only `rules_only` deployment, including native and HIP-3 venue families
**Last reviewed:** 2026-08-06

This file is the source of truth for where each required input comes from, what
can be used for production replay, and what remains only exploratory. A source
is not production-valid merely because it contains the right columns: every
decision input must be bound to an event time, receipt/availability time,
source identity, and continuity proof.

## Required contracts

| Contract | Required data | Primary source | Free/public status | Production acceptance |
| --- | --- | --- | --- | --- |
| Perp DEX and venue metadata | DEX identity, collateral, status, margin mode, limits, deployer/fee scale, asset listing, price/size decimals, leverage and contract constraints | Hyperliquid `perpDexs`, `allPerpMetas`, `perpDexStatus`, `perpDexLimits`, `meta` / `metaAndAssetCtxs` Info API; live context capture | Public API | Preserve `(perp_dex, coin)`; native uses the empty DEX, HIP-3 uses the qualified prefix; accept only typed, receipt-time-stamped metadata and a frozen digest |
| Asset context and oracle health | Mark/oracle/external price, premium, funding, open interest, update cadence, oracle identity, status/settlement state | Live DEX-qualified `activeAssetCtx`/context capture; official `asset_ctxs` archive for historical context | API is public; official archive is requester-pays | HIP-3 rows require oracle freshness/divergence evidence, source identity, status transitions, exact timestamps, and no late decision reuse |
| Executable L2 | Ordered top-20 bids/asks, quantities, exchange time, sequence/block identity | Live `l2Book` WebSocket; official `market_data/.../l2Book` archive | WebSocket is public; official archive is requester-pays | Must pass book ordering, freshness, recovery-fence, digest, and continuity checks |
| BBO | Best bid/ask and quantities | Live `bbo` WebSocket or derived from an accepted L2 snapshot | Public live feed | Historical BBO is derived only from accepted L2; never midpoint-imputed |
| Trades | Price, quantity, side, exchange identity/time, receipt time | Live `trades` WebSocket; official node fills/trades buckets; validated third-party files | Live feed is public; official buckets are requester-pays | Required for causal candle construction and trade-stream continuity |
| Completed candles | Exact 15-minute and hourly OHLCV bars plus contributing trade identities | Derived by the repository candle aggregator from timely trades | No separate official historical archive | API candles may warm exploratory state, but late backfill cannot become a past decision input without availability proof |
| Funding history | Funding rate at every settlement/observation boundary | Public Info API funding history; asset context archive; live context stream | Public API; archive requester-pays | Preserve signed rate, source timestamp, receipt time, and gap status |
| Universe candidates | Liquidity, spread, executable depth, daily notional, listing/state, DEX status, OI capacity, fee scale, coverage, exclusions | Recomputed from metadata, accepted books, trades, funding, oracle context, and DEX status at each hourly boundary | No authoritative ready-made feed | `UniverseSelector` must recompute and digest raw candidate inputs point-in-time; native and HIP-3 sub-scores remain separately explainable |
| Risk inputs | Precision, leverage/margin mode, impact ladder, spread/depth, funding reserve distribution, OI cap, fee/settlement policy, book digest | Metadata + accepted L2/trades/funding/context/status | No authoritative ready-made policy feed | `RiskPolicy` must be constructed from raw inputs; HIP-3 cannot inherit native fee/oracle/OI assumptions; serialized policy alone is never trusted |
| Recovery/continuity | Archive manifest, REST page chain, WebSocket sequence/heartbeat range, predecessor/successor identities | Official archive manifests, paginated API evidence, or our captured WS epochs | Depends on source | Mandatory; missing proof becomes an explicit excluded gap |

## Source inventory

### 1. Official Hyperliquid live feeds — primary forward source

Use the public WebSocket for `l2Book`, `trades`, `bbo`, candles, and asset
context. The current GIFGOBLIN collector already uses this path and persists
raw normalized facts before authority admission.

- [WebSocket subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)
- [Info API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)

This is the preferred source from the moment collection starts because the
collector controls receipt timestamps, reconnect epochs, duplicate handling,
and continuity evidence.

### 1a. HIP-3 DEX-qualified surfaces — primary venue extension

HIP-3 markets are public Hyperliquid perp DEXes, not a single interchangeable
asset list. The adapter must enumerate the DEXes, query each DEX's status and
limits, and request metadata, contexts, books, funding, and candles with the
DEX-qualified market identity. For example, `xyz:SNDK` must never collide with
a native `SNDK` symbol or with an identically named market on another DEX.

- [HIP-3 builder-deployed perpetuals](https://hyperliquid.gitbook.io/hyperliquid-docs/hyperliquid-improvement-proposals-hips/hip-3-builder-deployed-perpetuals)
- [HIP-3 deployer actions, oracle updates, and OI caps](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/hip-3-deployer-actions)
- [Perpetual Info API, including DEX-qualified requests](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals)
- [Perp asset IDs](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/asset-ids)

HIP-3 acceptance requires evidence for: active DEX and asset status; isolated
margin; oracle/external-price heartbeat and divergence; exact fee scale and
deployer fee; DEX and asset OI capacity; halt/settlement transitions; funding;
and source/session availability. The official protocol makes deployers
responsible for oracle and market operation, requires isolated-only margin,
and applies a higher user fee schedule, so native-perp assumptions are not
valid substitutes. A DEX or asset with missing history remains forward-
collection-only until its own replay witnesses pass.

### 2. Official Hyperliquid archive — historical backfill candidate

The official archive contains hourly L2 book files under `market_data` and
asset-context files under `asset_ctxs`. It also documents node fills/trades
under `hl-mainnet-node-data`.

- [Historical data documentation](https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data)

Constraints:

- S3 is requester-pays; transfer is not free.
- The archive does not provide historical candles.
- Archive availability can be incomplete or delayed.
- Historical L2 alone cannot satisfy the full candle/funding/context contract.
- Archive coverage and continuity must be checked separately for each HIP-3
  DEX/asset; a native archive member cannot satisfy a HIP-3 witness.

Acquire this on a dedicated/off-VPS machine, verify every object and manifest,
then transfer only the immutable verified bundle to the deployment storage.

### 3. Public Info API — bounded backfill/warmup source

Use the public API for metadata, funding, asset context, and candle snapshots.
The candle endpoint currently exposes at most the latest 5,000 candles.

API responses fetched after the fact are not automatically valid historical
decision inputs. They may be retained as warmup evidence only when their
availability and causal boundary are proven; otherwise the affected interval
is excluded.

### 4. SonarX public L2 mirror — optional secondary source

[SonarX public snapshots](https://docs.sonarx.com/datasets/HYPERLIQUID/public-l2-snapshots)
provide CC0 top-20 L2 summaries every 20 blocks, with block height and block
timestamp. The public bucket is requester-pays and weekly with a lag.

Use only as a secondary candidate until schema, coverage, source identity, and
availability proofs are verified. It does not supply trades, candles, funding,
or asset context, so it cannot be the complete production source by itself.

### 5. CryptoHFTData — free third-party candidate to validate

[CryptoHFTData documentation](https://www.cryptohftdata.com/docs) claims
Hyperliquid L2, trades, funding, mark prices, open interest, liquidations, and
hourly Zstandard-compressed Parquet history. It advertises free signup/API
access and Hyperliquid coverage from September 2025.

Before accepting it for replay, verify:

1. Raw schema maps losslessly to our normalized event types.
2. Exchange timestamps and source availability/receipt semantics are present.
3. Gaps, revisions, and duplicate identities are exposed rather than hidden.
4. Terms/licensing permit redistribution in this open-source project.
5. A sample reproduces exact event and continuity digests after import.

Until those checks pass, treat it as exploratory/backup data, not an artifact
authorization source.

### 6. Community GitHub datasets and collectors — partial only

- [Hyperliquid historical_data](https://github.com/hyperliquid-dex/historical_data)
  contains selected trades, liquidations, and ledger updates, not the complete
  book/candle/funding/context set.
- [Community realtime collector](https://github.com/bwroniszewski/hyperliquid-realtime-data)
  demonstrates public WebSocket collection but is session-oriented and does
  not provide the repository's continuity, provenance, or atomic-write proofs.

These are useful for format fixtures or exploratory checks only.

## What the repository derives

No external source supplies the final witnesses or rules artifact. The
repository must derive and verify:

1. Hourly universe candidate inputs and `UniverseSnapshot` commitments.
2. Completed-bar feature snapshots and long-horizon rule history.
3. Raw risk inputs and canonical `RiskPolicy` commitments.
4. Recovery boundaries and excluded source gaps.
5. Rules walk-forward report and content-addressed rules artifact.
6. Paper orders, fills, positions, PnL, leverage, and breaker transitions on
   the synthetic 100 USDC ledger.

Every item above is keyed by `(perp_dex, coin)` when market-specific. Native
and HIP-3 streams are never joined by symbol alone, and aggregate reports must
retain per-DEX fee, oracle, status, OI-cap, and continuity evidence.

The dashboard reads these SQLite/readonly API outputs; it never becomes a
source of trading truth.

## Acquisition policy

- **Allowed:** public REST/WebSocket/S3 endpoints, public datasets, and normal
  rate-limited downloads.
- **Not allowed:** wallet authentication, private account feeds, credential
  scraping, rate-limit bypass, or live `/exchange` actions.
- Keep acquisition credentials off the shared GIFGOBLIN VPS. Transfer only
  verified immutable data and manifests.
- Preserve raw source bytes long enough to reproduce every promoted decision;
  cold-archive them after the hot operational window is compacted.

## Storage topology

The measured current 18-market collector writes roughly 640--700 GiB for a
30-day raw window in its present SQLite plus uncompressed-Parquet format. The
existing GIFGOBLIN filesystem has 193 GB total capacity, so it cannot be the
sole historical store.

### Recommended: B2 archive plus local hot working storage

Use [Backblaze B2](https://www.backblaze.com/cloud-storage) as the canonical
immutable archive and keep only the active/replay working set on local disk.
B2 is S3-compatible, supports Object Lock, charges no upload bandwidth, and
currently lists storage at $0.005/GB-month after the first 10 GB; egress is
free up to three times the monthly stored volume, then $0.01/GB.

At the measured rate this is approximately $3--$4/month for one 30-day raw
window, before any transfer beyond the free allowance. A 545-day raw archive
would be roughly $60--$70/month before transfers.

Never place SQLite directly on object storage. Upload closed, content-addressed
Parquet/source bundles and manifests, then stage exact shards locally for
replay/compiler jobs.

### When a separate disk-only VPS is justified

Choose a separate storage VPS only when we need a continuously mounted,
high-throughput hot filesystem for replay/import and its monthly price is below
the equivalent block-volume cost. It must be in the same region/private
network, run no wallet or Telegram services, and still replicate immutable
bundles to B2; a single storage VPS disk is not a backup.

DigitalOcean block volumes are operationally simple but list 1,000 GiB at
$100/month and 2,000 GiB at about $200/month, making them poor archival value
compared with B2: [pricing](https://docs.digitalocean.com/products/volumes/details/pricing/).

Decision: provision B2 first. Add a separate 1--2 TB hot storage host only if
the 24-hour compression/throughput benchmark shows the live compiler needs it.

## Current status

| Item | Status |
| --- | --- |
| GIFGOBLIN forward WebSocket collection | Running, rules-only scope, collection-only mode |
| HIP-3 DEX-qualified collection | Design accepted; adapter/status/oracle/fee/OI gates still need implementation and witness coverage |
| Official historical archive access | Not available; requester-pays credentials/data absent |
| External storage | Not provisioned |
| Complete verified source bundle | Missing |
| Universe/feature/risk witnesses | Not promotable without the bundle |
| Rules artifact/report | Not eligible; runtime remains fail-closed |
| Wallet, signer, Telegram, live exchange path | Intentionally absent |
