# Research Evidence Compiler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a paper-only offline compiler that turns explicitly selected, committed Parquet shards into a verified, atomically published research sidecar that can drive a streaming production-Engine rules replay without enabling entries.

**Architecture:** The compiler accepts a bounded source-member input with no stored filesystem paths, reopens each exact Parquet or capture-batch member through a no-scan resolver, and external-sorts the facts by `(received_at, event_time, event_id)` into a content-addressed availability run. It compiles only raw, recomputable recovery/universe/feature/risk witnesses plus canonical excluded gaps into a staged sidecar. A streaming adapter verifies those witnesses while it advances one Engine state; it never materializes a global facts map, alters `RulesStartup`, creates an artifact, or writes SQLite.

**Tech Stack:** Rust 2024; `trench-core` deterministic Engine, feature, universe, risk, and validation types; `trench-storage` Parquet/Arrow, BLAKE3, serde JSON, rustix; `trenchd` Clap CLI; tempfile-backed integration tests.

---

## File map

- Modify: `crates/trench-storage/src/lib.rs` — export the research-plan, availability-run, sidecar, compiler, and streaming-replay modules.
- Modify: `crates/trench-storage/src/parquet.rs` — expose a strongly typed partition identity and a direct, symlink-safe member reader that validates one requested legacy/capture member without discovery.
- Create: `crates/trench-storage/src/research_plan.rs` — immutable source locators, interval/continuity coverage inputs, draft/build state, final plan validation, and a root-bound verified-plan handle.
- Create: `crates/trench-storage/src/research_runs.rs` — bounded per-member sort runs, 64-way multi-pass merge, final availability cursor, and run-digest validation.
- Create: `crates/trench-storage/src/research_sidecar.rs` — raw witness records, canonical coverage exclusions, bounded shard manifests, and atomic sidecar writer/reader.
- Create: `crates/trench-storage/src/research_compile.rs` — `ResearchEvidenceCompiler`, causal source processing, and recomputation of recovery, universe, feature, and risk witnesses.
- Create: `crates/trench-storage/src/research_stream.rs` — `StreamingRuleReplay`, sidecar lookup, bounded Engine-state fold, and fold-end persistence checkpoint.
- Modify: `crates/trench-storage/src/research.rs` — extract only the reusable Engine/broker transition helpers required by both the bounded fixture adapter and `StreamingRuleReplay`; retain the bounded `EngineRuleReplay` API and its limits unchanged.
- Modify: `crates/trenchd/src/commands.rs` — add offline `research source-plan` and `research compile-sidecar` commands; retain the current fail-closed `research rules` command and `RulesStartup` behavior.
- Create: `crates/trench-storage/tests/research_plan.rs` — direct resolver, source-plan identity, coverage, and path-hardening integration tests.
- Create: `crates/trench-storage/tests/research_runs.rs` — ordering, multi-pass merge, >64-member, and >100,000-event tests.
- Create: `crates/trench-storage/tests/research_sidecar.rs` — staged-publication, tamper, bounded-manifest, and deterministic-byte tests.
- Create: `crates/trench-storage/tests/research_compiler.rs` — causal decision clock, recovery/universe/feature/risk recomputation, gaps, and replay parity tests.
- Modify: `crates/trenchd/src/commands.rs` test module — command-boundary tests proving compilation cannot emit artifacts or change runtime readiness.
- Modify: `AGENTS.md` — record the source-plan/sidecar layout and the continued rules-entry seal once the implementation is complete.

## Shared implementation rules

- Keep `DeterministicReplay::{MAX_REPLAY_EVENTS, MAX_REPLAY_WIRE_BYTES}` unchanged. Large research must use only the new availability-run cursor.
- A source-plan member contains a validated identity and digest, never an absolute/relative source path. The configured canonical Parquet root is an execution-time argument, never serialized into the plan or sidecar.
- Preserve the existing `DataProvenance` equality rule. A source plan has exactly one config/code/schema provenance tuple; mixed provenance fails before a run is written.
- The only canonical availability key is `(received_at, event_time, event_id)`. Do not use an event's kind as a tie-breaker and do not reorder delayed facts to their event time.
- Every final persisted object is bounded, canonical JSON or fixed binary, fsynced, private (`0700` directories, `0600` files), self-digested, and only made visible by one rename. Source-plan construction has a private, interruptible draft directory, but it publishes its manifest and final availability run together only after both are verified. Existing final content is idempotent only if byte-identical.
- No implementation task may add wallet, signer, account, Telegram, AWS, HTTP acquisition, live exchange, `/exchange`, mutable daemon control, or strategy-artifact code. `trenchd` remains the only SQLite writer; these offline commands must not open SQLite.
- Before every commit step, run the project gate below in addition to the task's focused red/green test. Do not commit if any command fails:

  ```bash
  cargo fmt --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace
  ./scripts/check-paper-boundary.sh
  git diff --check
  ```

- Implement each task in its own worktree/branch. After an independent review, cherry-pick its tested commit onto `main`, rerun the gate there, push `main`, and remove the merged worktree. Never push a task branch as the public release branch.

### Task 1: Exact no-scan Parquet member resolution

**Files:**
- Modify: `crates/trench-storage/src/parquet.rs`
- Modify: `crates/trench-storage/src/lib.rs`
- Test: `crates/trench-storage/tests/research_plan.rs`

- [ ] **Step 1: Write failing direct-resolution tests for both stored layouts.**

  Add helpers that write one legacy partition and one capture batch, retain their returned manifests, then request them by identity. Assert that direct reads return the committed manifest and canonical event rows. Add cases for a missing member, a changed partition/capture manifest digest, a duplicated requested identity, and a symlink substituted for the final member directory or payload.

  ```rust
  #[test]
  fn direct_capture_member_read_revalidates_batch_and_partition() {
      let member = capture_member(&store, &events);
      let opened = store
          .open_capture_member(member.request())
          .expect("verified member");
      assert_eq!(opened.manifest(), member.partition_manifest());
      assert_eq!(opened.read_all().expect("rows"), events);
  }
  ```

- [ ] **Step 2: Run the targeted test to verify it fails.**

  Run: `cargo test -p trench-storage --test research_plan direct_capture_member_read_revalidates_batch_and_partition -- --exact`

  Expected: FAIL because the typed research member identity and direct reader do not exist.

- [ ] **Step 3: Add typed identities and a direct reader without discovery.**

  In `parquet.rs`, promote the validated partition key needed to reconstruct an exact final path to a public, field-private `PartitionIdentity`; add `PartitionManifest::identity()` and canonical `manifest_digest()` accessors. Add the equivalent canonical digest accessor for `CaptureBatchManifest`. Replace `write_capture_batch` / `write_capture_batch_with_failure`'s `Vec<PartitionManifest>` return with the complete `CaptureBatchManifest`, then update their existing callers/tests; the manifest exposes the batch ID and exact partition membership needed to create a capture locator. Keep legacy `write_events` returning its per-partition manifests.

  Implement direct legacy and capture-member readers on `ParquetStore` (the source-plan locator added in Task 2 dispatches to these readers) which:

  ```rust
  // Pseudocode: all path components are derived from validated identity fields.
  let path = match requested_layout {
      Legacy { identity, partition_id, .. } =>
          legacy_final_partition_path(root, identity, partition_id),
      Capture { batch_id, identity, partition_id, .. } =>
          capture_final_partition_path(root, batch_id, identity, partition_id),
  };
  reject_symlink_everywhere(&path)?;
  let actual = read_exact_manifest_and_rows(path)?;
  require_equal(actual.manifest_digest(), requested_partition_manifest_digest)?;
  require_capture_membership_if_needed(actual, requested_batch_manifest_digest)?;
  ```

  It must call neither `ParquetStore::partitions` nor any directory-scanning helper. Preserve `read_partition` for bounded replay; do not loosen its behavior in this task.

- [ ] **Step 4: Run storage formatting, targeted tests, and the complete storage suite.**

  Run: `cargo fmt --check && cargo test -p trench-storage --test research_plan && cargo test -p trench-storage`

  Expected: PASS, including direct legacy/capture success, digest drift, and symlink rejection cases.

- [ ] **Step 5: Commit the resolver layer.**

  ```bash
  git add crates/trench-storage/src/lib.rs crates/trench-storage/src/parquet.rs crates/trench-storage/tests/research_plan.rs
  git commit -m "feat(storage): resolve research members directly"
  ```

### Task 2: Immutable source plans and proof-backed coverage declarations

**Files:**
- Create: `crates/trench-storage/src/research_plan.rs`
- Modify: `crates/trench-storage/src/lib.rs`
- Test: `crates/trench-storage/tests/research_plan.rs`

- [ ] **Step 1: Write failing tests for canonical plans and coverage witnesses.**

  Cover: a plan with a legacy locator plus a capture locator; canonical source ordering independent of input order; duplicate source identities; mixed provenance; source-plan JSON that contains a path-like field; an operator-only `Complete` assertion; reversed/overlapping requested intervals; and `ObservedNoEvents` carrying the same continuity proof requirements as `Complete`.

  Use an in-test canonical page-chain/sequence fixture containing its source bytes, UTC interval, predecessor, successor, and digest. The test should establish that a complete witness is accepted only when the proof's source IDs/range/digest match the member facts; an `Unavailable` witness remains valid and produces no claim of coverage.

- [ ] **Step 2: Run the targeted source-plan tests to verify failure.**

  Run: `cargo test -p trench-storage --test research_plan source_plan -- --nocapture`

  Expected: FAIL because source-plan and coverage types are absent.

- [ ] **Step 3: Implement plan and coverage contracts with an acyclic digest graph.**

  Define, validate, and canonically serialize:

  ```rust
  pub enum ResearchMemberLocator {
      LegacyPartition {
          identity: PartitionIdentity,
          partition_id: String,
          partition_manifest_digest: String,
      },
      CapturePartition {
          batch_id: String,
          identity: PartitionIdentity,
          partition_id: String,
          batch_manifest_digest: String,
          partition_manifest_digest: String,
      },
  }

  pub enum CoverageWitness {
      Complete(CompleteCoverage),
      ObservedNoEvents(CompleteCoverage),
      Unavailable { reason: CoverageUnavailableReason },
  }
  ```

  `CompleteCoverage` must contain a bounded typed continuity artifact (archive manifest, captured REST-page chain, or captured WebSocket heartbeat/sequence range), its canonical source digest, exact half-open UTC range, and predecessor/successor identity. It must validate against the selected member evidence; it cannot be constructed from a Boolean or arbitrary assertion string.

  `ResearchSourcePlanBuilder` receives the configured root only while validating members, then creates a private `ResearchSourcePlanDraft` containing just locators, member digests, coverage witnesses, provenance, requested warmup/evaluation intervals, and `member_set_digest`. `member_set_digest` is computed from canonically sorted original locators/manifests/provenance before run construction. `source_plan_digest` is intentionally unavailable until Task 3 has verified a final availability run, preventing a cycle.

  The draft directory is not a readable/publishable source plan: interruption cleanup may remove it, and no `open` API accepts it. The final `publish_to` / `open_from(configured_parquet_root, final_directory)` APIs belong to Task 3. The latter returns a `VerifiedResearchSourcePlan` only after rejecting incomplete, symlinked, non-regular, oversized, noncanonical, or digest-mismatched plan data and re-resolving every selected member under the supplied configured root.

- [ ] **Step 4: Run focused and full storage tests.**

  Run: `cargo fmt --check && cargo test -p trench-storage --test research_plan && cargo test -p trench-storage`

  Expected: PASS; plan persistence is byte-stable and paths cannot appear in its decoded wire format.

- [ ] **Step 5: Commit the source-plan layer.**

  ```bash
  git add crates/trench-storage/src/lib.rs crates/trench-storage/src/research_plan.rs crates/trench-storage/tests/research_plan.rs
  git commit -m "feat(research): add immutable source plans"
  ```

### Task 3: Bounded availability runs and multi-pass external merge

**Files:**
- Create: `crates/trench-storage/src/research_runs.rs`
- Modify: `crates/trench-storage/src/research_plan.rs`
- Modify: `crates/trench-storage/src/lib.rs`
- Test: `crates/trench-storage/tests/research_runs.rs`

- [ ] **Step 1: Write failing ordering and scale tests.**

  Build 65 final legacy partitions across UTC days, each with 1,539 trade rows, retaining the returned manifests as they are written. This creates 100,035 committed rows while never asking the existing discovery/replay path to reopen more than its bounded limit. Build a source plan from those exact locators.

  Assert that (a) a delayed receipt reorders before a later receipt even when its event time is earlier, (b) equal receipt/event times use `event_id`, (c) the 65-member plan performs more than one merge pass, (d) its final-run digest equals an in-test reference sort of the same canonical event records, and (e) opening the plan does not call `DeterministicReplay` or change either replay constant.

  ```rust
  assert_eq!(run.records().count(), 100_035);
  assert_eq!(run.digest(), reference_availability_digest(&facts)?);
  assert!(run.merge_passes() >= 2);
  ```

- [ ] **Step 2: Run the scale test and confirm it fails before implementation.**

  Run: `cargo test -p trench-storage --test research_runs multi_pass_merge_is_equivalent_to_reference -- --exact`

  Expected: FAIL because no availability-run builder/cursor exists.

- [ ] **Step 3: Implement initial runs, 64-way merges, and final-run verification.**

  Add a fixed `MAX_RUN_MERGE_INPUTS: usize = 64` and explicit maximums for run records, wire bytes, run count, and plan metadata. For every verified source member, read only that member, sort it by:

  ```rust
  (event.received_at(), event.event_time(), event.event_id())
  ```

  and write an immutable initial run record containing the normalized event, original member ordinal, event ID, member manifest digest, and the precomputed `member_set_digest`.

  Repeatedly merge no more than 64 validated sorted runs into a new staged run until exactly one remains. The merge comparator must compare the availability key first, then member ordinal only as an internal stability check after the canonical key is proven unique. Do not collect all members or all events into a global `Vec`/map.

  Every run stores a canonical record count, byte count, min/max availability key, input-run digests, output digest, and `member_set_digest`; validation checks record monotonicity and exact digest before a run becomes an input. After the final run is fsynced/reopened, compute `source_plan_digest` from the plan metadata, all original member digests, and the final-run digest. Only then write the final source-plan manifest beside that final run in the same staged directory, validate the whole directory, fsync it, and perform its single publish rename. `ResearchSourcePlan::open_from(configured_parquet_root, final_directory)` must reopen this run and re-resolve every original source member before returning `VerifiedResearchSourcePlan`; a draft is never promoted by mutating a visible final plan.

- [ ] **Step 4: Run focused large-fixture tests, then the full gate.**

  Run: `cargo fmt --check && cargo test -p trench-storage --test research_runs && cargo test -p trench-storage && cargo test -p trench-core --doc`

  Expected: PASS. The >100,000-event test must complete using run files; neither bounded replay limit changes.

- [ ] **Step 5: Commit the availability-run layer.**

  ```bash
  git add crates/trench-storage/src/lib.rs crates/trench-storage/src/research_plan.rs crates/trench-storage/src/research_runs.rs crates/trench-storage/tests/research_runs.rs
  git commit -m "feat(research): merge availability runs externally"
  ```

### Task 4: Atomic, recomputable research-sidecar storage

**Files:**
- Create: `crates/trench-storage/src/research_sidecar.rs`
- Modify: `crates/trench-storage/src/lib.rs`
- Test: `crates/trench-storage/tests/research_sidecar.rs`

- [ ] **Step 1: Write failing sidecar durability and tamper tests.**

  Test an empty-but-valid excluded-only sidecar, several ordered witness shards, retrying an identical sidecar, a conflicting final directory, an interrupted staged publication, a tampered payload byte, source-plan digest drift, duplicated decision IDs across shards, out-of-order decisions, oversized shard manifests, and a symbolic link in any sidecar component.

- [ ] **Step 2: Run the focused sidecar test to verify failure.**

  Run: `cargo test -p trench-storage --test research_sidecar atomic_publish_reopens_verified_sidecar -- --exact`

  Expected: FAIL because `ResearchSidecarWriter` and `ResearchSidecar::open` do not exist.

- [ ] **Step 3: Implement canonical raw-witness payloads and publication.**

  Persist only recomputable raw witness contracts, never serialized authoritative `FeatureSnapshot`, `UniverseSnapshot`, `UniverseActivation`, `RiskPolicy`, `EngineState`, or a strategy artifact. Define bounded, canonical records for recovery proof, hourly selector input, decision feature input, raw risk input, and normalized excluded gaps. Add one canonical `DecisionWitnessIndex` record per decision ID that binds its exact availability cutoff, ordered input event IDs, source ranges, and recovery/universe/feature/risk witness shard references; no independently persisted record may redirect a decision to a different witness set.

  Implement `ResearchSidecarWriter` as a single-use staged directory writer:

  ```rust
  let staged = private_parent.join(format!(".sidecar-{digest}.tmp"));
  write_and_fsync_all_payload_shards(&staged)?;
  write_and_fsync_manifest(&staged, source_plan_digest, shard_digests)?;
  verify_sidecar_directory(&staged)?;
  fsync_directory(&staged)?;
  rename(staged, final_directory)?;
  fsync_directory(private_parent)?;
  ```

  The manifest commits source-plan/config/code/schema digests; counts and byte limits; shard ranges/digests; recovery/universe/decision indexes; and canonical merged half-open excluded gaps. Define `ResearchSidecar::open_from(sidecar_directory, &VerifiedResearchSourcePlan)`: it must require an exact source-plan digest/provenance match, validate the supplied plan's final-run digest, then reread and validate every shard/index against that manifest before exposing an iterator or lookup. A sidecar-only open API is forbidden, so replacing the plan or its committed member evidence cannot be hidden behind a matching embedded string.

- [ ] **Step 4: Run storage tests and the paper boundary check.**

  Run: `cargo fmt --check && cargo test -p trench-storage --test research_sidecar && cargo test -p trench-storage && ./scripts/check-paper-boundary.sh`

  Expected: PASS; interrupted/tampered sidecars are unreadable and boundary check reports no forbidden surface.

- [ ] **Step 5: Commit sidecar storage.**

  ```bash
  git add crates/trench-storage/src/lib.rs crates/trench-storage/src/research_sidecar.rs crates/trench-storage/tests/research_sidecar.rs
  git commit -m "feat(research): publish verified sidecars atomically"
  ```

### Task 5: Causal witness compiler for recovery, universe, features, and risk

**Files:**
- Create: `crates/trench-storage/src/research_compile.rs`
- Modify: `crates/trench-storage/src/research_sidecar.rs`
- Modify: `crates/trench-storage/src/research.rs`
- Test: `crates/trench-storage/tests/research_compiler.rs`

- [ ] **Step 1: Write failing causal-contract tests.**

  Build a compact complete fixture with trades, L2 snapshots, BBO, metadata/context, funding, completed candles, and a typed reconciled recovery witness. Test each adversarial condition independently:

  - a completed candle or contributing trade whose `received_at > decision_at` creates no decision;
  - a book older than one second at `decision_at` cannot feed risk;
  - a recovery anchor reopens the fence and needs a strictly later event/receipt book before execution;
  - a changed raw selector candidate makes the expected hourly digest fail;
  - a changed raw cost/constraint/funding input makes risk-policy recomputation fail;
  - a source absence without a complete `ObservedNoEvents` witness becomes a merged excluded gap.

  Include an exact-boundary success fixture proving the decision uses `decision_at == completed_candle.close_time()` and does not use the first later execution fact as a feature input.

- [ ] **Step 2: Run the compiler contract tests to verify failure.**

  Run: `cargo test -p trench-storage --test research_compiler late_candle_is_excluded_at_original_boundary -- --exact`

  Expected: FAIL because the compiler has not been implemented.

- [ ] **Step 3: Implement one availability-ordered compiler with raw witnesses.**

  `ResearchEvidenceCompiler::compile` accepts only `VerifiedResearchSourcePlan`, opens only its verified final availability run, validates the source plan and coverage first, and maintains bounded per-market source state. It must apply an event only when the event's receipt time has arrived; a candidate decision requires every contributing event to satisfy both `event_time <= decision_at` and `received_at <= decision_at`.

  Recompute, then persist a raw witness plus expected digest for each contract:

  - **Recovery:** validate the typed reconciled request/status/source/anchor/backfill proof against raw source IDs, enforce quarantine/release ordering, and retain the proof needed to reconstruct the boundary later.
  - **Universe:** derive every `UniverseCandidate` at each completed hour from raw metadata/context/trade/BBO/L2 history, rerun `UniverseSelector::select` and `activate`, then store raw candidate inputs and expected selector/activation digests.
  - **Features:** feed timely raw events and timely completed candles into `CommonFeatureEngine`; at a decision boundary store the required event-ID/range witness and expected snapshot/long-history digests, not the computed objects.
  - **Risk:** derive venue constraints, executable book/depth, impact ladder, current/trailing funding distribution, and static risk limits from raw facts plus the frozen config; build `RiskRequest::new(...).into_policy()` only in memory and store the raw inputs plus its expected commitment digest.

  The compiler must process all market candidates at each hourly boundary, including excluded/non-selected markets, so universe membership is not inferred from the future selected set. A missing/stale/late contract yields a canonical half-open exclusion rather than a substitute value. It may write an excluded-only sidecar, but a malformed proof, duplicate source ID, source-plan drift, or resource breach must fail the job before publication.

- [ ] **Step 4: Run the compiler suite and a deterministic recompilation check.**

  Run: `cargo fmt --check && cargo test -p trench-storage --test research_compiler && cargo test -p trench-storage`

  Expected: PASS. Add and run a test compiling the same plan twice into separate parents and asserting byte-identical manifests, witness shards, and exclusions.

- [ ] **Step 5: Commit the causal compiler.**

  ```bash
  git add crates/trench-storage/src/research.rs crates/trench-storage/src/research_compile.rs crates/trench-storage/src/research_sidecar.rs crates/trench-storage/tests/research_compiler.rs
  git commit -m "feat(research): compile causal market witnesses"
  ```

### Task 6: Streaming production-Engine replay over a verified sidecar

**Files:**
- Create: `crates/trench-storage/src/research_stream.rs`
- Modify: `crates/trench-storage/src/research.rs`
- Modify: `crates/trench-storage/src/lib.rs`
- Test: `crates/trench-storage/tests/research_compiler.rs`

- [ ] **Step 1: Write failing streaming/replay parity tests.**

  Use a small valid fixture to run both the existing bounded `EngineRuleReplay` and the new streaming path with the same frozen test artifact and setup. Assert byte-identical Engine persistence record sequence, prediction/intent/trade/cost stream digests, and explicit fold-end checkpoint.

  Add a large test that consumes the >100,000-event final availability run with a counting iterator. Assert that it never constructs a `ResearchFacts` map or source-wide event collection, preserves feature/recovery state across original member boundaries, and refuses a sidecar whose source-plan/config/code digest differs from the requested fold.

- [ ] **Step 2: Run the parity test and verify it fails.**

  Run: `cargo test -p trench-storage --test research_compiler streaming_replay_matches_bounded_engine_fixture -- --exact`

  Expected: FAIL because `StreamingRuleReplay` is not present.

- [ ] **Step 3: Extract shared transition code and implement the streaming adapter.**

  Refactor only the duplicated Engine/broker transition primitives from `research.rs` into private reusable helpers; keep `EngineRuleReplay`'s public constructor, bounded replay input, and validation behavior intact.

  Implement `StreamingRuleReplay` as a `RuleReplay` adapter constructed from a `VerifiedResearchSourcePlan` and `ResearchSidecar::open_from(..., &verified_plan)`. It owns one `EngineState`, `CommonFeatureEngine`, recovery/book state, sidecar indexes, and final-run cursor for one fold. At each decision ID it resolves exactly one `DecisionWitnessIndex`, recomputes the typed inputs from current raw state, compares the expected digests, and only then creates ephemeral `ResearchFeatureFacts`, `ResearchUniverseFacts`, and `ResearchRiskPolicies` for the existing Engine transition. It emits/persists only the existing `EnginePersistenceKind` record stream and an explicit fold-end checkpoint.

  The adapter must reject a decision absent from the sidecar, a sidecar record not reached by its exact source event, a post-decision execution fact reused as an input, an altered recovery witness, and every incompatible provenance value before an Engine evaluation begins.

- [ ] **Step 4: Run parity, large-stream, and core test gates.**

  Run: `cargo fmt --check && cargo test -p trench-storage --test research_compiler && cargo test -p trench-storage && cargo test -p trench-core --doc`

  Expected: PASS; bounded replay remains bounded and the streaming adapter passes identical small-fixture outcomes without entries escaping offline replay.

- [ ] **Step 5: Commit streaming replay.**

  ```bash
  git add crates/trench-storage/src/lib.rs crates/trench-storage/src/research.rs crates/trench-storage/src/research_stream.rs crates/trench-storage/tests/research_compiler.rs
  git commit -m "feat(research): replay verified sidecars in streams"
  ```

### Task 7: Offline CLI integration with the rules-entry seal intact

**Files:**
- Modify: `crates/trenchd/src/commands.rs`
- Test: `crates/trenchd/src/commands.rs`
- Modify: `AGENTS.md`

- [ ] **Step 1: Write failing command-boundary tests.**

  Add command tests for:

  - `trenchd research source-plan` accepts absolute private source-member and coverage input files, uses the configured Parquet root only at build time, and publishes a final plan;
  - `trenchd research compile-sidecar` accepts a final plan and private output parent, produces/reopens a sidecar, and never creates/open SQLite;
  - a path/symlink/non-private output is rejected;
  - `research compile-sidecar` cannot write `rules-validation.json`, a rules artifact, or change `RulesStartup::resolve` from `ReplayAdapterUnavailable`.

- [ ] **Step 2: Run the targeted command tests to verify failure.**

  Run: `cargo test -p trenchd commands::rules_research_tests::compile_sidecar_keeps_rules_startup_sealed -- --exact`

  Expected: FAIL because the two subcommands do not exist.

- [ ] **Step 3: Add two bounded offline subcommands.**

  Add `ResearchCommand::SourcePlan(ResearchSourcePlanArgs)` and `ResearchCommand::CompileSidecar(ResearchCompileSidecarArgs)`. The source-plan command reads a bounded canonical member/coverage document, exact requested UTC intervals, and a configured private output parent; it constructs/publishes a `ResearchSourcePlan` through its library API. The compile command reads one final plan and publishes one sidecar through `ResearchEvidenceCompiler`.

  The source-plan command completes the entire draft → final-run → one-rename source-plan publication before returning; it never exposes a draft directory. The compile command reopens that result as `VerifiedResearchSourcePlan` against the configured Parquet root before it can open/emit a sidecar. Both command result types return final path/digest/coverage exclusion count only. They must use `load_config` and the configured Parquet root, reject nonabsolute/unsafe paths, log no raw document bodies, and avoid all `SqliteStore`, runtime app, network, artifact, and readiness calls. Do not wire the sidecar into `research rules` yet: that validator/artifact admission is deliberately a later, separately reviewed decision.

  Update `AGENTS.md` to document the offline source-plan → sidecar topology, exact command names, and that active rules entries remain sealed pending a separate live-reactor specification.

- [ ] **Step 4: Run daemon/package tests and boundary checks.**

  Run: `cargo fmt --check && cargo test -p trenchd && cargo test --workspace && ./scripts/check-paper-boundary.sh`

  Expected: PASS; `research rules` stays canonically ineligible and no forbidden surface is introduced.

- [ ] **Step 5: Commit CLI integration and documentation.**

  ```bash
  git add AGENTS.md crates/trenchd/src/commands.rs
  git commit -m "feat(research): add offline sidecar commands"
  ```

### Task 8: Final adversarial audit and release verification

**Files:**
- Modify: `crates/trench-storage/tests/research_plan.rs`
- Modify: `crates/trench-storage/tests/research_runs.rs`
- Modify: `crates/trench-storage/tests/research_sidecar.rs`
- Modify: `crates/trench-storage/tests/research_compiler.rs`
- Modify: `crates/trenchd/src/commands.rs`

- [ ] **Step 1: Add an end-to-end adversarial fixture.**

  Compose one plan larger than 100,000 rows / 64 members containing all failure classes: a late trade, a late completed candle, an exact-boundary but stale book, a changed source manifest, an invalid continuity predecessor, an interrupted sidecar, an altered raw risk input, recovery re-fencing, and a first valid post-decision execution event. Assert the affected decisions become exclusions or fail before replay—never predictions based on substituted/current data.

- [ ] **Step 2: Run the end-to-end test to verify the missing assertion or implementation gap.**

  Run: `cargo test -p trench-storage --test research_compiler end_to_end_adversarial_source_is_fail_closed -- --exact`

  Expected: PASS only after every prior task is integrated; otherwise fix the smallest failing layer before proceeding.

- [ ] **Step 3: Verify no sidecar can be mistaken for activation authority.**

  Add explicit assertions that a successful compile leaves `RulesStartup` unready, does not create a `RulesArtifact`, cannot change daemon status/readiness, does not submit an Engine entry outside the supplied offline replay request, and makes no SQLite writes. Verify source-plan and sidecar digests are required on reopen.

- [ ] **Step 4: Run the complete project release gate.**

  Run:

  ```bash
  cargo fmt --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace
  cargo test -p trench-core --doc
  cargo build -p trench-core --target x86_64-pc-windows-gnu --release
  ./scripts/check-paper-boundary.sh
  git diff --check
  ```

  Expected: every command exits zero. Do not replace failures with ignored tests, broaden replay limits, or add fallbacks.

- [ ] **Step 5: Commit the audit suite, integrate, and publish `main`.**

  ```bash
  git add crates/trench-storage/tests/research_plan.rs crates/trench-storage/tests/research_runs.rs crates/trench-storage/tests/research_sidecar.rs crates/trench-storage/tests/research_compiler.rs crates/trenchd/src/commands.rs
  git commit -m "test(research): harden evidence compiler boundaries"
  # Parent integration worker: cherry-pick onto main, repeat the release gate,
  # then remove the task worktree.
  git push origin main
  ```

## Acceptance checklist

- [ ] A >100,000-event, >64-member source plan compiles through multi-pass run files without modifying `DeterministicReplay` limits.
- [ ] Recompilation is byte-identical and uses no global event/facts materialization.
- [ ] Plan/sidecar readers reject drift, duplicates, symlinks, partial directories, noncanonical ordering, forged/missing coverage, late inputs, reopened recovery, stale books, and altered universe/risk witnesses.
- [ ] Every decision is tied to a completed-candle `decision_at`; every input is timely at that boundary; the first later executable event is a separate fact.
- [ ] `StreamingRuleReplay` produces production Engine persistence/outcomes from recomputed witnesses and agrees with the bounded fixture adapter.
- [ ] The CLI stays offline and paper-only: no rule artifact, readiness mutation, SQLite write, or entry activation is possible.
