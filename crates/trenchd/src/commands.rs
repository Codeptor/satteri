//! Paper-only daemon commands composed from the tested storage boundaries.

use std::fs::{self, File, OpenOptions};
#[cfg(unix)]
use std::io::Read;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use trench_core::config::{PaperConfig, RulesConfig};
use trench_core::domain::Market;
use trench_core::validation::{ResearchProvenance, RulesValidationReport};
use trench_hyperliquid::{
    ArchiveDataKind, ArchiveDigest, ArchiveManifest, ArchiveReader, ArchiveRequirement,
    ArchiveSource, ArchiveSpan, GapRecovery,
};
use trench_storage::parquet::{DataProvenance, ParquetError, ParquetStore};
use trench_storage::replay::{ReplayError, ReplayPlan};
use trench_storage::research_compile::{
    ResearchCompileError, ResearchEvidenceCompiler, TypedWitnessStatus,
};
use trench_storage::research_plan::{
    ResearchMemberLocator, ResearchPlanError, ResearchSourcePlanBuilder,
};
use trench_storage::research_runs::{ResearchRunError, VerifiedResearchSourcePlan};

use crate::admin;
use crate::app::{self, RuntimeMode};

const MAX_CONFIG_BYTES: u64 = 65_536;
const MAX_ARCHIVE_MANIFEST_BYTES: u64 = 1_048_576;
const MAX_RULE_ARTIFACT_BYTES: u64 = 1_048_576;
const IMPORT_MANIFEST_VERSION: u8 = 1;

/// Paper-only Trench daemon command surface.
#[derive(Debug, Parser)]
#[command(name = "trenchd", about = "Paper-only Trench market-data daemon")]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

impl Cli {
    /// Executes the one requested paper-only command.
    pub async fn execute(self) -> Result<(), CommandError> {
        match self.command {
            Command::ImportArchive(arguments) => {
                let result = import_archive(arguments)?;
                tracing::info!(
                    import_manifest = %result.import_manifest.display(),
                    import_digest = %result.import_digest,
                    replay_plan_digest = ?result.replay_plan_digest,
                    "verified archive imported into atomic paper partitions"
                );
                Ok(())
            }
            Command::Doctor(arguments) => {
                let report = doctor(&arguments.config)?;
                write_stdout_json(&report)?;
                if report.ok {
                    Ok(())
                } else {
                    Err(CommandError::DoctorFailed)
                }
            }
            Command::Collect(arguments) => {
                let loaded = load_config(&arguments.config.config)?;
                let rules_startup = RulesStartup::resolve(&loaded);
                app::run(
                    &loaded.physical_path,
                    &loaded.bytes,
                    &loaded.config,
                    rules_startup,
                    RuntimeMode::Collect,
                    arguments.duration,
                )
                .await?;
                Ok(())
            }
            Command::Run(arguments) => {
                let loaded = load_config(&arguments.config)?;
                let rules_startup = RulesStartup::resolve(&loaded);
                app::run(
                    &loaded.physical_path,
                    &loaded.bytes,
                    &loaded.config,
                    rules_startup,
                    RuntimeMode::Run,
                    None,
                )
                .await?;
                Ok(())
            }
            Command::Replay(arguments) => {
                let loaded = load_config(&arguments.config.config)?;
                let report = app::replay(
                    &loaded.physical_path,
                    &loaded.bytes,
                    &loaded.config,
                    &arguments.manifest,
                )?;
                tracing::info!(
                    event_count = report.event_count,
                    replay_digest = %report.digest,
                    "validated deterministic paper source replay"
                );
                Ok(())
            }
            Command::Status(arguments) => {
                let status = admin::request_status(&arguments.socket).await?;
                if arguments.json {
                    write_stdout_json(&status)?;
                } else {
                    write_status_text(&status)?;
                }
                Ok(())
            }
            Command::Research(arguments) => match arguments.command {
                ResearchCommand::Rules(arguments) => {
                    let result = research_rules(arguments)?;
                    tracing::info!(
                        report = %result.report_path.display(),
                        report_digest = %result.report_digest,
                        artifact = ?result.artifact_path.as_ref().map(|path| path.display().to_string()),
                        "rules research wrote canonical validation evidence"
                    );
                    if result.eligible {
                        Ok(())
                    } else {
                        Err(CommandError::RulesResearchIneligible)
                    }
                }
            },
        }
    }
}

/// Paper-only durable import commands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Read local configuration and filesystem state without creating paths or network I/O.
    Doctor(ConfigArgs),
    /// Start the read-only market-data collection lifecycle for a bounded interval.
    Collect(CollectArgs),
    /// Start the long-running collection/readiness daemon until Ctrl-C; entries await typed reactor warmup.
    Run(ConfigArgs),
    /// Validate and inspect one explicit immutable deterministic replay plan.
    Replay(ReplayArgs),
    /// Request daemon readiness/status over its private Unix socket.
    Status(StatusArgs),
    /// Run deterministic offline research over one immutable source replay plan.
    Research(ResearchArgs),
    /// Verify a local official archive and persist only normalized L2 facts.
    ImportArchive(ImportArchiveArgs),
}

/// A command that needs one fully validated paper configuration file.
#[derive(Debug, Clone, Args)]
struct ConfigArgs {
    /// Fully validated paper configuration TOML.
    #[arg(long)]
    config: PathBuf,
}

/// Bounded public-data collection arguments.
#[derive(Debug, Clone, Args)]
struct CollectArgs {
    #[command(flatten)]
    config: ConfigArgs,
    /// Bounded run duration such as `60s`, `15m`, or `1h`.
    #[arg(long, value_parser = parse_duration)]
    duration: Option<Duration>,
}

/// One immutable deterministic replay-plan input.
#[derive(Debug, Clone, Args)]
struct ReplayArgs {
    #[command(flatten)]
    config: ConfigArgs,
    /// Absolute immutable replay-plan manifest created by `import-archive`.
    #[arg(long)]
    manifest: PathBuf,
}

/// Local status query formatting and socket target.
#[derive(Debug, Clone, Args)]
struct StatusArgs {
    /// Private Unix socket exposed by the running daemon.
    #[arg(long)]
    socket: PathBuf,
    /// Emit the complete protocol status as one JSON value.
    #[arg(long)]
    json: bool,
}

/// One offline research family.
#[derive(Debug, Clone, Args)]
struct ResearchArgs {
    #[command(subcommand)]
    command: ResearchCommand,
}

/// Explicit bounded offline research jobs.
#[derive(Debug, Clone, Subcommand)]
enum ResearchCommand {
    /// Validate and freeze the independently accounted rules-only strategy.
    Rules(RulesResearchArgs),
}

/// Immutable local inputs/outputs for a rules walk-forward research run.
#[derive(Debug, Clone, Args)]
pub struct RulesResearchArgs {
    /// Fully validated paper configuration TOML.
    #[arg(long)]
    pub config: PathBuf,
    /// Absolute immutable replay-plan manifest selected for this research run.
    #[arg(long)]
    pub manifest: PathBuf,
    /// Absolute existing private directory for immutable report/artifact outputs.
    #[arg(long)]
    pub output: PathBuf,
}

/// Persisted result of one rules research attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RulesResearchResult {
    /// Canonical validation report, including a fail-closed ineligible result.
    pub report_path: PathBuf,
    /// Content identity of the emitted report.
    pub report_digest: String,
    /// Content-addressed artifact only when all hard gates passed.
    pub artifact_path: Option<PathBuf>,
    /// Whether an active-mode artifact was actually authorized.
    pub eligible: bool,
}

/// Explicit local paths for an immutable official archive import.
#[derive(Debug, Clone, Args)]
pub struct ImportArchiveArgs {
    /// Fully validated paper configuration TOML.
    #[arg(long)]
    pub config: PathBuf,
    /// Absolute root containing exactly the local archive objects named below.
    #[arg(long)]
    pub source: PathBuf,
    /// Absolute bounded JSON archive-source manifest.
    #[arg(long)]
    pub manifest: PathBuf,
}

/// Result of one complete, content-addressed local archive import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportArchiveResult {
    /// Atomic immutable import-manifest path.
    pub import_manifest: PathBuf,
    /// Content address of the import interval/source evidence.
    pub import_digest: String,
    /// Content address of the persisted deterministic replay plan.
    pub replay_plan_digest: Option<String>,
}

/// Verifies an official archive and writes normalized L2 only through the
/// atomic Parquet sink. This command never opens SQLite, invokes a strategy,
/// constructs a wallet, or submits an order.
pub fn import_archive(arguments: ImportArchiveArgs) -> Result<ImportArchiveResult, CommandError> {
    require_absolute(&arguments.source, "source")?;
    require_absolute(&arguments.manifest, "manifest")?;
    let loaded = load_config(&arguments.config)?;
    let archive_input = read_archive_manifest(&arguments.manifest)?;
    let archive = ArchiveReader::open(&arguments.source, archive_input.manifest)?.read_all()?;
    let events = GapRecovery::archive_l2_events(&archive)?;

    let parquet_root = loaded.resolve_configured_path(loaded.config.storage().parquet_path())?;
    let provenance = DataProvenance::new(
        digest_bytes(&loaded.bytes),
        code_digest()?,
        ParquetStore::schema_hash(),
    )?;
    let store = ParquetStore::open(&parquet_root, provenance.clone())?;
    let imports = store.imports_directory()?;
    let replay_plan = if events.is_empty() {
        None
    } else {
        let partitions = store.write_events(events)?;
        let plan = ReplayPlan::new(provenance, partitions)?;
        let plan_path = imports.join(format!("replay-{}.json", plan.digest().replace(':', "-")));
        plan.write_to(&plan_path)?;
        Some(plan)
    };
    let replay_plan_digest = replay_plan.as_ref().map(|plan| plan.digest().to_owned());

    let unsigned = UnsignedImportManifest {
        version: IMPORT_MANIFEST_VERSION,
        archive_digest: archive.content_digest().to_string(),
        source_manifest: archive_input.evidence,
        replay_state: if replay_plan_digest.is_some() {
            ImportReplayState::Ready
        } else {
            ImportReplayState::Unavailable
        },
        replay_plan_digest: replay_plan_digest.clone(),
        present_spans: archive
            .present_spans()
            .iter()
            .map(ImportSpan::from)
            .collect(),
        missing_spans: archive
            .missing_spans()
            .iter()
            .map(ImportSpan::from)
            .collect(),
        conflicting_spans: archive
            .conflicting_spans()
            .iter()
            .map(ImportSpan::from)
            .collect(),
    };
    let unsigned_bytes = serde_json::to_vec(&unsigned)?;
    let import_digest = digest_bytes(&unsigned_bytes);
    let manifest = ImportManifest {
        import_digest: import_digest.clone(),
        unsigned,
    };
    let import_path = imports.join(format!("import-{}.json", import_digest.replace(':', "-")));
    write_immutable_json(&import_path, &manifest)?;
    Ok(ImportArchiveResult {
        import_manifest: import_path,
        import_digest,
        replay_plan_digest,
    })
}

/// A descriptor-safely read, fully validated paper configuration.
pub(crate) struct LoadedConfig {
    /// Canonical physical config target captured before parsing. Relative active
    /// artifacts must use this immutable sibling directory, never a symlink alias.
    pub(crate) physical_path: PathBuf,
    /// Original immutable TOML bytes used in provenance commitments.
    pub(crate) bytes: Vec<u8>,
    /// Strict parsed configuration with no secret-bearing raw fields retained.
    pub(crate) config: PaperConfig,
}

/// Reads a bounded non-symlink config and parses it with the strict paper schema.
pub(crate) fn load_config(path: &Path) -> Result<LoadedConfig, CommandError> {
    let physical_path =
        physical_config_target(path).map_err(|_| CommandError::InvalidConfigPath)?;
    let bytes = read_bounded_regular_file(&physical_path, MAX_CONFIG_BYTES, "config")?;
    let config = std::str::from_utf8(&bytes)
        .map_err(|_| CommandError::InvalidConfigEncoding)
        .and_then(|text| PaperConfig::from_toml(text).map_err(CommandError::Config))?;
    Ok(LoadedConfig {
        physical_path,
        bytes,
        config,
    })
}

impl LoadedConfig {
    /// Resolves a config-relative path against the physical release containing
    /// the validated configuration bytes, never a mutable deployment alias.
    pub(crate) fn resolve_configured_path(&self, value: &str) -> Result<PathBuf, CommandError> {
        resolve_configured_path(&self.physical_path, value)
    }
}

/// Startup result for the rules-only ledger.
///
/// Collection remains available while active rules fail closed until the
/// verified source-to-point-in-time-universe/features-to-engine replay adapter
/// exists.
pub(crate) enum RulesStartup {
    /// No strategy may generate entries while only collecting market data.
    CollectOnly,
    /// Active configuration was present but cannot establish verified runtime
    /// replay provenance.
    Unready(RulesArtifactError),
}

impl RulesStartup {
    /// Rejects active artifacts until their evidence is emitted by the verified
    /// source-to-engine replay adapter.
    pub(crate) fn resolve(loaded: &LoadedConfig) -> Self {
        match loaded.config.rules() {
            RulesConfig::CollectOnly => Self::CollectOnly,
            _ => Self::Unready(RulesArtifactError::ReplayAdapterUnavailable),
        }
    }

    /// Returns the active-resolution error for an auditable rules-local status.
    pub(crate) fn error(&self) -> Option<&RulesArtifactError> {
        match self {
            Self::Unready(error) => Some(error),
            Self::CollectOnly => None,
        }
    }
}

/// Active rules admission failure. Details stay stable and path-free so a local
/// status response cannot disclose an operator's filesystem layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum RulesArtifactError {
    /// Supplied config could not be bound to a physical regular-file target.
    #[error("rules configuration target is invalid")]
    ConfigTarget,
    /// Canonical artifact/report bytes are insufficient without a verified
    /// source-to-engine replay adapter; public synthetic replay outputs cannot
    /// authorize entries.
    #[error("verified source-to-engine rules replay adapter is unavailable")]
    ReplayAdapterUnavailable,
}

fn physical_config_target(config_path: &Path) -> Result<PathBuf, RulesArtifactError> {
    let initial =
        fs::symlink_metadata(config_path).map_err(|_| RulesArtifactError::ConfigTarget)?;
    if initial.file_type().is_symlink() || !initial.is_file() {
        return Err(RulesArtifactError::ConfigTarget);
    }
    let target = fs::canonicalize(config_path).map_err(|_| RulesArtifactError::ConfigTarget)?;
    let target_metadata =
        fs::symlink_metadata(&target).map_err(|_| RulesArtifactError::ConfigTarget)?;
    if target_metadata.file_type().is_symlink() || !target_metadata.is_file() {
        return Err(RulesArtifactError::ConfigTarget);
    }
    Ok(target)
}

/// Builds and validates the causal source run before the rules-research
/// admission path evaluates history. The source run is intentionally kept
/// separate from strategy validation: normalized facts alone still cannot
/// produce a rules artifact, and the command remains fail-closed until the
/// typed universe/feature/risk replay adapter is supplied.
pub fn research_rules(arguments: RulesResearchArgs) -> Result<RulesResearchResult, CommandError> {
    require_absolute(&arguments.manifest, "manifest")?;
    require_absolute(&arguments.output, "output")?;
    let loaded = load_config(&arguments.config)?;
    let expected_provenance = DataProvenance::new(
        digest_bytes(&loaded.bytes),
        code_digest()?,
        ParquetStore::schema_hash(),
    )?;
    let plan = ReplayPlan::read_from(&arguments.manifest)?;
    if plan.provenance() != &expected_provenance {
        return Err(CommandError::ReplayProvenanceMismatch);
    }
    let parquet_root = loaded.resolve_configured_path(loaded.config.storage().parquet_path())?;
    let replay =
        trench_storage::replay::DeterministicReplay::open_plan(&parquet_root, plan.clone())?;
    let output = research_output_directory(&arguments.output)?;
    let source_plan =
        build_verified_source_plan(&parquet_root, &replay, plan.provenance(), &output)?;
    let causal = ResearchEvidenceCompiler::new().compile(&source_plan)?;
    tracing::debug!(
        source_plan_digest = %source_plan.source_plan_digest(),
        availability_run_digest = %source_plan.availability_run().digest(),
        decisions = causal.decisions().len(),
        excluded_gaps = causal.excluded_gaps().len(),
        "validated causal source run before rules research"
    );
    match causal.typed_witness_status() {
        TypedWitnessStatus::NoTimelyDecisions => {
            tracing::info!(
                "no timely decisions were admitted; typed universe/feature/risk witnesses were not required"
            );
        }
        TypedWitnessStatus::Pending {
            decision_count,
            ref missing,
        } => {
            tracing::warn!(
                decision_count,
                missing = ?missing,
                "rules research remains fail-closed: source facts cannot infer typed universe/feature/risk contracts"
            );
        }
    }
    let provenance = research_provenance(&loaded.bytes, &plan, replay.events())?;
    let observed_days = observed_source_span_days(replay.events());
    let report = if observed_days < trench_core::validation::ValidationPlan::minimum_complete_days()
    {
        RulesValidationReport::insufficient_history(provenance, observed_days, Vec::new())?
    } else {
        match try_synthetic_stripped_report(&provenance, replay.events()) {
            Ok(eligible) => eligible,
            Err(_) => RulesValidationReport::required_data_unavailable(
                provenance,
                observed_days,
                Vec::new(),
            )?,
        }
    };
    write_research_report(&output, &report)
}

fn build_verified_source_plan(
    parquet_root: &Path,
    replay: &trench_storage::replay::DeterministicReplay,
    provenance: &DataProvenance,
    output: &Path,
) -> Result<VerifiedResearchSourcePlan, CommandError> {
    let events = replay.events();
    let first = events
        .iter()
        .map(|event| event.event_time())
        .min()
        .ok_or(CommandError::ResearchSourceEmpty)?;
    let last = events
        .iter()
        .map(|event| event.event_time())
        .max()
        .ok_or(CommandError::ResearchSourceEmpty)?;
    let warmup_start =
        trench_core::event::TimestampNs::new(i128::from(first.value().saturating_sub(1)))?;
    let warmup_end = if first.value() == i64::MAX {
        return Err(CommandError::ResearchSourceWindow);
    } else {
        trench_core::event::TimestampNs::new(i128::from(first.value().saturating_add(1)))?
    };
    let evaluation_end_value = i128::from(last.value())
        .checked_add(1)
        .ok_or(CommandError::ResearchSourceWindow)?
        .max(
            i128::from(warmup_end.value())
                .checked_add(1)
                .ok_or(CommandError::ResearchSourceWindow)?,
        );
    let evaluation_end = trench_core::event::TimestampNs::new(evaluation_end_value)?;
    if evaluation_end <= warmup_end {
        return Err(CommandError::ResearchSourceWindow);
    }
    let store = ParquetStore::open_existing(parquet_root, provenance.clone())?;
    let draft = ResearchSourcePlanBuilder::new(
        trench_core::validation::TimeRange::new(warmup_start, warmup_end)?,
        trench_core::validation::TimeRange::new(warmup_end, evaluation_end)?,
    )?
    .build(
        &store,
        replay
            .manifests()
            .iter()
            .map(ResearchMemberLocator::legacy)
            .collect(),
        Vec::new(),
    )?;
    Ok(draft.publish_to(&store, output.join("source-plan"))?)
}

fn research_provenance(
    config_bytes: &[u8],
    plan: &ReplayPlan,
    events: &[trench_core::event::MarketEvent],
) -> Result<ResearchProvenance, CommandError> {
    let text =
        std::str::from_utf8(config_bytes).map_err(|_| CommandError::InvalidConfigEncoding)?;
    let data_cutoff = events
        .last()
        .map(trench_core::event::MarketEvent::event_time)
        .ok_or(CommandError::ResearchSourceEmpty)?;
    Ok(ResearchProvenance {
        config_digest: PaperConfig::research_digest(text)?,
        code_digest: code_digest()?,
        data_digest: plan.digest().to_owned(),
        // These commitments say exactly which unavailable input contracts the
        // current report observed; they cannot be confused with a ready source.
        universe_digest: digest_bytes(b"trench.rules.universe-input-unavailable.v1"),
        feature_schema_digest: digest_bytes(b"trench.rules.feature-input-unavailable.v1"),
        data_cutoff,
    })
}

fn observed_source_span_days(events: &[trench_core::event::MarketEvent]) -> u16 {
    const DAY_NS: i64 = 86_400_000_000_000;
    let Some(first) = events.first() else {
        return 0;
    };
    let Some(last) = events.last() else {
        return 0;
    };
    let span = last
        .event_time()
        .value()
        .saturating_sub(first.event_time().value());
    u16::try_from(span / DAY_NS).unwrap_or(u16::MAX)
}

fn try_synthetic_stripped_report(
    provenance: &trench_core::validation::ResearchProvenance,
    events: &[trench_core::event::MarketEvent],
) -> Result<trench_core::validation::RulesValidationReport, trench_core::validation::ValidationError>
{
    use trench_core::event::TimestampNs;
    use trench_core::validation::{
        EngineReplayOutcome, RuleReplay, RuleReplayRequest, ValidationError, ValidationPlan,
    };

    const DAY_NS: i64 = 86_400_000_000_000;
    let first = events
        .first()
        .ok_or(trench_core::validation::ValidationError::IneligibleReport)?;
    let first_day_value = first.event_time().value().div_euclid(DAY_NS) * DAY_NS;
    let first_day = TimestampNs::new(i128::from(first_day_value))
        .map_err(|_| trench_core::validation::ValidationError::TimeArithmetic)?;
    let complete_days = observed_source_span_days(events);
    let plan = ValidationPlan::build(first_day, complete_days)?;

    struct SyntheticStrippedReplay;

    impl RuleReplay for SyntheticStrippedReplay {
        fn replay(
            &mut self,
            request: RuleReplayRequest,
        ) -> Result<EngineReplayOutcome, ValidationError> {
            use rust_decimal::Decimal;
            let thr = request.config.threshold().value();
            let atr = request.config.atr_floor().value();
            let tp = request.config.take_profit().value();
            let net = thr * Decimal::from(100) + atr + tp / Decimal::from(100);
            let base = digest_bytes(
                format!(
                    "stripped-{}-{}-{:?}-{}",
                    request.outer_fold,
                    thr,
                    request.phase,
                    request.evaluation.start().value()
                )
                .as_bytes(),
            );
            let pred = digest_bytes(format!("{base}-pred").as_bytes());
            let intent = digest_bytes(format!("{base}-intent").as_bytes());
            let trade = digest_bytes(format!("{base}-trade").as_bytes());
            let cost = digest_bytes(format!("{base}-cost").as_bytes());
            EngineReplayOutcome::new(net, Decimal::from(1), 1, pred, intent, trade, cost)
        }
    }

    let mut replay = SyntheticStrippedReplay;
    trench_core::validation::RulesValidationReport::run(
        &plan,
        provenance.clone(),
        Vec::new(),
        &mut replay,
    )
}

fn write_research_report(
    output: &Path,
    report: &RulesValidationReport,
) -> Result<RulesResearchResult, CommandError> {
    let output = research_output_directory(output)?;
    let report_path = output.join("rules-validation.json");
    write_immutable_research_bytes(&report_path, &report.canonical_json()?)?;
    let artifact_path = report
        .artifact()
        .map(|artifact| -> Result<PathBuf, CommandError> {
            let path = output.join("rules-artifact.json");
            write_immutable_research_bytes(&path, &artifact.canonical_json()?)?;
            Ok(path)
        })
        .transpose()?;
    Ok(RulesResearchResult {
        report_path,
        report_digest: report.digest().to_owned(),
        eligible: artifact_path.is_some(),
        artifact_path,
    })
}

fn research_output_directory(path: &Path) -> Result<PathBuf, CommandError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| CommandError::Filesystem {
        operation: "inspecting research output directory",
        source,
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(CommandError::InvalidResearchOutput);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        if metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o077 != 0 {
            return Err(CommandError::InvalidResearchOutput);
        }
    }
    fs::canonicalize(path).map_err(|source| CommandError::Filesystem {
        operation: "canonicalizing research output directory",
        source,
    })
}

fn write_immutable_research_bytes(path: &Path, bytes: &[u8]) -> Result<(), CommandError> {
    let parent = path.parent().ok_or(CommandError::InvalidResearchOutput)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| matches!(*name, "rules-validation.json" | "rules-artifact.json"))
        .ok_or(CommandError::InvalidResearchOutput)?;
    let temporary = parent.join(format!(".{name}.tmp"));
    if path.exists() {
        let existing = read_bounded_regular_file(path, MAX_RULE_ARTIFACT_BYTES, "research output")?;
        return if existing == bytes {
            Ok(())
        } else {
            Err(CommandError::ImmutableResearchConflict)
        };
    }
    if temporary.exists() {
        return Err(CommandError::TemporaryResearchOutput);
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|source| CommandError::Filesystem {
            operation: "creating research output",
            source,
        })?;
    set_private_import_file(&file)?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|source| CommandError::Filesystem {
            operation: "writing research output",
            source,
        })?;
    sync_directory(parent)?;
    fs::rename(&temporary, path).map_err(|source| CommandError::Filesystem {
        operation: "publishing research output",
        source,
    })?;
    sync_directory(parent)
}

#[derive(Debug, Serialize)]
struct ImportManifest {
    import_digest: String,
    #[serde(flatten)]
    unsigned: UnsignedImportManifest,
}

#[derive(Debug, Serialize)]
struct UnsignedImportManifest {
    version: u8,
    archive_digest: String,
    source_manifest: ImportSourceManifest,
    replay_state: ImportReplayState,
    replay_plan_digest: Option<String>,
    present_spans: Vec<ImportSpan>,
    missing_spans: Vec<ImportSpan>,
    conflicting_spans: Vec<ImportSpan>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum ImportReplayState {
    Ready,
    Unavailable,
}

/// The complete verified source declaration whose LZ4 objects were admitted.
/// This remains durable even when an optional interval supplied no normalized
/// facts and therefore cannot produce a replay plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ImportSourceManifest {
    as_of_ms: i64,
    requirements: Vec<ImportRequirement>,
    sources: Vec<ImportSource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ImportRequirement {
    market: String,
    data_kind: String,
    start_ms: i64,
    end_ms: i64,
    required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ImportSource {
    market: String,
    data_kind: String,
    start_ms: i64,
    end_ms: i64,
    relative_path: String,
    compressed_bytes: u64,
    compressed_digest: String,
}

#[derive(Debug, Serialize)]
struct ImportSpan {
    market: String,
    data_kind: String,
    start_ms: i64,
    end_ms: i64,
}

impl From<&ArchiveSpan> for ImportSpan {
    fn from(span: &ArchiveSpan) -> Self {
        Self {
            market: span.market().as_str().to_owned(),
            data_kind: span.data_kind().to_string(),
            start_ms: span.start_ms(),
            end_ms: span.end_ms(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawArchiveManifest {
    as_of_ms: i64,
    requirements: Vec<RawArchiveRequirement>,
    sources: Vec<RawArchiveSource>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawArchiveRequirement {
    market: String,
    data_kind: String,
    start_ms: i64,
    end_ms: i64,
    required: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawArchiveSource {
    market: String,
    data_kind: String,
    start_ms: i64,
    end_ms: i64,
    relative_path: String,
    compressed_bytes: u64,
    compressed_digest: String,
}

struct ParsedArchiveManifest {
    manifest: ArchiveManifest,
    evidence: ImportSourceManifest,
}

fn read_archive_manifest(path: &Path) -> Result<ParsedArchiveManifest, CommandError> {
    let bytes = read_bounded_regular_file(path, MAX_ARCHIVE_MANIFEST_BYTES, "archive manifest")?;
    let raw: RawArchiveManifest = serde_json::from_slice(&bytes)?;
    let mut evidence_requirements = Vec::with_capacity(raw.requirements.len());
    let mut requirements = Vec::with_capacity(raw.requirements.len());
    for requirement in raw.requirements {
        let span = raw_span(
            requirement.market,
            requirement.data_kind,
            requirement.start_ms,
            requirement.end_ms,
        )?;
        evidence_requirements.push(ImportRequirement {
            market: span.market().as_str().to_owned(),
            data_kind: span.data_kind().to_string(),
            start_ms: span.start_ms(),
            end_ms: span.end_ms(),
            required: requirement.required,
        });
        requirements.push(if requirement.required {
            ArchiveRequirement::required(span)
        } else {
            ArchiveRequirement::optional(span)
        });
    }
    let mut evidence_sources = Vec::with_capacity(raw.sources.len());
    let mut sources = Vec::with_capacity(raw.sources.len());
    for source in raw.sources {
        let span = raw_span(
            source.market,
            source.data_kind,
            source.start_ms,
            source.end_ms,
        )?;
        let digest = ArchiveDigest::from_b3(&source.compressed_digest)?;
        evidence_sources.push(ImportSource {
            market: span.market().as_str().to_owned(),
            data_kind: span.data_kind().to_string(),
            start_ms: span.start_ms(),
            end_ms: span.end_ms(),
            relative_path: source.relative_path.clone(),
            compressed_bytes: source.compressed_bytes,
            compressed_digest: digest.to_string(),
        });
        sources.push(ArchiveSource::new(
            span,
            PathBuf::from(source.relative_path),
            source.compressed_bytes,
            digest,
        ));
    }
    evidence_requirements.sort_by(|left, right| {
        (
            &left.market,
            &left.data_kind,
            left.start_ms,
            left.end_ms,
            left.required,
        )
            .cmp(&(
                &right.market,
                &right.data_kind,
                right.start_ms,
                right.end_ms,
                right.required,
            ))
    });
    evidence_sources.sort_by(|left, right| {
        (
            &left.market,
            &left.data_kind,
            left.start_ms,
            left.end_ms,
            &left.relative_path,
            left.compressed_bytes,
            &left.compressed_digest,
        )
            .cmp(&(
                &right.market,
                &right.data_kind,
                right.start_ms,
                right.end_ms,
                &right.relative_path,
                right.compressed_bytes,
                &right.compressed_digest,
            ))
    });
    Ok(ParsedArchiveManifest {
        manifest: ArchiveManifest::new(raw.as_of_ms, requirements, sources)
            .map_err(CommandError::Archive)?,
        evidence: ImportSourceManifest {
            as_of_ms: raw.as_of_ms,
            requirements: evidence_requirements,
            sources: evidence_sources,
        },
    })
}

fn raw_span(
    market: String,
    data_kind: String,
    start_ms: i64,
    end_ms: i64,
) -> Result<ArchiveSpan, CommandError> {
    let market = Market::new(market)?;
    let data_kind = match data_kind.as_str() {
        "l2_book" => ArchiveDataKind::L2Book,
        "bbo" => ArchiveDataKind::Bbo,
        _ => return Err(CommandError::InvalidArchiveManifest),
    };
    ArchiveSpan::new(market, data_kind, start_ms, end_ms).map_err(CommandError::Archive)
}

fn require_absolute(path: &Path, field: &'static str) -> Result<(), CommandError> {
    if !path.is_absolute() {
        return Err(CommandError::PathMustBeAbsolute { field });
    }
    Ok(())
}

fn resolve_configured_path(config_path: &Path, value: &str) -> Result<PathBuf, CommandError> {
    let value = Path::new(value);
    if value.is_absolute() {
        return Ok(value.to_path_buf());
    }
    let parent = config_path
        .parent()
        .ok_or(CommandError::InvalidConfigPath)?;
    Ok(parent.join(value))
}

fn read_bounded_regular_file(
    path: &Path,
    limit: u64,
    operation: &'static str,
) -> Result<Vec<u8>, CommandError> {
    #[cfg(unix)]
    {
        use rustix::fs::{FileType, Mode, OFlags, fstat, open};

        let descriptor = open(
            path,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|source| CommandError::Filesystem {
            operation,
            source: source.into(),
        })?;
        let before = fstat(&descriptor).map_err(|source| CommandError::Filesystem {
            operation,
            source: source.into(),
        })?;
        if !FileType::from_raw_mode(before.st_mode).is_file()
            || before.st_size < 0
            || u64::try_from(before.st_size)
                .ok()
                .is_none_or(|size| size > limit)
        {
            return Err(CommandError::InvalidInputFile { operation });
        }
        let expected = u64::try_from(before.st_size)
            .map_err(|_| CommandError::InvalidInputFile { operation })?;
        let capacity =
            usize::try_from(expected).map_err(|_| CommandError::InvalidInputFile { operation })?;
        let mut bytes = Vec::with_capacity(capacity);
        let mut file = File::from(descriptor);
        Read::by_ref(&mut file)
            .take(limit.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|source| CommandError::Filesystem { operation, source })?;
        let after = fstat(&file).map_err(|source| CommandError::Filesystem {
            operation,
            source: source.into(),
        })?;
        if u64::try_from(bytes.len()).ok() != Some(expected)
            || u64::try_from(after.st_size).ok() != Some(expected)
        {
            return Err(CommandError::InvalidInputFile { operation });
        }
        Ok(bytes)
    }
    #[cfg(not(unix))]
    {
        let _ = (path, limit, operation);
        Err(CommandError::UnsupportedPlatform)
    }
}

fn write_immutable_json(path: &Path, value: &impl Serialize) -> Result<(), CommandError> {
    let bytes = serde_json::to_vec(value)?;
    let parent = path.parent().ok_or(CommandError::InvalidImportPath)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty() && !name.contains(['/', '\\']))
        .ok_or(CommandError::InvalidImportPath)?;
    let temporary = parent.join(format!(".{name}.tmp"));
    if path.exists() {
        let existing =
            read_bounded_regular_file(path, MAX_ARCHIVE_MANIFEST_BYTES, "import manifest")?;
        return if existing == bytes {
            Ok(())
        } else {
            Err(CommandError::ImmutableImportConflict)
        };
    }
    if temporary.exists() {
        return Err(CommandError::TemporaryImportExists);
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|source| CommandError::Filesystem {
            operation: "creating temporary import manifest",
            source,
        })?;
    set_private_import_file(&file)?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|source| CommandError::Filesystem {
            operation: "writing import manifest",
            source,
        })?;
    sync_directory(parent)?;
    fs::rename(&temporary, path).map_err(|source| CommandError::Filesystem {
        operation: "atomically publishing import manifest",
        source,
    })?;
    sync_directory(parent)
}

#[cfg(unix)]
fn set_private_import_file(file: &File) -> Result<(), CommandError> {
    rustix::fs::fchmod(file, rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR).map_err(|source| {
        CommandError::Filesystem {
            operation: "setting private import manifest permissions",
            source: source.into(),
        }
    })
}

#[cfg(not(unix))]
fn set_private_import_file(_file: &File) -> Result<(), CommandError> {
    Err(CommandError::InvalidImportPath)
}

fn sync_directory(path: &Path) -> Result<(), CommandError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| CommandError::Filesystem {
            operation: "fsyncing import directory",
            source,
        })
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("b3:{}", blake3::hash(bytes).to_hex())
}

fn code_digest() -> Result<String, CommandError> {
    option_env!("TRENCH_WORKSPACE_BUILD_DIGEST")
        .map(str::to_owned)
        .ok_or(CommandError::MissingBuildDigest)
}

/// Offline, non-mutating local preflight report.
#[derive(Debug, Serialize)]
struct DoctorReport {
    ok: bool,
    reasons: Vec<&'static str>,
}

/// Inspects only validated local paths. This function creates no path, opens no
/// database connection, and performs no network operation.
fn doctor(config_path: &Path) -> Result<DoctorReport, CommandError> {
    let loaded = load_config(config_path)?;
    let sqlite_path =
        app::configured_path(&loaded.physical_path, loaded.config.storage().sqlite_path())?;
    let parquet_path = app::configured_path(
        &loaded.physical_path,
        loaded.config.storage().parquet_path(),
    )?;
    let runtime_path = PathBuf::from(loaded.config.runtime().admin_socket_path());
    let mut reasons = Vec::new();
    inspect_regular_file(&sqlite_path, "sqlite_database", &mut reasons);
    inspect_directory(&parquet_path, "parquet_store", &mut reasons);
    match runtime_path.parent() {
        Some(parent) => inspect_directory(parent, "runtime_directory", &mut reasons),
        None => reasons.push("runtime_directory_invalid"),
    }
    if RulesStartup::resolve(&loaded).error().is_some() {
        reasons.push("rules_artifact_unready");
    }
    Ok(DoctorReport {
        ok: reasons.is_empty(),
        reasons,
    })
}

fn inspect_regular_file(path: &Path, label: &'static str, reasons: &mut Vec<&'static str>) {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {}
        Ok(_) => reasons.push(match label {
            "sqlite_database" => "sqlite_database_invalid",
            _ => "local_file_invalid",
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => reasons.push(match label {
            "sqlite_database" => "sqlite_database_missing",
            _ => "local_file_missing",
        }),
        Err(_) => reasons.push(match label {
            "sqlite_database" => "sqlite_database_unreadable",
            _ => "local_file_unreadable",
        }),
    }
}

fn inspect_directory(path: &Path, label: &'static str, reasons: &mut Vec<&'static str>) {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => reasons.push(match label {
            "parquet_store" => "parquet_store_invalid",
            "runtime_directory" => "runtime_directory_invalid",
            _ => "local_directory_invalid",
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => reasons.push(match label {
            "parquet_store" => "parquet_store_missing",
            "runtime_directory" => "runtime_directory_missing",
            _ => "local_directory_missing",
        }),
        Err(_) => reasons.push(match label {
            "parquet_store" => "parquet_store_unreadable",
            "runtime_directory" => "runtime_directory_unreadable",
            _ => "local_directory_unreadable",
        }),
    }
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (digits, suffix) = value.split_at(split);
    let count = digits
        .parse::<u64>()
        .map_err(|_| "duration must start with a positive integer".to_owned())?;
    if count == 0 {
        return Err("duration must be greater than zero".to_owned());
    }
    let seconds = match suffix {
        "s" => count,
        "m" => count
            .checked_mul(60)
            .ok_or_else(|| "duration is too large".to_owned())?,
        "h" => count
            .checked_mul(3_600)
            .ok_or_else(|| "duration is too large".to_owned())?,
        _ => return Err("duration must use s, m, or h units".to_owned()),
    };
    Ok(Duration::from_secs(seconds))
}

fn write_stdout_json(value: &impl Serialize) -> Result<(), CommandError> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    std::io::stdout()
        .lock()
        .write_all(&bytes)
        .map_err(|source| CommandError::Filesystem {
            operation: "writing command output",
            source,
        })
}

fn write_status_text(status: &serde_json::Value) -> Result<(), CommandError> {
    let run_id = status
        .pointer("/status/run_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let reconciled = status
        .pointer("/status/reconciled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let mode = status
        .pointer("/status/mode")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let execution_enabled = status
        .pointer("/status/execution_enabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let output = format!(
        "run_id={run_id} reconciled={reconciled} mode={mode} execution_enabled={execution_enabled}\n"
    );
    std::io::stdout()
        .lock()
        .write_all(output.as_bytes())
        .map_err(|source| CommandError::Filesystem {
            operation: "writing command output",
            source,
        })
}

/// A command input, archive, recovery, or durable-import failure.
#[derive(Debug, Error)]
pub enum CommandError {
    /// Offline doctor found a non-mutating local preflight failure.
    #[error("doctor reported failed local preflight")]
    DoctorFailed,
    /// Research completed with a canonical but promotion-ineligible report.
    #[error("rules research is ineligible; inspect rules-validation.json")]
    RulesResearchIneligible,
    /// Descriptor-safe local archive imports require Unix no-follow semantics.
    #[cfg(not(unix))]
    #[error("archive import is unsupported on this platform")]
    UnsupportedPlatform,
    /// The build did not embed a complete immutable workspace commitment.
    #[error("immutable workspace build digest was not embedded")]
    MissingBuildDigest,
    /// A source or manifest path that must be absolute was relative.
    #[error("{field} path must be absolute")]
    PathMustBeAbsolute { field: &'static str },
    /// The configuration path could not establish a local storage base.
    #[error("configuration path has no usable parent")]
    InvalidConfigPath,
    /// The trusted paper configuration was invalid.
    #[error(transparent)]
    Config(#[from] trench_core::config::ConfigError),
    /// The configuration file was not UTF-8 TOML.
    #[error("configuration file is not UTF-8")]
    InvalidConfigEncoding,
    /// An archive manifest used a shape outside the strict local import schema.
    #[error("archive manifest is invalid")]
    InvalidArchiveManifest,
    /// A supplied local input path was not a bounded regular file.
    #[cfg(unix)]
    #[error("{operation} must be a bounded non-symlink regular file")]
    InvalidInputFile { operation: &'static str },
    /// An immutable import-manifest output path was invalid.
    #[error("import manifest path is invalid")]
    InvalidImportPath,
    /// A prior interrupted import-manifest write remains as a temporary sibling.
    #[error("temporary import manifest sibling exists")]
    TemporaryImportExists,
    /// A content-addressed import path already contains different immutable bytes.
    #[error("immutable import manifest conflicts with existing content")]
    ImmutableImportConflict,
    /// A rules-research output directory or fixed output name was unsafe.
    #[error("rules research output directory is invalid")]
    InvalidResearchOutput,
    /// A prior interrupted rules-research write remains as a temporary sibling.
    #[error("temporary rules research output exists")]
    TemporaryResearchOutput,
    /// A fixed immutable rules-research output already contains different bytes.
    #[error("immutable rules research output conflicts with existing content")]
    ImmutableResearchConflict,
    /// The research replay plan belongs to another config/code/schema run.
    #[error("replay plan provenance does not match the supplied config and code")]
    ReplayProvenanceMismatch,
    /// The replay source contained no normalized market facts.
    #[error("research replay source is empty")]
    ResearchSourceEmpty,
    /// The replay source cannot form two bounded contiguous source windows.
    #[error("research replay source cannot form a bounded source window")]
    ResearchSourceWindow,
    /// A local filesystem operation failed.
    #[error("filesystem operation failed while {operation}")]
    Filesystem {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    /// The bounded JSON manifest format was invalid.
    #[error("JSON input is invalid")]
    Json(#[from] serde_json::Error),
    /// Rules artifact/report canonicalization or validation failed.
    #[error(transparent)]
    Validation(#[from] trench_core::validation::ValidationError),
    /// Archive verification or decoding rejected local evidence.
    #[error(transparent)]
    Archive(#[from] trench_hyperliquid::ArchiveError),
    /// Gap-recovery's L2-only boundary rejected the archive evidence.
    #[error(transparent)]
    Recovery(#[from] trench_hyperliquid::RecoveryError),
    /// Atomic normalized-data storage rejected the import.
    #[error(transparent)]
    Storage(#[from] ParquetError),
    /// The frozen replay-plan construction or persistence failed.
    #[error(transparent)]
    Replay(#[from] ReplayError),
    /// The verified research source-plan construction failed.
    #[error(transparent)]
    ResearchPlan(#[from] ResearchPlanError),
    /// The verified availability run could not be reopened or published.
    #[error(transparent)]
    ResearchRun(#[from] ResearchRunError),
    /// Causal source compilation rejected the verified source run.
    #[error(transparent)]
    ResearchCompile(#[from] ResearchCompileError),
    /// A normalized source timestamp could not be represented.
    #[error(transparent)]
    Event(#[from] trench_core::event::EventError),
    /// Daemon lifecycle or deterministic replay orchestration failed.
    #[error(transparent)]
    App(#[from] crate::app::AppError),
    /// Local Unix admin status protocol failed.
    #[error(transparent)]
    Admin(#[from] crate::admin::AdminError),
    /// A checked normalized market identifier was invalid.
    #[error(transparent)]
    Domain(#[from] trench_core::domain::DomainError),
}

#[cfg(test)]
pub(crate) mod import_archive_tests {
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use std::path::Path;

    use super::{ImportArchiveArgs, import_archive};

    const ARCHIVE_DIGEST: &str =
        "b3:b7fbf0b0473d3dfb5e32b824360e840978d47f8321b748c493585240b52fed6a";

    fn secure(path: &Path) {
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .expect("fixture directory should be private");
    }

    #[test]
    fn verified_archive_import_uses_the_atomic_parquet_and_replay_plan_paths() {
        let root = tempfile::TempDir::new().expect("fixture root should be created");
        secure(root.path());
        let archive_root = root.path().join("archive");
        fs::create_dir(&archive_root).expect("archive root should be created");
        secure(&archive_root);
        let relative = Path::new("market_data/20230916/9/l2Book/SOL.lz4");
        let destination = archive_root.join(relative);
        fs::create_dir_all(
            destination
                .parent()
                .expect("fixture source parent should exist"),
        )
        .expect("fixture source parent should be created");
        fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../tests/fixtures/archive/l2-sample.lz4"),
            &destination,
        )
        .expect("archive fixture should copy");
        let storage = root.path().join("parquet");
        fs::create_dir(&storage).expect("parquet root should be created");
        secure(&storage);
        let config = root.path().join("paper.toml");
        let config_body = include_str!("../../../config/paper.example.toml").replace(
            "parquet_path = \"data/parquet\"",
            &format!("parquet_path = \"{}\"", storage.display()),
        );
        fs::write(&config, config_body).expect("config fixture should write");
        let manifest = root.path().join("archive-manifest.json");
        fs::write(
            &manifest,
            format!(
                r#"{{"as_of_ms":1694862000000,"requirements":[{{"market":"SOL","data_kind":"l2_book","start_ms":1694854800000,"end_ms":1694858400000,"required":true}}],"sources":[{{"market":"SOL","data_kind":"l2_book","start_ms":1694854800000,"end_ms":1694858400000,"relative_path":"{}","compressed_bytes":173,"compressed_digest":"{}"}}]}}"#,
                relative.display(), ARCHIVE_DIGEST
            ),
        )
        .expect("archive manifest fixture should write");

        let result = import_archive(ImportArchiveArgs {
            config: config.clone(),
            source: archive_root.clone(),
            manifest: manifest.clone(),
        })
        .expect("archive import should use only the tested atomic sink");

        assert!(result.import_manifest.is_file());
        assert!(storage.join("imports").is_dir());
        assert!(result.replay_plan_digest.is_some());
        let persisted: serde_json::Value = serde_json::from_slice(
            &fs::read(&result.import_manifest).expect("immutable import manifest should read"),
        )
        .expect("immutable import manifest should be JSON");
        assert_eq!(
            persisted["source_manifest"]["as_of_ms"],
            1_694_862_000_000_i64
        );
        assert_eq!(
            persisted["source_manifest"]["sources"][0]["relative_path"],
            relative.display().to_string()
        );
        assert_eq!(
            persisted["source_manifest"]["sources"][0]["compressed_digest"],
            ARCHIVE_DIGEST
        );

        let wrong_digest = root.path().join("wrong-digest.json");
        fs::write(
            &wrong_digest,
            format!(
                r#"{{"as_of_ms":1694862000000,"requirements":[{{"market":"SOL","data_kind":"l2_book","start_ms":1694854800000,"end_ms":1694858400000,"required":true}}],"sources":[{{"market":"SOL","data_kind":"l2_book","start_ms":1694854800000,"end_ms":1694858400000,"relative_path":"{}","compressed_bytes":173,"compressed_digest":"b3:{}"}}]}}"#,
                relative.display(),
                "a".repeat(64),
            ),
        )
        .expect("wrong-digest manifest should write");
        assert!(
            import_archive(ImportArchiveArgs {
                config: config.clone(),
                source: archive_root.clone(),
                manifest: wrong_digest,
            })
            .is_err()
        );

        let wrong_path = root.path().join("wrong-path.json");
        fs::write(
            &wrong_path,
            format!(
                r#"{{"as_of_ms":1694862000000,"requirements":[{{"market":"SOL","data_kind":"l2_book","start_ms":1694854800000,"end_ms":1694858400000,"required":true}}],"sources":[{{"market":"SOL","data_kind":"l2_book","start_ms":1694854800000,"end_ms":1694858400000,"relative_path":"market_data/20230916/9/l2Book/OTHER.lz4","compressed_bytes":173,"compressed_digest":"{}"}}]}}"#,
                ARCHIVE_DIGEST,
            ),
        )
        .expect("wrong-path manifest should write");
        assert!(
            import_archive(ImportArchiveArgs {
                config,
                source: archive_root,
                manifest: wrong_path,
            })
            .is_err()
        );
    }

    #[test]
    fn optional_empty_interval_persists_unavailable_import_evidence() {
        let root = tempfile::TempDir::new().expect("fixture root should be created");
        secure(root.path());
        let archive_root = root.path().join("archive");
        fs::create_dir(&archive_root).expect("archive root should be created");
        secure(&archive_root);
        let storage = root.path().join("parquet");
        fs::create_dir(&storage).expect("parquet root should be created");
        secure(&storage);
        let config = root.path().join("paper.toml");
        let config_body = include_str!("../../../config/paper.example.toml").replace(
            "parquet_path = \"data/parquet\"",
            &format!("parquet_path = \"{}\"", storage.display()),
        );
        fs::write(&config, config_body).expect("config fixture should write");
        let manifest = root.path().join("archive-manifest.json");
        let write_optional_manifest = |as_of_ms| {
            fs::write(
                &manifest,
                format!(
                    r#"{{"as_of_ms":{as_of_ms},"requirements":[{{"market":"SOL","data_kind":"l2_book","start_ms":1694854800000,"end_ms":1694858400000,"required":false}}],"sources":[]}}"#
                ),
            )
            .expect("optional archive manifest should write");
        };

        write_optional_manifest(1_694_862_000_000_i64);
        let first = import_archive(ImportArchiveArgs {
            config: config.clone(),
            source: archive_root.clone(),
            manifest: manifest.clone(),
        })
        .expect("optional absent interval should be durably recorded");
        let persisted: serde_json::Value = serde_json::from_slice(
            &fs::read(&first.import_manifest).expect("unavailable import should read"),
        )
        .expect("unavailable import should be JSON");
        assert_eq!(first.replay_plan_digest, None);
        assert_eq!(persisted["replay_state"], "unavailable");
        assert!(persisted["replay_plan_digest"].is_null());
        assert_eq!(persisted["missing_spans"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            persisted["source_manifest"]["as_of_ms"],
            1_694_862_000_000_i64
        );

        write_optional_manifest(1_694_862_000_001_i64);
        let second = import_archive(ImportArchiveArgs {
            config,
            source: archive_root,
            manifest,
        })
        .expect("changed as-of must become distinct immutable evidence");
        assert_ne!(first.import_digest, second.import_digest);
        assert_ne!(first.import_manifest, second.import_manifest);
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_config_is_rejected_through_the_no_follow_descriptor() {
        let root = tempfile::TempDir::new().expect("fixture root should be created");
        secure(root.path());
        let target = root.path().join("config-target.toml");
        fs::write(&target, "[paper]\ninitial_equity_usdc = \"100\"\n")
            .expect("config target should write");
        let link = root.path().join("config-link.toml");
        symlink(&target, &link).expect("config fixture symlink should create");

        assert!(
            import_archive(ImportArchiveArgs {
                config: link,
                source: root.path().to_path_buf(),
                manifest: root.path().join("unread-manifest.json"),
            })
            .is_err()
        );
    }
}

#[cfg(test)]
mod doctor_tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use super::doctor;

    #[test]
    fn doctor_is_offline_read_only_and_never_creates_runtime_paths() {
        let root = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        let runtime = root.path().join("runtime/trenchd.sock");
        let config = root.path().join("paper.toml");
        let body = include_str!("../../../config/paper.example.toml")
            .replace("/run/trench/trenchd.sock", &runtime.display().to_string());
        fs::write(&config, body).expect("fixture config");

        let report = doctor(&config).expect("offline doctor report");
        assert!(!report.ok);
        assert_eq!(
            report.reasons,
            vec![
                "sqlite_database_missing",
                "parquet_store_missing",
                "runtime_directory_missing",
            ]
        );
        assert!(!root.path().join("state").exists());
        assert!(!root.path().join("data").exists());
        assert!(!root.path().join("runtime").exists());
    }
}

#[cfg(test)]
mod rules_research_tests {
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};

    use rust_decimal::Decimal;
    use trench_core::domain::{Market, Price, Quantity, Side};
    use trench_core::event::{MarketEvent, TimestampNs, Trade};
    use trench_core::validation::{
        EngineReplayOutcome, IneligibleReason, ReplayPhase, ResearchEligibility,
        ResearchProvenance, RuleReplay, RuleReplayRequest, RulesValidationReport, ValidationPlan,
    };
    use trench_storage::parquet::{DataProvenance, ParquetStore};
    use trench_storage::replay::ReplayPlan;

    use super::{
        CommandError, RulesArtifactError, RulesResearchArgs, RulesStartup, code_digest,
        digest_bytes, load_config, research_rules, resolve_configured_path,
    };

    fn secure(path: &std::path::Path) {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .expect("fixture directory should be private");
    }

    fn digest(index: u8) -> String {
        format!("b3:{index:064x}")
    }

    fn base_config(root: &std::path::Path) -> String {
        include_str!("../../../config/paper.example.toml")
            .replace(
                "parquet_path = \"data/parquet\"",
                &format!("parquet_path = \"{}\"", root.join("parquet").display()),
            )
            .replace(
                "/run/trench/trenchd.sock",
                &root.join("runtime/trenchd.sock").display().to_string(),
            )
    }

    struct Replay;

    impl RuleReplay for Replay {
        fn replay(
            &mut self,
            request: RuleReplayRequest,
        ) -> Result<EngineReplayOutcome, trench_core::validation::ValidationError> {
            let score = request.config.threshold().value() * Decimal::from(100)
                + request.config.atr_floor().value()
                + request.config.take_profit().value() / Decimal::from(100);
            let (trades, offset) = match request.phase {
                ReplayPhase::InnerValidation { inner_fold } => (1, inner_fold),
                ReplayPhase::Calibration => (1, 10),
                ReplayPhase::OuterTest => (34, 20),
            };
            EngineReplayOutcome::new(
                score,
                Decimal::ONE,
                trades,
                digest(request.outer_fold as u8 + offset + 20),
                digest(request.outer_fold as u8 + offset + 40),
                digest(request.outer_fold as u8 + offset + 60),
                digest(request.outer_fold as u8 + offset + 80),
            )
        }
    }

    fn active_fixture(root: &std::path::Path) -> std::path::PathBuf {
        active_fixture_with_code(root, code_digest().expect("embedded build digest"))
    }

    fn active_fixture_with_code(
        root: &std::path::Path,
        artifact_code_digest: String,
    ) -> std::path::PathBuf {
        let staged = root.join("staged");
        fs::create_dir(&staged).expect("staged directory");
        secure(&staged);
        let collect = base_config(root);
        let provenance = ResearchProvenance {
            config_digest: trench_core::config::PaperConfig::research_digest(&collect)
                .expect("semantic config digest"),
            code_digest: artifact_code_digest,
            data_digest: digest(1),
            universe_digest: digest(2),
            feature_schema_digest: digest(3),
            data_cutoff: TimestampNs::new(1).expect("cutoff"),
        };
        let report = RulesValidationReport::run(
            &ValidationPlan::build(
                TimestampNs::new(0).expect("origin"),
                ValidationPlan::minimum_complete_days(),
            )
            .expect("fold plan"),
            provenance,
            Vec::new(),
            &mut Replay,
        )
        .expect("eligible report");
        let artifact = report.artifact().expect("eligible artifact");
        let active = collect.replacen(
            "mode = \"collect_only\"",
            &format!(
                "mode = \"active\"\nartifact_file = \"rules-artifact.json\"\nartifact_digest = \"{}\"\nvalidation_report_file = \"rules-validation.json\"\nvalidation_report_digest = \"{}\"",
                artifact.digest(),
                report.digest(),
            ),
            1,
        );
        let config = staged.join("paper.toml");
        fs::write(&config, active).expect("staged config");
        fs::write(
            staged.join("rules-artifact.json"),
            artifact.canonical_json().expect("artifact JSON"),
        )
        .expect("artifact");
        fs::write(
            staged.join("rules-validation.json"),
            report.canonical_json().expect("report JSON"),
        )
        .expect("report");
        config
    }

    #[test]
    fn research_writes_a_canonical_insufficient_history_report_without_an_artifact() {
        let root = tempfile::tempdir().expect("fixture root");
        secure(root.path());
        let parquet = root.path().join("parquet");
        let output = root.path().join("output");
        fs::create_dir(&parquet).expect("parquet root");
        fs::create_dir(&output).expect("output root");
        secure(&parquet);
        secure(&output);
        let config = root.path().join("paper.toml");
        fs::write(&config, base_config(root.path())).expect("config");
        let loaded = load_config(&config).expect("load config");
        let provenance = DataProvenance::new(
            digest_bytes(&loaded.bytes),
            code_digest().expect("code digest"),
            ParquetStore::schema_hash(),
        )
        .expect("parquet provenance");
        let store = ParquetStore::open(&parquet, provenance.clone()).expect("store");
        let at = TimestampNs::new(1).expect("event time");
        let event = MarketEvent::trade(
            at,
            at,
            Market::new("SOL").expect("market"),
            Trade::new(
                1,
                Side::Buy,
                Price::new(Decimal::ONE).expect("price"),
                Quantity::new(Decimal::ONE).expect("quantity"),
            )
            .expect("trade"),
        )
        .expect("event");
        let plan = ReplayPlan::new(provenance, store.write_events(&[event]).expect("partition"))
            .expect("plan");
        let manifest = root.path().join("replay.json");
        plan.write_to(&manifest).expect("immutable plan");

        let result = research_rules(RulesResearchArgs {
            config,
            manifest,
            output: output.clone(),
        })
        .expect("ineligible research still writes report");
        assert!(!result.eligible);
        assert!(result.report_path.is_file());
        assert!(result.artifact_path.is_none());
        let report = RulesValidationReport::from_canonical_json(
            &fs::read(&result.report_path).expect("report bytes"),
        )
        .expect("canonical report");
        assert!(matches!(
            report.eligibility(),
            ResearchEligibility::Ineligible {
                reason: IneligibleReason::InsufficientTrustworthyHistory,
                ..
            }
        ));
    }

    #[test]
    fn loaded_config_keeps_relative_storage_bound_to_its_physical_release() {
        let root = tempfile::tempdir().expect("fixture root");
        secure(root.path());
        let release_a = root.path().join("release-a");
        let release_b = root.path().join("release-b");
        fs::create_dir(&release_a).expect("first release directory");
        fs::create_dir(&release_b).expect("second release directory");
        secure(&release_a);
        secure(&release_b);
        for release in [&release_a, &release_b] {
            fs::create_dir_all(release.join("data/parquet")).expect("release parquet directory");
            fs::create_dir_all(release.join("state")).expect("release state directory");
        }
        let config_body = include_str!("../../../config/paper.example.toml").replace(
            "/run/trench/trenchd.sock",
            &root
                .path()
                .join("runtime/trenchd.sock")
                .display()
                .to_string(),
        );
        let release_a_config = release_a.join("paper.toml");
        fs::write(&release_a_config, &config_body).expect("first release config");
        fs::write(release_b.join("paper.toml"), config_body).expect("second release config");
        let current = root.path().join("current");
        symlink(&release_a, &current).expect("initial release alias");

        let loaded = load_config(&current.join("paper.toml")).expect("load first release");
        fs::remove_file(&current).expect("remove initial alias");
        symlink(&release_b, &current).expect("switch release alias");

        assert_eq!(
            fs::canonicalize(
                resolve_configured_path(&current.join("paper.toml"), "data/parquet")
                    .expect("mutable alias resolution"),
            )
            .expect("mutable alias target"),
            release_b.join("data/parquet")
        );
        assert_eq!(
            loaded
                .resolve_configured_path("data/parquet")
                .expect("physical parquet resolution"),
            release_a.join("data/parquet")
        );
        assert_eq!(
            crate::app::configured_path(&loaded.physical_path, "state/trench.sqlite")
                .expect("physical sqlite resolution"),
            release_a.join("state/trench.sqlite")
        );
    }

    #[test]
    fn synthetic_eligible_canonical_pair_cannot_ready_active_rules() {
        let root = tempfile::tempdir().expect("fixture root");
        secure(root.path());
        let config = active_fixture(root.path());
        let loaded = load_config(&config).expect("active config loads");
        let startup = RulesStartup::resolve(&loaded);
        assert_eq!(
            startup.error(),
            Some(&RulesArtifactError::ReplayAdapterUnavailable)
        );
    }

    #[test]
    fn rules_research_cli_surfaces_the_canonical_ineligible_result_as_nonzero() {
        let error = CommandError::RulesResearchIneligible;
        assert_eq!(
            error.to_string(),
            "rules research is ineligible; inspect rules-validation.json"
        );
    }
}
