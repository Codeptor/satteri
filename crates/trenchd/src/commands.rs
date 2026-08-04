//! Paper-only daemon commands composed from the tested storage boundaries.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use trench_core::config::PaperConfig;
use trench_core::domain::Market;
use trench_hyperliquid::{
    ArchiveDataKind, ArchiveDigest, ArchiveManifest, ArchiveReader, ArchiveRequirement,
    ArchiveSource, ArchiveSpan, GapRecovery,
};
use trench_storage::parquet::{DataProvenance, ParquetError, ParquetStore};
use trench_storage::replay::{ReplayError, ReplayPlan};

const MAX_CONFIG_BYTES: u64 = 65_536;
const MAX_ARCHIVE_MANIFEST_BYTES: u64 = 1_048_576;
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
    pub fn execute(self) -> Result<(), CommandError> {
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
        }
    }
}

/// Paper-only durable import commands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Verify a local official archive and persist only normalized L2 facts.
    ImportArchive(ImportArchiveArgs),
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
    let config_bytes = read_bounded_regular_file(&arguments.config, MAX_CONFIG_BYTES, "config")?;
    let config = std::str::from_utf8(&config_bytes)
        .map_err(|_| CommandError::InvalidConfigEncoding)
        .and_then(|text| PaperConfig::from_toml(text).map_err(CommandError::Config))?;
    let archive_input = read_archive_manifest(&arguments.manifest)?;
    let archive = ArchiveReader::open(&arguments.source, archive_input.manifest)?.read_all()?;
    let events = GapRecovery::archive_l2_events(&archive)?;

    let parquet_root = resolve_configured_path(&arguments.config, config.storage().parquet_path())?;
    let provenance = DataProvenance::new(
        digest_bytes(&config_bytes),
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

/// A command input, archive, recovery, or durable-import failure.
#[derive(Debug, Error)]
pub enum CommandError {
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
