//! Deterministic merge and validation of complete normalized-event partitions.
//!
//! This module intentionally owns no engine state, SQLite connection, clock,
//! strategy, or execution adapter. It establishes only the validated source
//! stream that the single runtime engine path may consume.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use trench_core::domain::EventId;
use trench_core::event::MarketEvent;

use crate::parquet::{
    DataProvenance, ParquetError, ParquetStore, PartitionManifest, events_digest, replay_order,
};

const MAX_REPLAY_EVENTS: u64 = 100_000;
const MAX_REPLAY_WIRE_BYTES: u64 = 16 * 1_024 * 1_024;
const MAX_REPLAY_PARTITIONS: usize = 4_096;
const MAX_REPLAY_PLAN_BYTES: u64 = 1_048_576;
const REPLAY_PLAN_VERSION: u8 = 1;

/// A completely validated, immutable normalized event replay stream.
#[derive(Debug, Clone)]
pub struct DeterministicReplay {
    manifests: Vec<PartitionManifest>,
    events: Vec<MarketEvent>,
    digest: String,
}

/// A frozen, bounded set of content-addressed partitions for one replay epoch.
///
/// Import creates and persists this exact plan before a later state recovery.
/// Supplying a plan avoids scanning an unbounded historical root and binds the
/// selected replay window to config, code, schema, and partition content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayPlan {
    provenance: DataProvenance,
    manifests: Vec<PartitionManifest>,
    digest: String,
}

impl ReplayPlan {
    /// Freezes a finite, complete set of validated partition manifests.
    pub fn new(
        provenance: DataProvenance,
        mut manifests: Vec<PartitionManifest>,
    ) -> Result<Self, ReplayError> {
        if manifests.is_empty() {
            return Err(ReplayError::Empty);
        }
        if manifests.len() > MAX_REPLAY_PARTITIONS {
            return Err(ReplayError::ResourceLimit);
        }
        let mut partition_ids = BTreeMap::new();
        let (rows, bytes) =
            manifests
                .iter()
                .try_fold((0_u64, 0_u64), |(rows, bytes), manifest| {
                    manifest.validate()?;
                    if manifest.provenance() != &provenance {
                        return Err(ReplayError::Storage(ParquetError::ProvenanceMismatch));
                    }
                    if partition_ids
                        .insert(manifest.partition_id().to_owned(), ())
                        .is_some()
                    {
                        return Err(ReplayError::DuplicatePartition {
                            partition_id: manifest.partition_id().to_owned(),
                        });
                    }
                    bounded_totals(rows, bytes, manifest)
                })?;
        if rows == 0 || bytes == 0 {
            return Err(ReplayError::Empty);
        }
        manifests.sort_by(|left, right| left.partition_id().cmp(right.partition_id()));
        let digest = replay_plan_digest(&provenance, &manifests);
        Ok(Self {
            provenance,
            manifests,
            digest,
        })
    }

    /// Returns the frozen provenance shared by every selected partition.
    #[must_use]
    pub const fn provenance(&self) -> &DataProvenance {
        &self.provenance
    }

    /// Returns selected complete partition manifests in stable ID order.
    #[must_use]
    pub fn manifests(&self) -> &[PartitionManifest] {
        &self.manifests
    }

    /// Returns the content-addressed replay-window commitment.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Atomically persists this frozen replay selection as a canonical JSON
    /// manifest. Rewriting the same digest is idempotent; changing a path's
    /// immutable plan is rejected.
    pub fn write_to(&self, path: impl AsRef<Path>) -> Result<(), ReplayError> {
        let path = canonical_plan_path(path.as_ref())?;
        if path.exists() {
            let existing = Self::read_from(&path)?;
            return if existing == *self {
                Ok(())
            } else {
                Err(ReplayError::PlanMismatch)
            };
        }
        let wire = ReplayPlanWire::from(self);
        let bytes = serde_json::to_vec(&wire)?;
        if bytes.len() > MAX_REPLAY_PLAN_BYTES as usize {
            return Err(ReplayError::ResourceLimit);
        }
        let parent = path.parent().ok_or(ReplayError::InvalidPlanPath)?;
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty() && !name.contains(['/', '\\']))
            .ok_or(ReplayError::InvalidPlanPath)?;
        let temporary = parent.join(format!(".{name}.tmp"));
        if temporary.exists() {
            return Err(ReplayError::TemporaryPlanExists);
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|source| ReplayError::Filesystem {
                operation: "creating temporary replay plan",
                source,
            })?;
        set_private_file(&file)?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|source| ReplayError::Filesystem {
                operation: "writing replay plan",
                source,
            })?;
        sync_directory(parent)?;
        fs::rename(&temporary, &path).map_err(|source| ReplayError::Filesystem {
            operation: "atomically publishing replay plan",
            source,
        })?;
        sync_directory(parent)?;
        Ok(())
    }

    /// Reopens and validates one persisted replay-plan manifest without
    /// discovering or mutating any Parquet partition path.
    pub fn read_from(path: impl AsRef<Path>) -> Result<Self, ReplayError> {
        let path = canonical_plan_path(path.as_ref())?;
        let metadata = fs::symlink_metadata(&path).map_err(|source| ReplayError::Filesystem {
            operation: "inspecting replay plan",
            source,
        })?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() > MAX_REPLAY_PLAN_BYTES
        {
            return Err(ReplayError::InvalidPlanPath);
        }
        let mut bytes = Vec::with_capacity(
            usize::try_from(metadata.len()).map_err(|_| ReplayError::ResourceLimit)?,
        );
        File::open(&path)
            .and_then(|mut file| file.read_to_end(&mut bytes))
            .map_err(|source| ReplayError::Filesystem {
                operation: "reading replay plan",
                source,
            })?;
        if u64::try_from(bytes.len()).ok() != Some(metadata.len()) {
            return Err(ReplayError::InvalidPlanPath);
        }
        let wire: ReplayPlanWire = serde_json::from_slice(&bytes)?;
        if wire.version != REPLAY_PLAN_VERSION {
            return Err(ReplayError::InvalidPlan);
        }
        let plan = Self::new(wire.provenance, wire.manifests)?;
        if plan.digest != wire.digest {
            return Err(ReplayError::PlanMismatch);
        }
        Ok(plan)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplayPlanWire {
    version: u8,
    provenance: DataProvenance,
    manifests: Vec<PartitionManifest>,
    digest: String,
}

impl From<&ReplayPlan> for ReplayPlanWire {
    fn from(plan: &ReplayPlan) -> Self {
        Self {
            version: REPLAY_PLAN_VERSION,
            provenance: plan.provenance.clone(),
            manifests: plan.manifests.clone(),
            digest: plan.digest.clone(),
        }
    }
}

impl DeterministicReplay {
    /// Reopens complete partitions beneath `root` and validates their frozen
    /// config, code, schema, and content commitments before yielding facts.
    pub fn open(root: impl AsRef<Path>, provenance: DataProvenance) -> Result<Self, ReplayError> {
        let store = ParquetStore::open_existing(root, provenance.clone())?;
        let manifests = store.partitions()?;
        let plan = ReplayPlan::new(provenance, manifests)?;
        Self::open_plan_with_store(store, plan)
    }

    /// Replays only an explicit immutable partition plan without scanning or
    /// creating paths outside that bounded source window.
    pub fn open_plan(root: impl AsRef<Path>, plan: ReplayPlan) -> Result<Self, ReplayError> {
        let store = ParquetStore::open_existing(root, plan.provenance.clone())?;
        Self::open_plan_with_store(store, plan)
    }

    fn open_plan_with_store(store: ParquetStore, plan: ReplayPlan) -> Result<Self, ReplayError> {
        let manifests = plan.manifests;
        let mut by_id = BTreeMap::<EventId, MarketEvent>::new();
        for manifest in &manifests {
            for event in store.read_partition(manifest)? {
                match by_id.get(event.event_id()) {
                    Some(previous) if previous != &event => {
                        return Err(ReplayError::ConflictingEvent {
                            event_id: event.event_id().as_str().to_owned(),
                        });
                    }
                    Some(_) => {}
                    None => {
                        by_id.insert(event.event_id().clone(), event);
                    }
                }
            }
        }
        let mut events = by_id.into_values().collect::<Vec<_>>();
        events.sort_by(replay_order);
        let digest = events_digest(&events)?;
        Ok(Self {
            manifests,
            events,
            digest,
        })
    }

    /// Returns every complete manifest admitted to this replay.
    #[must_use]
    pub fn manifests(&self) -> &[PartitionManifest] {
        &self.manifests
    }

    /// Returns normalized facts sorted by `(event_time, kind order, event_id)`.
    #[must_use]
    pub fn events(&self) -> &[MarketEvent] {
        &self.events
    }

    /// Returns the BLAKE3 digest over this exact ordered normalized stream.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Calculates the canonical replay digest for an already ordered event slice.
    pub fn digest_events(events: &[MarketEvent]) -> Result<String, ReplayError> {
        events_digest(events).map_err(ReplayError::Storage)
    }
}

fn bounded_totals(
    rows: u64,
    bytes: u64,
    manifest: &PartitionManifest,
) -> Result<(u64, u64), ReplayError> {
    let rows = rows
        .checked_add(manifest.row_count())
        .ok_or(ReplayError::ResourceLimit)?;
    let bytes = bytes
        .checked_add(manifest.encoded_bytes())
        .ok_or(ReplayError::ResourceLimit)?;
    if rows > MAX_REPLAY_EVENTS || bytes > MAX_REPLAY_WIRE_BYTES {
        return Err(ReplayError::ResourceLimit);
    }
    Ok((rows, bytes))
}

fn replay_plan_digest(provenance: &DataProvenance, manifests: &[PartitionManifest]) -> String {
    let mut hasher = blake3::Hasher::new_derive_key("trench.replay-plan.v1");
    for value in [
        provenance.config_digest(),
        provenance.code_digest(),
        provenance.schema_hash(),
    ] {
        hasher.update(&(value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    for manifest in manifests {
        for value in [manifest.partition_id(), manifest.content_digest()] {
            hasher.update(&(value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
    }
    format!("b3:{}", hasher.finalize().to_hex())
}

/// A partition validation or deterministic-source-merge failure.
#[derive(Debug, Error)]
pub enum ReplayError {
    /// A Parquet partition was incomplete, malformed, or not frozen to this replay.
    #[error(transparent)]
    Storage(#[from] ParquetError),
    /// Two complete sources claimed the same canonical event identity differently.
    #[error("conflicting normalized replay event {event_id}")]
    ConflictingEvent { event_id: String },
    /// A replay plan repeated an immutable physical partition identity.
    #[error("duplicate partition identity in replay plan {partition_id}")]
    DuplicatePartition { partition_id: String },
    /// State reconstruction has no complete normalized source facts to apply.
    #[error("deterministic replay has no complete partitions")]
    Empty,
    /// The explicit finite replay window exceeds its fixed memory contract.
    #[error("deterministic replay exceeds its bounded event or wire budget")]
    ResourceLimit,
    /// A persisted replay plan was malformed or had an invalid version.
    #[error("replay plan is invalid")]
    InvalidPlan,
    /// A replay-plan path was not an absolute private regular-file target.
    #[error("replay plan path is invalid")]
    InvalidPlanPath,
    /// A temporary replay-plan sibling already records an interrupted publish.
    #[error("temporary replay plan sibling exists")]
    TemporaryPlanExists,
    /// A persisted plan's digest or immutable content conflicted with its path.
    #[error("replay plan digest or immutable content mismatch")]
    PlanMismatch,
    /// A replay-plan filesystem operation failed.
    #[error("filesystem operation failed while {operation}")]
    Filesystem {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    /// A replay plan did not use bounded valid JSON.
    #[error("replay plan JSON is invalid")]
    Json(#[from] serde_json::Error),
}

impl ReplayError {
    /// Returns whether replay rejected a partition from another frozen run.
    #[must_use]
    pub const fn is_provenance_mismatch(&self) -> bool {
        matches!(self, Self::Storage(error) if error.is_provenance_mismatch())
    }
}

fn canonical_plan_path(path: &Path) -> Result<std::path::PathBuf, ReplayError> {
    if !path.is_absolute() || path.file_name().is_none() {
        return Err(ReplayError::InvalidPlanPath);
    }
    let parent = path.parent().ok_or(ReplayError::InvalidPlanPath)?;
    validate_plan_ancestors(parent)?;
    let canonical_parent = fs::canonicalize(parent).map_err(|source| ReplayError::Filesystem {
        operation: "canonicalizing replay plan parent",
        source,
    })?;
    let metadata =
        fs::symlink_metadata(&canonical_parent).map_err(|source| ReplayError::Filesystem {
            operation: "inspecting replay plan parent",
            source,
        })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() || !private_directory(&metadata) {
        return Err(ReplayError::InvalidPlanPath);
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty() && !name.contains(['/', '\\']))
        .ok_or(ReplayError::InvalidPlanPath)?;
    Ok(canonical_parent.join(name))
}

fn validate_plan_ancestors(parent: &Path) -> Result<(), ReplayError> {
    #[cfg(not(unix))]
    {
        let _ = parent;
        return Err(ReplayError::InvalidPlanPath);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        use std::path::Component;

        let mut current = std::path::PathBuf::from("/");
        for component in parent.components() {
            match component {
                Component::RootDir => continue,
                Component::Normal(part) => current.push(part),
                _ => return Err(ReplayError::InvalidPlanPath),
            }
            let metadata =
                fs::symlink_metadata(&current).map_err(|source| ReplayError::Filesystem {
                    operation: "inspecting replay plan ancestor",
                    source,
                })?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(ReplayError::InvalidPlanPath);
            }
            let mode = metadata.mode();
            let sticky_world_writable = mode & 0o1000 != 0 && mode & 0o002 != 0;
            if mode & 0o022 != 0 && !sticky_world_writable {
                return Err(ReplayError::InvalidPlanPath);
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
fn private_directory(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    metadata.uid() == rustix::process::geteuid().as_raw() && metadata.mode() & 0o077 == 0
}

#[cfg(not(unix))]
fn private_directory(_metadata: &fs::Metadata) -> bool {
    false
}

fn sync_directory(path: &Path) -> Result<(), ReplayError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| ReplayError::Filesystem {
            operation: "fsyncing replay plan directory",
            source,
        })
}

#[cfg(unix)]
fn set_private_file(file: &File) -> Result<(), ReplayError> {
    rustix::fs::fchmod(file, rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR).map_err(|source| {
        ReplayError::Filesystem {
            operation: "setting replay plan permissions",
            source: source.into(),
        }
    })
}

#[cfg(not(unix))]
fn set_private_file(_file: &File) -> Result<(), ReplayError> {
    Err(ReplayError::InvalidPlanPath)
}
