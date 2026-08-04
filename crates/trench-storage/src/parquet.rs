//! Atomic, self-validating Parquet partitions for normalized public market data.
//!
//! A complete partition is a directory containing one Parquet file and one
//! manifest. The directory is written as a temporary sibling and renamed only
//! after both files are durable and the Parquet rows reopen successfully. A
//! recovery scan ignores temporary siblings, so a process loss can never make
//! a half-written partition replayable.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use trench_core::domain::{EventId, Market, Price, Quantity, Side, Usdc};
use trench_core::event::{
    AssetContext, Bbo, BookLevel, BookSnapshot, CandleInterval, CompletedCandle, EventError,
    Funding, FundingRate, MarketEvent, MarketEventKind, Metadata, TimestampNs, Trade,
};

const PARTITIONS_DIRECTORY: &str = "partitions";
const IMPORTS_DIRECTORY: &str = "imports";
const EVENT_FILE: &str = "events.parquet";
const MANIFEST_FILE: &str = "manifest.json";
const PARTITION_SCHEMA_VERSION: u8 = 1;
const MAX_EVENTS_PER_BATCH: usize = 100_000;
const MAX_DISCOVERED_PARTITIONS: usize = 4_096;
const MAX_BOOK_LEVELS_PER_EVENT: usize = 2_000;
const MAX_EVENT_WIRE_BYTES: usize = 64 * 1_024;
const MAX_PARTITION_WIRE_BYTES: usize = 8 * 1_024 * 1_024;
const MAX_MANIFEST_BYTES: u64 = 1_048_576;
const MAX_PARQUET_BYTES: u64 = 64 * 1_024 * 1_024;
const MAX_PARQUET_METADATA_BYTES: u32 = 1_048_576;
const MAX_PARQUET_UNCOMPRESSED_BYTES: i64 = 32 * 1_024 * 1_024;
const REPLAY_READ_BATCH_ROWS: usize = 128;

/// Immutable frozen run/provenance commitments required for every partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataProvenance {
    config_digest: String,
    code_digest: String,
    schema_hash: String,
}

impl DataProvenance {
    /// Creates validated BLAKE3 commitments for a deterministic data run.
    ///
    /// `schema_hash` must equal [`ParquetStore::schema_hash`], which prevents a
    /// reader from interpreting one physical Arrow schema as another.
    pub fn new(
        config_digest: impl Into<String>,
        code_digest: impl Into<String>,
        schema_hash: impl Into<String>,
    ) -> Result<Self, ParquetError> {
        let provenance = Self {
            config_digest: config_digest.into(),
            code_digest: code_digest.into(),
            schema_hash: schema_hash.into(),
        };
        provenance.validate()?;
        Ok(provenance)
    }

    /// Returns the frozen configuration digest.
    #[must_use]
    pub fn config_digest(&self) -> &str {
        &self.config_digest
    }

    /// Returns the immutable code/build digest.
    #[must_use]
    pub fn code_digest(&self) -> &str {
        &self.code_digest
    }

    /// Returns the physical normalized-event Arrow schema hash.
    #[must_use]
    pub fn schema_hash(&self) -> &str {
        &self.schema_hash
    }

    pub(crate) fn validate(&self) -> Result<(), ParquetError> {
        for (field, value) in [
            ("config digest", self.config_digest.as_str()),
            ("code digest", self.code_digest.as_str()),
            ("schema hash", self.schema_hash.as_str()),
        ] {
            validate_digest(field, value)?;
        }
        if self.schema_hash != ParquetStore::schema_hash() {
            return Err(ParquetError::SchemaHashMismatch {
                expected: ParquetStore::schema_hash(),
                actual: self.schema_hash.clone(),
            });
        }
        Ok(())
    }
}

/// One immutable partition manifest committed alongside its Parquet rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionManifest {
    version: u8,
    partition_id: String,
    date: String,
    event_kind: String,
    market: String,
    row_count: u64,
    encoded_bytes: u64,
    min_event_time_ns: i64,
    max_event_time_ns: i64,
    content_digest: String,
    provenance: DataProvenance,
}

impl PartitionManifest {
    /// Returns the content-addressed immutable partition identifier.
    #[must_use]
    pub fn partition_id(&self) -> &str {
        &self.partition_id
    }

    /// Returns the committed event row count.
    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns the exact canonical normalized-wire byte count in this partition.
    #[must_use]
    pub const fn encoded_bytes(&self) -> u64 {
        self.encoded_bytes
    }

    /// Returns the earliest authoritative event time.
    #[must_use]
    pub fn min_event_time(&self) -> TimestampNs {
        // Validated when the manifest was created or read from disk.
        TimestampNs::new(i128::from(self.min_event_time_ns))
            .expect("partition manifest retains validated event time")
    }

    /// Returns the latest authoritative event time.
    #[must_use]
    pub fn max_event_time(&self) -> TimestampNs {
        TimestampNs::new(i128::from(self.max_event_time_ns))
            .expect("partition manifest retains validated event time")
    }

    /// Returns the BLAKE3 digest over canonical normalized rows.
    #[must_use]
    pub fn content_digest(&self) -> &str {
        &self.content_digest
    }

    /// Returns the frozen run and physical-schema commitments.
    #[must_use]
    pub const fn provenance(&self) -> &DataProvenance {
        &self.provenance
    }

    fn key(&self) -> Result<PartitionKey, ParquetError> {
        PartitionKey::from_manifest(self)
    }

    pub(crate) fn validate(&self) -> Result<(), ParquetError> {
        if self.version != PARTITION_SCHEMA_VERSION {
            return Err(ParquetError::UnsupportedManifestVersion {
                actual: self.version,
            });
        }
        validate_digest("partition id", &self.partition_id)?;
        validate_digest("partition content digest", &self.content_digest)?;
        self.provenance.validate()?;
        let key = self.key()?;
        if self.row_count == 0
            || self.row_count > MAX_EVENTS_PER_BATCH as u64
            || self.encoded_bytes == 0
            || self.encoded_bytes > MAX_PARTITION_WIRE_BYTES as u64
        {
            return Err(ParquetError::InvalidManifest {
                reason: "row count is outside the bounded partition range",
            });
        }
        let min_event_time =
            TimestampNs::new(i128::from(self.min_event_time_ns)).map_err(|_| {
                ParquetError::InvalidManifest {
                    reason: "minimum event time is invalid",
                }
            })?;
        let max_event_time =
            TimestampNs::new(i128::from(self.max_event_time_ns)).map_err(|_| {
                ParquetError::InvalidManifest {
                    reason: "maximum event time is invalid",
                }
            })?;
        if min_event_time > max_event_time || key.date != utc_day_component(min_event_time) {
            return Err(ParquetError::InvalidManifest {
                reason: "manifest event-time range does not match its UTC date",
            });
        }
        Ok(())
    }
}

/// Deterministic failure injection used only by recovery tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionFailure {
    /// Stop after the temporary sibling has been fully validated and fsynced.
    BeforeRename,
}

/// A root-owned, append-only normalized-event Parquet store.
#[derive(Debug, Clone)]
pub struct ParquetStore {
    root: PathBuf,
    provenance: DataProvenance,
}

impl ParquetStore {
    /// Opens or creates a private local store rooted at an absolute path.
    ///
    /// The root and every managed child are rejected if they are symlinks. No
    /// externally supplied market identifier is ever used as a path component.
    pub fn open(root: impl AsRef<Path>, provenance: DataProvenance) -> Result<Self, ParquetError> {
        provenance.validate()?;
        let root = root.as_ref();
        if !root.is_absolute() {
            return Err(ParquetError::InvalidRoot {
                reason: "root must be absolute",
            });
        }
        let root = validate_private_root(root)?;
        let partitions = root.join(PARTITIONS_DIRECTORY);
        ensure_private_directory(&partitions)?;
        Ok(Self { root, provenance })
    }

    /// Opens an existing private store without creating any path or partition.
    ///
    /// Replay uses this method so a misspelled root cannot silently become an
    /// empty new experiment.
    pub fn open_existing(
        root: impl AsRef<Path>,
        provenance: DataProvenance,
    ) -> Result<Self, ParquetError> {
        provenance.validate()?;
        let root = root.as_ref();
        if !root.is_absolute() {
            return Err(ParquetError::InvalidRoot {
                reason: "root must be absolute",
            });
        }
        let root = validate_private_root(root)?;
        ensure_existing_private_directory(&root.join(PARTITIONS_DIRECTORY))?;
        Ok(Self { root, provenance })
    }

    /// Returns the BLAKE3 hash of the only physical Arrow schema accepted here.
    #[must_use]
    pub fn schema_hash() -> String {
        digest_bytes(b"trench.parquet.normalized-event.arrow.v1")
    }

    /// Returns the root-owned frozen provenance commitment.
    #[must_use]
    pub const fn provenance(&self) -> &DataProvenance {
        &self.provenance
    }

    /// Writes one bounded event batch as deterministic, complete partitions.
    pub fn write_events(
        &self,
        events: &[MarketEvent],
    ) -> Result<Vec<PartitionManifest>, ParquetError> {
        self.write_events_inner(events, None)
    }

    /// Writes one bounded batch and stops before publishing final directories.
    ///
    /// This exists solely to prove crash-recovery behavior; runtime code uses
    /// [`Self::write_events`].
    pub fn write_events_with_failure(
        &self,
        events: &[MarketEvent],
        failure: PartitionFailure,
    ) -> Result<Vec<PartitionManifest>, ParquetError> {
        self.write_events_inner(events, Some(failure))
    }

    fn write_events_inner(
        &self,
        events: &[MarketEvent],
        failure: Option<PartitionFailure>,
    ) -> Result<Vec<PartitionManifest>, ParquetError> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        if events.len() > MAX_EVENTS_PER_BATCH {
            return Err(ParquetError::BatchTooLarge {
                count: events.len(),
                limit: MAX_EVENTS_PER_BATCH,
            });
        }

        validate_write_events(events)?;
        let events = deduplicate_events(events)?;
        let mut partitions = BTreeMap::<PartitionKey, Vec<MarketEvent>>::new();
        for event in events {
            partitions
                .entry(PartitionKey::from_event(&event))
                .or_default()
                .push(event);
        }

        // A store is one frozen run. Read and fence the complete existing root
        // before creating a single new directory: a retry may reproduce one
        // whole partition, but no partial/superset batch may reuse its events.
        self.fence_existing_events(&partitions)?;

        partitions
            .into_iter()
            .map(|(key, events)| self.write_partition(&key, &events, failure))
            .collect()
    }

    fn fence_existing_events(
        &self,
        candidates: &BTreeMap<PartitionKey, Vec<MarketEvent>>,
    ) -> Result<(), ParquetError> {
        let manifests = self.partitions()?;
        let existing_partition_ids = manifests
            .iter()
            .map(|manifest| manifest.partition_id.clone())
            .collect::<BTreeSet<_>>();
        let mut existing_events = BTreeMap::<EventId, MarketEvent>::new();
        for manifest in &manifests {
            for event in self.read_partition(manifest)? {
                let event_id = event.event_id().clone();
                if existing_events.insert(event_id.clone(), event).is_some() {
                    return Err(ParquetError::DuplicateEvent {
                        event_id: event_id.as_str().to_owned(),
                    });
                }
            }
        }
        for (key, events) in candidates {
            let normalized = normalize_partition_events(events)?;
            let candidate =
                PartitionManifest::from_events(key, &normalized, self.provenance.clone())?;
            let exact_retry = existing_partition_ids.contains(&candidate.partition_id);
            for event in normalized {
                let Some(existing) = existing_events.get(event.event_id()) else {
                    continue;
                };
                if existing != &event {
                    return Err(ParquetError::ConflictingEvent {
                        event_id: event.event_id().as_str().to_owned(),
                    });
                }
                if !exact_retry {
                    return Err(ParquetError::DuplicateEvent {
                        event_id: event.event_id().as_str().to_owned(),
                    });
                }
            }
        }
        Ok(())
    }

    fn write_partition(
        &self,
        key: &PartitionKey,
        events: &[MarketEvent],
        failure: Option<PartitionFailure>,
    ) -> Result<PartitionManifest, ParquetError> {
        let normalized = normalize_partition_events(events)?;
        let manifest = PartitionManifest::from_events(key, &normalized, self.provenance.clone())?;
        let parent = self.partition_parent(key)?;
        let final_directory = parent.join(format!("part-{}.part", manifest.partition_id));
        let temporary_directory = parent.join(format!("part-{}.part.tmp", manifest.partition_id));

        if final_directory.exists() {
            return self.validate_existing_partition(&final_directory, &manifest);
        }
        if temporary_directory.exists() {
            return Err(ParquetError::TemporarySiblingExists {
                path: temporary_directory,
            });
        }

        fs::create_dir(&temporary_directory).map_err(|source| ParquetError::Filesystem {
            operation: "creating temporary partition directory",
            source,
        })?;
        set_private_permissions(&temporary_directory)?;
        sync_directory(&temporary_directory)?;
        sync_parent_directory(&temporary_directory)?;

        let event_path = temporary_directory.join(EVENT_FILE);
        let event_file = write_parquet(&event_path, &normalized)?;
        event_file
            .sync_all()
            .map_err(|source| ParquetError::Filesystem {
                operation: "fsyncing temporary parquet file",
                source,
            })?;
        let manifest_path = temporary_directory.join(MANIFEST_FILE);
        let manifest_file = write_manifest(&manifest_path, &manifest)?;
        manifest_file
            .sync_all()
            .map_err(|source| ParquetError::Filesystem {
                operation: "fsyncing temporary partition manifest",
                source,
            })?;
        sync_directory(&temporary_directory)?;

        let validated = read_partition_directory(
            &temporary_directory,
            &self.provenance,
            key,
            &manifest.partition_id,
        )?;
        if validated != manifest {
            return Err(ParquetError::InvalidPartition {
                reason: "temporary partition changed during validation",
            });
        }
        if failure == Some(PartitionFailure::BeforeRename) {
            return Err(ParquetError::InjectedFailure);
        }

        fs::rename(&temporary_directory, &final_directory).map_err(|source| {
            ParquetError::Filesystem {
                operation: "atomically publishing partition directory",
                source,
            }
        })?;
        sync_directory(&parent)?;
        Ok(manifest)
    }

    /// Returns only complete, validated partitions; temporary siblings are ignored.
    pub fn partitions(&self) -> Result<Vec<PartitionManifest>, ParquetError> {
        let mut manifest_ids = BTreeSet::new();
        let mut event_ids = BTreeSet::new();
        let mut manifests = Vec::new();
        let mut rows = 0_u64;
        let mut encoded_bytes = 0_u64;
        for candidate in scan_complete_partition_directories(&self.root)? {
            let manifest = read_partition_directory(
                &candidate.directory,
                &self.provenance,
                &candidate.key,
                &candidate.partition_id,
            )?;
            if !manifest_ids.insert(manifest.partition_id.clone()) {
                return Err(ParquetError::DuplicatePartition {
                    partition_id: manifest.partition_id,
                });
            }
            rows = rows
                .checked_add(manifest.row_count)
                .ok_or(ParquetError::ResourceLimit {
                    field: "discovered partition rows",
                })?;
            encoded_bytes = encoded_bytes.checked_add(manifest.encoded_bytes).ok_or(
                ParquetError::ResourceLimit {
                    field: "discovered partition normalized wire bytes",
                },
            )?;
            if rows > MAX_EVENTS_PER_BATCH as u64 || encoded_bytes > MAX_PARTITION_WIRE_BYTES as u64
            {
                return Err(ParquetError::ResourceLimit {
                    field: "discovered partition aggregate",
                });
            }
            for event in read_events_file(&candidate.directory.join(EVENT_FILE))? {
                if !event_ids.insert(event.event_id().clone()) {
                    return Err(ParquetError::DuplicateEvent {
                        event_id: event.event_id().as_str().to_owned(),
                    });
                }
            }
            manifests.push(manifest);
        }
        manifests.sort_by(|left, right| left.partition_id.cmp(&right.partition_id));
        Ok(manifests)
    }

    /// Reopens, validates, and returns the normalized facts from one manifest.
    pub fn read_partition(
        &self,
        manifest: &PartitionManifest,
    ) -> Result<Vec<MarketEvent>, ParquetError> {
        manifest.validate()?;
        if manifest.provenance != self.provenance {
            return Err(ParquetError::ProvenanceMismatch);
        }
        let path = self
            .existing_partition_parent(&manifest.key()?)?
            .join(format!("part-{}.part", manifest.partition_id));
        let actual = read_partition_directory(
            &path,
            &self.provenance,
            &manifest.key()?,
            &manifest.partition_id,
        )?;
        if &actual != manifest {
            return Err(ParquetError::ManifestMismatch {
                partition_id: manifest.partition_id.clone(),
            });
        }
        read_events_file(&path.join(EVENT_FILE))
    }

    /// Returns the managed root path for read-only replay construction.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the private managed directory for content-addressed import plans
    /// and their immutable interval manifests.
    pub fn imports_directory(&self) -> Result<PathBuf, ParquetError> {
        let imports = self.root.join(IMPORTS_DIRECTORY);
        ensure_private_directory(&imports)?;
        Ok(imports)
    }

    fn partition_parent(&self, key: &PartitionKey) -> Result<PathBuf, ParquetError> {
        let partitions = self.root.join(PARTITIONS_DIRECTORY);
        ensure_private_directory(&partitions)?;
        let date = partitions.join(format!("date={}", key.date));
        ensure_private_directory(&date)?;
        let kind = date.join(format!("kind={}", key.event_kind));
        ensure_private_directory(&kind)?;
        let market = kind.join(format!("market={}", encode_component(&key.market)));
        ensure_private_directory(&market)?;
        Ok(market)
    }

    fn existing_partition_parent(&self, key: &PartitionKey) -> Result<PathBuf, ParquetError> {
        let partitions = self.root.join(PARTITIONS_DIRECTORY);
        ensure_existing_private_directory(&partitions)?;
        let date = partitions.join(format!("date={}", key.date));
        ensure_existing_private_directory(&date)?;
        let kind = date.join(format!("kind={}", key.event_kind));
        ensure_existing_private_directory(&kind)?;
        let market = kind.join(format!("market={}", encode_component(&key.market)));
        ensure_existing_private_directory(&market)?;
        Ok(market)
    }

    fn validate_existing_partition(
        &self,
        directory: &Path,
        expected: &PartitionManifest,
    ) -> Result<PartitionManifest, ParquetError> {
        let key = expected.key()?;
        let actual =
            read_partition_directory(directory, &self.provenance, &key, &expected.partition_id)?;
        if &actual != expected {
            return Err(ParquetError::ManifestMismatch {
                partition_id: expected.partition_id.clone(),
            });
        }
        Ok(actual)
    }
}

/// A storage, manifest, Arrow, or normalized-wire validation failure.
#[derive(Debug, Error)]
pub enum ParquetError {
    /// The caller supplied a root that cannot be a deterministic local store.
    #[error("invalid parquet root: {reason}")]
    InvalidRoot { reason: &'static str },
    /// This local durable-partition layout is supported only on Unix hosts.
    #[error("atomic parquet partitions are unsupported on this platform")]
    UnsupportedPlatform,
    /// A digest did not use the exact `b3:<64 lowercase hex>` wire form.
    #[error("invalid {field}")]
    InvalidDigest { field: &'static str },
    /// The caller's schema hash does not describe this fixed Arrow schema.
    #[error("normalized-event schema hash mismatch")]
    SchemaHashMismatch { expected: String, actual: String },
    /// A batch would exceed the bounded write contract.
    #[error("event batch has {count} rows, exceeding {limit}")]
    BatchTooLarge { count: usize, limit: usize },
    /// The same normalized event identity was supplied with different facts.
    #[error("conflicting normalized event identity {event_id}")]
    ConflictingEvent { event_id: String },
    /// A temporary sibling already proves a previous interrupted attempt.
    #[error("temporary partition sibling exists at {path}")]
    TemporarySiblingExists { path: PathBuf },
    /// A complete partition manifest has an unsupported format version.
    #[error("unsupported partition manifest version {actual}")]
    UnsupportedManifestVersion { actual: u8 },
    /// A manifest or Parquet row was malformed or violated a frozen invariant.
    #[error("invalid parquet partition: {reason}")]
    InvalidPartition { reason: &'static str },
    /// A bounded decoded partition input exceeded the frozen resource ceiling.
    #[error("partition resource limit exceeded for {field}")]
    ResourceLimit { field: &'static str },
    /// A manifest's self-consistency checks failed before replay.
    #[error("invalid partition manifest: {reason}")]
    InvalidManifest { reason: &'static str },
    /// A final partition did not match the expected content-addressed manifest.
    #[error("partition manifest mismatch for {partition_id}")]
    ManifestMismatch { partition_id: String },
    /// Two physical paths named one content-addressed completed partition.
    #[error("duplicate physical partition {partition_id}")]
    DuplicatePartition { partition_id: String },
    /// Complete partitions repeated one canonical normalized event identity.
    #[error("duplicate normalized event identity {event_id}")]
    DuplicateEvent { event_id: String },
    /// A partition came from a different immutable replay commitment.
    #[error("partition provenance does not match this frozen replay")]
    ProvenanceMismatch,
    /// A test-only recovery fault was injected before the atomic publish rename.
    #[error("deterministic pre-rename partition failure injected")]
    InjectedFailure,
    /// A filesystem operation failed.
    #[error("filesystem operation failed while {operation}")]
    Filesystem {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    /// Arrow could not encode the normalized row batch.
    #[error("arrow record-batch construction failed")]
    Arrow(#[from] arrow_schema::ArrowError),
    /// Parquet could not encode or reopen a partition.
    #[error("parquet operation failed")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// A partition manifest was not bounded valid JSON.
    #[error("partition manifest JSON is invalid")]
    Json(#[from] serde_json::Error),
    /// A normalized event could not be reconstructed from the strict wire form.
    #[error("normalized event wire validation failed")]
    Event(#[from] EventError),
    /// A checked core identifier or unit rejected the normalized wire form.
    #[error("normalized event domain validation failed")]
    Domain(#[from] trench_core::domain::DomainError),
    /// A decimal string in the stored wire event was invalid.
    #[error("invalid decimal in stored event")]
    Decimal,
}

impl ParquetError {
    /// Returns whether this is the deterministic test-only pre-rename fault.
    #[must_use]
    pub const fn is_injected_failure(&self) -> bool {
        matches!(self, Self::InjectedFailure)
    }

    /// Returns whether a completed partition was rejected for foreign provenance.
    #[must_use]
    pub const fn is_provenance_mismatch(&self) -> bool {
        matches!(self, Self::ProvenanceMismatch)
    }

    /// Returns whether malformed input exceeded a frozen storage resource bound.
    #[must_use]
    pub const fn is_resource_limit(&self) -> bool {
        matches!(self, Self::ResourceLimit { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PartitionKey {
    date: String,
    event_kind: String,
    market: String,
}

impl PartitionKey {
    fn from_event(event: &MarketEvent) -> Self {
        Self {
            date: utc_day_component(event.event_time()),
            event_kind: event_kind_name(event.kind()).to_owned(),
            market: event.market().as_str().to_owned(),
        }
    }

    fn from_manifest(manifest: &PartitionManifest) -> Result<Self, ParquetError> {
        Self::new(
            manifest.date.clone(),
            manifest.event_kind.clone(),
            manifest.market.clone(),
        )
    }

    fn new(date: String, event_kind: String, market: String) -> Result<Self, ParquetError> {
        if !date.starts_with("utc-day-")
            || date[8..].parse::<u64>().is_err()
            || !matches!(
                event_kind.as_str(),
                "metadata"
                    | "asset_context"
                    | "book_snapshot"
                    | "bbo"
                    | "trade"
                    | "funding"
                    | "completed_candle"
            )
        {
            return Err(ParquetError::InvalidManifest {
                reason: "partition key is not an approved normalized-event key",
            });
        }
        Market::new(market.clone())?;
        Ok(Self {
            date,
            event_kind,
            market,
        })
    }
}

impl PartitionManifest {
    fn from_events(
        key: &PartitionKey,
        events: &[MarketEvent],
        provenance: DataProvenance,
    ) -> Result<Self, ParquetError> {
        let row_count =
            u64::try_from(events.len()).map_err(|_| ParquetError::InvalidPartition {
                reason: "event count does not fit manifest",
            })?;
        let first = events.first().ok_or(ParquetError::InvalidPartition {
            reason: "partition cannot be empty",
        })?;
        let last = events.last().ok_or(ParquetError::InvalidPartition {
            reason: "partition cannot be empty",
        })?;
        let content_digest = events_digest(events)?;
        let encoded_bytes = encoded_event_bytes(events)?;
        let mut hasher = blake3::Hasher::new_derive_key("trench.parquet.partition-id.v1");
        for value in [
            key.date.as_str(),
            key.event_kind.as_str(),
            key.market.as_str(),
            content_digest.as_str(),
            provenance.config_digest(),
            provenance.code_digest(),
            provenance.schema_hash(),
        ] {
            hasher.update(&(value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
        Ok(Self {
            version: PARTITION_SCHEMA_VERSION,
            partition_id: format!("b3:{}", hasher.finalize().to_hex()),
            date: key.date.clone(),
            event_kind: key.event_kind.clone(),
            market: key.market.clone(),
            row_count,
            encoded_bytes,
            min_event_time_ns: first.event_time().value(),
            max_event_time_ns: last.event_time().value(),
            content_digest,
            provenance,
        })
    }
}

fn normalize_partition_events(events: &[MarketEvent]) -> Result<Vec<MarketEvent>, ParquetError> {
    let mut normalized = events.to_vec();
    normalized.sort_by(replay_order);
    let Some(first) = normalized.first() else {
        return Err(ParquetError::InvalidPartition {
            reason: "partition cannot be empty",
        });
    };
    let key = PartitionKey::from_event(first);
    if normalized
        .iter()
        .any(|event| PartitionKey::from_event(event) != key)
    {
        return Err(ParquetError::InvalidPartition {
            reason: "partition batch contains more than one key",
        });
    }
    Ok(normalized)
}

fn deduplicate_events(events: &[MarketEvent]) -> Result<Vec<MarketEvent>, ParquetError> {
    let mut unique = BTreeMap::<EventId, MarketEvent>::new();
    for event in events {
        match unique.get(event.event_id()) {
            Some(previous) if previous != event => {
                return Err(ParquetError::ConflictingEvent {
                    event_id: event.event_id().as_str().to_owned(),
                });
            }
            Some(_) => {}
            None => {
                unique.insert(event.event_id().clone(), event.clone());
            }
        }
    }
    Ok(unique.into_values().collect())
}

fn write_parquet(path: &Path, events: &[MarketEvent]) -> Result<File, ParquetError> {
    let rows = events
        .iter()
        .map(StoredEvent::from_event)
        .collect::<Result<Vec<_>, _>>()?;
    let schema = event_schema();
    let event_time = Int64Array::from_iter_values(rows.iter().map(|row| row.event_time_ns));
    let received_at = Int64Array::from_iter_values(rows.iter().map(|row| row.received_at_ns));
    let event_id = StringArray::from_iter_values(rows.iter().map(|row| row.event_id.as_str()));
    let market = StringArray::from_iter_values(rows.iter().map(|row| row.market.as_str()));
    let kind = StringArray::from_iter_values(rows.iter().map(|row| row.kind.as_str()));
    let payloads = rows
        .iter()
        .map(StoredEvent::canonical_json)
        .collect::<Result<Vec<_>, _>>()?;
    let payload = StringArray::from_iter_values(payloads.iter());
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(event_time),
            Arc::new(received_at),
            Arc::new(event_id),
            Arc::new(market),
            Arc::new(kind),
            Arc::new(payload),
        ],
    )?;
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|source| ParquetError::Filesystem {
            operation: "creating temporary parquet file",
            source,
        })?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_size(MAX_EVENTS_PER_BATCH)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(properties))?;
    writer.write(&batch)?;
    let file = writer.into_inner()?;
    set_private_file_permissions(&file)?;
    Ok(file)
}

fn write_manifest(path: &Path, manifest: &PartitionManifest) -> Result<File, ParquetError> {
    let bytes = serde_json::to_vec(manifest)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|source| ParquetError::Filesystem {
            operation: "creating temporary partition manifest",
            source,
        })?;
    file.write_all(&bytes)
        .and_then(|()| file.flush())
        .map_err(|source| ParquetError::Filesystem {
            operation: "writing temporary partition manifest",
            source,
        })?;
    set_private_file_permissions(&file)?;
    Ok(file)
}

fn read_partition_directory(
    directory: &Path,
    expected_provenance: &DataProvenance,
    physical_key: &PartitionKey,
    physical_partition_id: &str,
) -> Result<PartitionManifest, ParquetError> {
    ensure_existing_private_directory(directory)?;
    let manifest_path = directory.join(MANIFEST_FILE);
    let manifest = read_manifest(&manifest_path)?;
    manifest.validate()?;
    if manifest.key()? != *physical_key || manifest.partition_id != physical_partition_id {
        return Err(ParquetError::ManifestMismatch {
            partition_id: physical_partition_id.to_owned(),
        });
    }
    if &manifest.provenance != expected_provenance {
        return Err(ParquetError::ProvenanceMismatch);
    }
    let events = read_events_file(&directory.join(EVENT_FILE))?;
    let expected =
        PartitionManifest::from_events(&manifest.key()?, &events, manifest.provenance.clone())?;
    if expected != manifest {
        return Err(ParquetError::ManifestMismatch {
            partition_id: manifest.partition_id.clone(),
        });
    }
    Ok(manifest)
}

fn read_manifest(path: &Path) -> Result<PartitionManifest, ParquetError> {
    ensure_regular_file(path)?;
    let bytes = read_bounded_file(path, MAX_MANIFEST_BYTES)?;
    serde_json::from_slice(&bytes).map_err(ParquetError::Json)
}

fn read_events_file(path: &Path) -> Result<Vec<MarketEvent>, ParquetError> {
    ensure_regular_file(path)?;
    let metadata = fs::metadata(path).map_err(|source| ParquetError::Filesystem {
        operation: "reading parquet metadata",
        source,
    })?;
    if metadata.len() > MAX_PARQUET_BYTES {
        return Err(ParquetError::InvalidPartition {
            reason: "parquet file exceeds the fixed partition byte bound",
        });
    }
    validate_parquet_footer(path, metadata.len())?;
    let file = File::open(path).map_err(|source| ParquetError::Filesystem {
        operation: "opening parquet partition",
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    validate_parquet_metadata(builder.metadata())?;
    if builder.schema().as_ref() != event_schema().as_ref() {
        return Err(ParquetError::InvalidPartition {
            reason: "parquet physical schema does not match normalized-event schema",
        });
    }
    let reader = builder.with_batch_size(REPLAY_READ_BATCH_ROWS).build()?;
    let mut events = Vec::new();
    let mut encoded_bytes = 0_u64;
    for batch in reader {
        let batch = batch?;
        if batch.num_columns() != 6 || batch.num_rows() > MAX_EVENTS_PER_BATCH {
            return Err(ParquetError::InvalidPartition {
                reason: "parquet row batch is outside the fixed schema bounds",
            });
        }
        let event_times = required_i64_column(&batch, 0, "event_time_ns")?;
        let received_at = required_i64_column(&batch, 1, "received_at_ns")?;
        let event_id = required_string_column(&batch, 2, "event_id")?;
        let market = required_string_column(&batch, 3, "market")?;
        let kind = required_string_column(&batch, 4, "kind")?;
        let payload = required_string_column(&batch, 5, "payload_json")?;
        for row in 0..batch.num_rows() {
            if payload.value(row).len() > MAX_EVENT_WIRE_BYTES {
                return Err(ParquetError::ResourceLimit {
                    field: "stored event payload bytes",
                });
            }
            encoded_bytes = encoded_bytes
                .checked_add(u64::try_from(payload.value(row).len()).map_err(|_| {
                    ParquetError::ResourceLimit {
                        field: "stored event payload bytes",
                    }
                })?)
                .ok_or(ParquetError::ResourceLimit {
                    field: "partition decoded bytes",
                })?;
            if encoded_bytes > MAX_PARTITION_WIRE_BYTES as u64 {
                return Err(ParquetError::ResourceLimit {
                    field: "partition decoded bytes",
                });
            }
            let stored: StoredEvent = serde_json::from_str(payload.value(row))?;
            if stored.event_time_ns != event_times.value(row)
                || stored.received_at_ns != received_at.value(row)
                || stored.event_id != event_id.value(row)
                || stored.market != market.value(row)
                || stored.kind != kind.value(row)
            {
                return Err(ParquetError::InvalidPartition {
                    reason: "parquet projected columns disagree with canonical event payload",
                });
            }
            let event = stored.into_event()?;
            validate_event_shape(&event)?;
            events.push(event);
            if events.len() > MAX_EVENTS_PER_BATCH {
                return Err(ParquetError::InvalidPartition {
                    reason: "parquet partition exceeds the fixed event bound",
                });
            }
        }
    }
    let events = normalize_partition_events(&events)?;
    Ok(events)
}

fn validate_parquet_footer(path: &Path, length: u64) -> Result<(), ParquetError> {
    if length < 12 {
        return Err(ParquetError::InvalidPartition {
            reason: "parquet file is too short to contain a complete footer",
        });
    }
    let mut file = File::open(path).map_err(|source| ParquetError::Filesystem {
        operation: "opening parquet footer",
        source,
    })?;
    file.seek(SeekFrom::End(-8))
        .and_then(|_| {
            let mut footer = [0_u8; 8];
            file.read_exact(&mut footer)?;
            Ok(footer)
        })
        .map_err(|source| ParquetError::Filesystem {
            operation: "reading parquet footer",
            source,
        })
        .and_then(|footer| {
            if footer[4..] != *b"PAR1" {
                return Err(ParquetError::InvalidPartition {
                    reason: "parquet footer magic is invalid",
                });
            }
            let metadata_bytes = u32::from_le_bytes([footer[0], footer[1], footer[2], footer[3]]);
            if metadata_bytes > MAX_PARQUET_METADATA_BYTES
                || u64::from(metadata_bytes).saturating_add(12) > length
            {
                return Err(ParquetError::ResourceLimit {
                    field: "parquet metadata bytes",
                });
            }
            Ok(())
        })
}

fn validate_parquet_metadata(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Result<(), ParquetError> {
    let rows = metadata.file_metadata().num_rows();
    if rows <= 0 || rows > MAX_EVENTS_PER_BATCH as i64 || metadata.row_groups().len() != 1 {
        return Err(ParquetError::ResourceLimit {
            field: "parquet row-group layout",
        });
    }
    let row_group = &metadata.row_groups()[0];
    if row_group.num_rows() != rows || row_group.num_columns() != 6 {
        return Err(ParquetError::InvalidPartition {
            reason: "parquet row group does not match the fixed normalized-event layout",
        });
    }
    if row_group.total_byte_size() <= 0
        || row_group.total_byte_size() > MAX_PARQUET_UNCOMPRESSED_BYTES
        || row_group.compressed_size() <= 0
        || row_group.compressed_size() > MAX_PARQUET_UNCOMPRESSED_BYTES
    {
        return Err(ParquetError::ResourceLimit {
            field: "parquet row-group bytes",
        });
    }
    for column in row_group.columns() {
        if column.compression() != Compression::UNCOMPRESSED
            || column.num_values() != rows
            || column.compressed_size() <= 0
            || column.uncompressed_size() <= 0
            || column.uncompressed_size() > MAX_PARQUET_UNCOMPRESSED_BYTES
        {
            return Err(ParquetError::InvalidPartition {
                reason: "parquet column layout or compression is unsupported",
            });
        }
    }
    Ok(())
}

fn validate_write_events(events: &[MarketEvent]) -> Result<(), ParquetError> {
    let mut total = 0_u64;
    for event in events {
        validate_event_shape(event)?;
        let stored = StoredEvent::from_event(event)?;
        let bytes = stored.canonical_json()?.len();
        if bytes > MAX_EVENT_WIRE_BYTES {
            return Err(ParquetError::ResourceLimit {
                field: "normalized event wire bytes",
            });
        }
        total = total
            .checked_add(
                u64::try_from(bytes).map_err(|_| ParquetError::ResourceLimit {
                    field: "normalized event wire bytes",
                })?,
            )
            .ok_or(ParquetError::ResourceLimit {
                field: "partition normalized wire bytes",
            })?;
        if total > MAX_PARTITION_WIRE_BYTES as u64 {
            return Err(ParquetError::ResourceLimit {
                field: "partition normalized wire bytes",
            });
        }
    }
    Ok(())
}

fn validate_event_shape(event: &MarketEvent) -> Result<(), ParquetError> {
    if event.market().as_str().len() > 128 || event.event_id().as_str().len() > 128 {
        return Err(ParquetError::ResourceLimit {
            field: "normalized identifier bytes",
        });
    }
    if let MarketEventKind::BookSnapshot(book) = event.kind()
        && book
            .bids()
            .len()
            .checked_add(book.asks().len())
            .is_none_or(|levels| levels > MAX_BOOK_LEVELS_PER_EVENT)
    {
        return Err(ParquetError::ResourceLimit {
            field: "book levels per event",
        });
    }
    Ok(())
}

fn encoded_event_bytes(events: &[MarketEvent]) -> Result<u64, ParquetError> {
    let mut total = 0_u64;
    for event in events {
        let bytes = StoredEvent::from_event(event)?.canonical_json()?.len();
        total = total
            .checked_add(
                u64::try_from(bytes).map_err(|_| ParquetError::ResourceLimit {
                    field: "partition normalized wire bytes",
                })?,
            )
            .ok_or(ParquetError::ResourceLimit {
                field: "partition normalized wire bytes",
            })?;
    }
    if total == 0 || total > MAX_PARTITION_WIRE_BYTES as u64 {
        return Err(ParquetError::ResourceLimit {
            field: "partition normalized wire bytes",
        });
    }
    Ok(total)
}

fn required_i64_column<'a>(
    batch: &'a RecordBatch,
    index: usize,
    _name: &'static str,
) -> Result<&'a Int64Array, ParquetError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or(ParquetError::InvalidPartition {
            reason: "parquet integer column type is invalid",
        })
}

fn required_string_column<'a>(
    batch: &'a RecordBatch,
    index: usize,
    _name: &'static str,
) -> Result<&'a StringArray, ParquetError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(ParquetError::InvalidPartition {
            reason: "parquet string column type is invalid",
        })
}

struct CompletePartitionDirectory {
    directory: PathBuf,
    key: PartitionKey,
    partition_id: String,
}

fn scan_complete_partition_directories(
    root: &Path,
) -> Result<Vec<CompletePartitionDirectory>, ParquetError> {
    let partitions = root.join(PARTITIONS_DIRECTORY);
    ensure_existing_private_directory(&partitions)?;
    let mut completed = Vec::new();
    for date in read_managed_directories(&partitions, "date=")? {
        for kind in read_managed_directories(&date, "kind=")? {
            for market in read_managed_directories(&kind, "market=")? {
                let key = partition_key_from_paths(&date, &kind, &market)?;
                for entry in fs::read_dir(&market).map_err(|source| ParquetError::Filesystem {
                    operation: "scanning partition siblings",
                    source,
                })? {
                    let entry = entry.map_err(|source| ParquetError::Filesystem {
                        operation: "reading partition sibling",
                        source,
                    })?;
                    let name = entry.file_name();
                    let Some(name) = name.to_str() else {
                        return Err(ParquetError::InvalidPartition {
                            reason: "partition sibling name is not valid UTF-8",
                        });
                    };
                    if name.ends_with(".tmp") {
                        continue;
                    }
                    if !name.starts_with("part-") || !name.ends_with(".part") {
                        continue;
                    }
                    let partition_id = name
                        .strip_prefix("part-")
                        .and_then(|name| name.strip_suffix(".part"))
                        .ok_or(ParquetError::InvalidPartition {
                            reason: "partition directory name is malformed",
                        })?
                        .to_owned();
                    validate_digest("partition directory identifier", &partition_id)?;
                    let path = entry.path();
                    ensure_existing_private_directory(&path)?;
                    if completed.len() == MAX_DISCOVERED_PARTITIONS {
                        return Err(ParquetError::ResourceLimit {
                            field: "discovered partition count",
                        });
                    }
                    completed.push(CompletePartitionDirectory {
                        directory: path,
                        key: key.clone(),
                        partition_id,
                    });
                }
            }
        }
    }
    completed.sort_by(|left, right| left.directory.cmp(&right.directory));
    Ok(completed)
}

fn partition_key_from_paths(
    date: &Path,
    kind: &Path,
    market: &Path,
) -> Result<PartitionKey, ParquetError> {
    let date = managed_component(date, "date=")?;
    let event_kind = managed_component(kind, "kind=")?;
    let encoded_market = managed_component(market, "market=")?;
    let market = decode_component(encoded_market)?;
    PartitionKey::new(date.to_owned(), event_kind.to_owned(), market)
}

fn managed_component<'a>(path: &'a Path, prefix: &str) -> Result<&'a str, ParquetError> {
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix(prefix))
        .filter(|value| !value.is_empty())
        .ok_or(ParquetError::InvalidPartition {
            reason: "managed partition path component is invalid",
        })
}

fn read_managed_directories(parent: &Path, prefix: &str) -> Result<Vec<PathBuf>, ParquetError> {
    let mut children = Vec::new();
    for entry in fs::read_dir(parent).map_err(|source| ParquetError::Filesystem {
        operation: "scanning managed partition directories",
        source,
    })? {
        let entry = entry.map_err(|source| ParquetError::Filesystem {
            operation: "reading managed partition directory",
            source,
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(ParquetError::InvalidPartition {
                reason: "partition directory name is not valid UTF-8",
            });
        };
        if !name.starts_with(prefix) {
            continue;
        }
        let path = entry.path();
        ensure_existing_private_directory(&path)?;
        children.push(path);
    }
    children.sort();
    Ok(children)
}

fn validate_private_root(path: &Path) -> Result<PathBuf, ParquetError> {
    #[cfg(not(unix))]
    {
        let _ = path;
        return Err(ParquetError::UnsupportedPlatform);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let metadata = fs::symlink_metadata(path).map_err(|source| ParquetError::Filesystem {
            operation: "inspecting parquet root",
            source,
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ParquetError::InvalidRoot {
                reason: "root must be an existing non-symlink directory",
            });
        }
        if metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o077 != 0 {
            return Err(ParquetError::InvalidRoot {
                reason: "root must be owned by the effective user and mode 0700",
            });
        }
        let canonical = fs::canonicalize(path).map_err(|source| ParquetError::Filesystem {
            operation: "canonicalizing parquet root",
            source,
        })?;
        let parent = canonical.parent().ok_or(ParquetError::InvalidRoot {
            reason: "root must have a parent directory",
        })?;
        let parent_metadata = fs::metadata(parent).map_err(|source| ParquetError::Filesystem {
            operation: "inspecting parquet root parent",
            source,
        })?;
        let parent_mode = parent_metadata.mode();
        let sticky_world_writable = parent_mode & 0o1000 != 0 && parent_mode & 0o002 != 0;
        if parent_mode & 0o022 != 0 && !sticky_world_writable {
            return Err(ParquetError::InvalidRoot {
                reason: "root parent must not permit untrusted renames",
            });
        }
        Ok(canonical)
    }
}

fn ensure_private_directory(path: &Path) -> Result<(), ParquetError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink()
                || !metadata.is_dir()
                || !private_owned_directory(&metadata)
            {
                return Err(ParquetError::InvalidRoot {
                    reason: "managed path must be an effective-user owned mode-0700 directory",
                });
            }
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(|source| ParquetError::Filesystem {
                operation: "creating managed directory",
                source,
            })?;
            set_private_permissions(path)?;
            sync_directory(path)?;
            sync_parent_directory(path)?;
        }
        Err(source) => {
            return Err(ParquetError::Filesystem {
                operation: "inspecting managed directory",
                source,
            });
        }
    }
    Ok(())
}

fn ensure_existing_private_directory(path: &Path) -> Result<(), ParquetError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| ParquetError::Filesystem {
        operation: "inspecting existing managed directory",
        source,
    })?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || !private_owned_directory(&metadata)
    {
        return Err(ParquetError::InvalidRoot {
            reason: "managed path must be an effective-user owned mode-0700 directory",
        });
    }
    Ok(())
}

#[cfg(unix)]
fn private_owned_directory(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    metadata.uid() == rustix::process::geteuid().as_raw() && metadata.mode() & 0o077 == 0
}

#[cfg(not(unix))]
fn private_owned_directory(_metadata: &fs::Metadata) -> bool {
    false
}

fn ensure_regular_file(path: &Path) -> Result<(), ParquetError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| ParquetError::Filesystem {
        operation: "inspecting partition file",
        source,
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ParquetError::InvalidPartition {
            reason: "partition file must be a regular non-symlink file",
        });
    }
    Ok(())
}

fn read_bounded_file(path: &Path, limit: u64) -> Result<Vec<u8>, ParquetError> {
    let metadata = fs::metadata(path).map_err(|source| ParquetError::Filesystem {
        operation: "reading partition file metadata",
        source,
    })?;
    if metadata.len() > limit {
        return Err(ParquetError::InvalidPartition {
            reason: "partition file exceeds its fixed byte bound",
        });
    }
    let mut file = File::open(path).map_err(|source| ParquetError::Filesystem {
        operation: "opening bounded partition file",
        source,
    })?;
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).map_err(|_| {
        ParquetError::InvalidPartition {
            reason: "partition file length does not fit memory bounds",
        }
    })?);
    file.read_to_end(&mut bytes)
        .map_err(|source| ParquetError::Filesystem {
            operation: "reading bounded partition file",
            source,
        })?;
    if u64::try_from(bytes.len()).ok() != Some(metadata.len()) {
        return Err(ParquetError::InvalidPartition {
            reason: "partition file changed while being read",
        });
    }
    Ok(bytes)
}

fn sync_directory(path: &Path) -> Result<(), ParquetError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| ParquetError::Filesystem {
            operation: "fsyncing partition directory",
            source,
        })
}

fn sync_parent_directory(path: &Path) -> Result<(), ParquetError> {
    let parent = path.parent().ok_or(ParquetError::InvalidRoot {
        reason: "managed path must have a parent directory",
    })?;
    sync_directory(parent)
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> Result<(), ParquetError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = fs::metadata(path).map_err(|source| ParquetError::Filesystem {
        operation: "reading partition permissions",
        source,
    })?;
    let mode = if metadata.is_dir() { 0o700 } else { 0o600 };
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).map_err(|source| {
        ParquetError::Filesystem {
            operation: "setting private partition permissions",
            source,
        }
    })
}

#[cfg(unix)]
fn set_private_file_permissions(file: &File) -> Result<(), ParquetError> {
    rustix::fs::fchmod(file, rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR).map_err(|source| {
        ParquetError::Filesystem {
            operation: "setting private partition file permissions",
            source: source.into(),
        }
    })
}

#[cfg(not(unix))]
fn set_private_file_permissions(_file: &File) -> Result<(), ParquetError> {
    Ok(())
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> Result<(), ParquetError> {
    Ok(())
}

fn event_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_time_ns", DataType::Int64, false),
        Field::new("received_at_ns", DataType::Int64, false),
        Field::new("event_id", DataType::Utf8, false),
        Field::new("market", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("payload_json", DataType::Utf8, false),
    ]))
}

fn event_kind_name(kind: &MarketEventKind) -> &'static str {
    match kind {
        MarketEventKind::Metadata(_) => "metadata",
        MarketEventKind::AssetContext(_) => "asset_context",
        MarketEventKind::BookSnapshot(_) => "book_snapshot",
        MarketEventKind::Bbo(_) => "bbo",
        MarketEventKind::Trade(_) => "trade",
        MarketEventKind::Funding(_) => "funding",
        MarketEventKind::CompletedCandle(_) => "completed_candle",
    }
}

pub(crate) fn replay_order(left: &MarketEvent, right: &MarketEvent) -> std::cmp::Ordering {
    left.event_time()
        .cmp(&right.event_time())
        .then_with(|| kind_order(left.kind()).cmp(&kind_order(right.kind())))
        .then_with(|| left.event_id().cmp(right.event_id()))
}

fn kind_order(kind: &MarketEventKind) -> u8 {
    match kind {
        MarketEventKind::Metadata(_) => 0,
        MarketEventKind::AssetContext(_) => 1,
        MarketEventKind::BookSnapshot(_) => 2,
        MarketEventKind::Bbo(_) => 3,
        MarketEventKind::Trade(_) => 4,
        MarketEventKind::Funding(_) => 5,
        MarketEventKind::CompletedCandle(_) => 6,
    }
}

pub(crate) fn events_digest(events: &[MarketEvent]) -> Result<String, ParquetError> {
    let mut hasher = blake3::Hasher::new_derive_key("trench.parquet.events.v1");
    for event in events {
        let row = StoredEvent::from_event(event)?;
        let canonical = row.canonical_json()?;
        hasher.update(&(canonical.len() as u64).to_be_bytes());
        hasher.update(canonical.as_bytes());
    }
    Ok(format!("b3:{}", hasher.finalize().to_hex()))
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("b3:{}", blake3::hash(bytes).to_hex())
}

fn validate_digest(field: &'static str, value: &str) -> Result<(), ParquetError> {
    let Some(hex) = value.strip_prefix("b3:") else {
        return Err(ParquetError::InvalidDigest { field });
    };
    if hex.len() != blake3::OUT_LEN * 2
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ParquetError::InvalidDigest { field });
    }
    Ok(())
}

fn utc_day_component(time: TimestampNs) -> String {
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;
    format!("utc-day-{}", time.value() / NANOS_PER_DAY)
}

fn encode_component(value: &str) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn decode_component(value: &str) -> Result<String, ParquetError> {
    if value.is_empty() || !value.len().is_multiple_of(2) || value.len() > 256 {
        return Err(ParquetError::InvalidPartition {
            reason: "encoded partition market component is invalid",
        });
    }
    let bytes = value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0])?;
            let low = hex_nibble(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect::<Result<Vec<_>, ParquetError>>()?;
    String::from_utf8(bytes).map_err(|_| ParquetError::InvalidPartition {
        reason: "encoded partition market is not UTF-8",
    })
}

fn hex_nibble(value: u8) -> Result<u8, ParquetError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(ParquetError::InvalidPartition {
            reason: "encoded partition market is not lowercase hexadecimal",
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StoredEvent {
    version: u8,
    event_id: String,
    event_time_ns: i64,
    received_at_ns: i64,
    market: String,
    kind: String,
    payload: Value,
}

impl StoredEvent {
    fn from_event(event: &MarketEvent) -> Result<Self, ParquetError> {
        let payload = match event.kind() {
            MarketEventKind::Metadata(metadata) => json!({
                "size_decimals": metadata.size_decimals(),
                "venue_max_leverage": metadata.venue_max_leverage(),
                "active": metadata.is_active(),
            }),
            MarketEventKind::AssetContext(context) => json!({
                "mark_price": context.mark_price().value().to_string(),
                "oracle_price": context.oracle_price().value().to_string(),
                "mid_price": context.mid_price().map(|price| price.value().to_string()),
                "open_interest": context.open_interest().value().to_string(),
                "day_notional_volume": context.day_notional_volume().value().to_string(),
                "funding_rate": context.funding_rate().value().to_string(),
            }),
            MarketEventKind::BookSnapshot(snapshot) => json!({
                "sequence": snapshot.sequence(),
                "bids": levels_json(snapshot.bids()),
                "asks": levels_json(snapshot.asks()),
            }),
            MarketEventKind::Bbo(bbo) => json!({
                "sequence": bbo.sequence(),
                "bid": level_json(bbo.bid()),
                "ask": level_json(bbo.ask()),
            }),
            MarketEventKind::Trade(trade) => json!({
                "trade_id": trade.trade_id(),
                "side": side_name(trade.side()),
                "price": trade.price().value().to_string(),
                "quantity": trade.quantity().value().to_string(),
            }),
            MarketEventKind::Funding(funding) => json!({
                "rate": funding.rate().value().to_string(),
                "mark_price": funding.mark_price().map(|price| price.value().to_string()),
            }),
            MarketEventKind::CompletedCandle(candle) => json!({
                "interval": interval_name(candle.interval()),
                "open_time_ns": candle.open_time().value(),
                "open": candle.open().value().to_string(),
                "high": candle.high().value().to_string(),
                "low": candle.low().value().to_string(),
                "close": candle.close().value().to_string(),
                "volume": candle.volume().value().to_string(),
                "trade_count": candle.trade_count(),
            }),
        };
        Ok(Self {
            version: PARTITION_SCHEMA_VERSION,
            event_id: event.event_id().as_str().to_owned(),
            event_time_ns: event.event_time().value(),
            received_at_ns: event.received_at().value(),
            market: event.market().as_str().to_owned(),
            kind: event_kind_name(event.kind()).to_owned(),
            payload,
        })
    }

    fn canonical_json(&self) -> Result<String, ParquetError> {
        serde_json::to_string(self).map_err(ParquetError::Json)
    }

    fn into_event(self) -> Result<MarketEvent, ParquetError> {
        if self.version != PARTITION_SCHEMA_VERSION {
            return Err(ParquetError::InvalidPartition {
                reason: "stored event has an unsupported version",
            });
        }
        let event_time = TimestampNs::new(i128::from(self.event_time_ns))?;
        let received_at = TimestampNs::new(i128::from(self.received_at_ns))?;
        let market = Market::new(self.market.clone())?;
        let event = match self.kind.as_str() {
            "metadata" => MarketEvent::metadata(
                event_time,
                received_at,
                market,
                Metadata::new(
                    required_u8(&self.payload, "size_decimals")?,
                    required_u16(&self.payload, "venue_max_leverage")?,
                    required_bool(&self.payload, "active")?,
                ),
            ),
            "asset_context" => MarketEvent::asset_context(
                event_time,
                received_at,
                market,
                AssetContext::new(
                    required_price(&self.payload, "mark_price")?,
                    required_price(&self.payload, "oracle_price")?,
                    optional_price(&self.payload, "mid_price")?,
                    required_quantity(&self.payload, "open_interest")?,
                    required_usdc(&self.payload, "day_notional_volume")?,
                    FundingRate::new(required_decimal(&self.payload, "funding_rate")?),
                ),
            ),
            "book_snapshot" => MarketEvent::book_snapshot(
                event_time,
                received_at,
                market,
                BookSnapshot::new(
                    required_u64(&self.payload, "sequence")?,
                    required_levels(&self.payload, "bids")?,
                    required_levels(&self.payload, "asks")?,
                ),
            ),
            "bbo" => MarketEvent::bbo(
                event_time,
                received_at,
                market,
                Bbo::new(
                    required_u64(&self.payload, "sequence")?,
                    required_level(&self.payload, "bid")?,
                    required_level(&self.payload, "ask")?,
                )?,
            ),
            "trade" => MarketEvent::trade(
                event_time,
                received_at,
                market,
                Trade::new(
                    required_u64(&self.payload, "trade_id")?,
                    required_side(&self.payload, "side")?,
                    required_price(&self.payload, "price")?,
                    required_quantity(&self.payload, "quantity")?,
                )?,
            ),
            "funding" => MarketEvent::funding(
                event_time,
                received_at,
                market,
                match optional_price(&self.payload, "mark_price")? {
                    Some(mark_price) => Funding::with_mark(
                        FundingRate::new(required_decimal(&self.payload, "rate")?),
                        mark_price,
                    ),
                    None => Funding::historical(FundingRate::new(required_decimal(
                        &self.payload,
                        "rate",
                    )?)),
                },
            ),
            "completed_candle" => MarketEvent::completed_candle(
                event_time,
                received_at,
                market,
                CompletedCandle::new(
                    required_interval(&self.payload, "interval")?,
                    TimestampNs::new(i128::from(required_i64(&self.payload, "open_time_ns")?))?,
                    required_price(&self.payload, "open")?,
                    required_price(&self.payload, "high")?,
                    required_price(&self.payload, "low")?,
                    required_price(&self.payload, "close")?,
                    required_quantity(&self.payload, "volume")?,
                    required_u64(&self.payload, "trade_count")?,
                )?,
            ),
            _ => {
                return Err(ParquetError::InvalidPartition {
                    reason: "stored event kind is unsupported",
                });
            }
        }?;
        if event.event_id().as_str() != self.event_id {
            return Err(ParquetError::InvalidPartition {
                reason: "stored event identifier disagrees with canonical event identity",
            });
        }
        Ok(event)
    }
}

fn level_json(level: BookLevel) -> Value {
    json!({
        "price": level.price().value().to_string(),
        "quantity": level.quantity().value().to_string(),
    })
}

fn levels_json(levels: &[BookLevel]) -> Vec<Value> {
    levels.iter().copied().map(level_json).collect()
}

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

fn interval_name(interval: CandleInterval) -> &'static str {
    match interval {
        CandleInterval::FifteenMinutes => "15m",
        CandleInterval::OneHour => "1h",
    }
}

fn required_value<'a>(payload: &'a Value, field: &'static str) -> Result<&'a Value, ParquetError> {
    payload.get(field).ok_or(ParquetError::InvalidPartition {
        reason: "stored event payload is missing a required field",
    })
}

fn required_string<'a>(payload: &'a Value, field: &'static str) -> Result<&'a str, ParquetError> {
    required_value(payload, field)?
        .as_str()
        .ok_or(ParquetError::InvalidPartition {
            reason: "stored event payload field has an invalid type",
        })
}

fn required_decimal(payload: &Value, field: &'static str) -> Result<Decimal, ParquetError> {
    Decimal::from_str(required_string(payload, field)?).map_err(|_| ParquetError::Decimal)
}

fn required_price(payload: &Value, field: &'static str) -> Result<Price, ParquetError> {
    Price::new(required_decimal(payload, field)?).map_err(ParquetError::Domain)
}

fn optional_price(payload: &Value, field: &'static str) -> Result<Option<Price>, ParquetError> {
    match required_value(payload, field)? {
        Value::Null => Ok(None),
        Value::String(value) => {
            Price::new(Decimal::from_str(value).map_err(|_| ParquetError::Decimal)?)
                .map(Some)
                .map_err(ParquetError::Domain)
        }
        _ => Err(ParquetError::InvalidPartition {
            reason: "stored optional price has an invalid type",
        }),
    }
}

fn required_quantity(payload: &Value, field: &'static str) -> Result<Quantity, ParquetError> {
    Quantity::new(required_decimal(payload, field)?).map_err(ParquetError::Domain)
}

fn required_usdc(payload: &Value, field: &'static str) -> Result<Usdc, ParquetError> {
    Usdc::new(required_decimal(payload, field)?).map_err(ParquetError::Domain)
}

fn required_u64(payload: &Value, field: &'static str) -> Result<u64, ParquetError> {
    required_value(payload, field)?
        .as_u64()
        .ok_or(ParquetError::InvalidPartition {
            reason: "stored event integer is invalid",
        })
}

fn required_u16(payload: &Value, field: &'static str) -> Result<u16, ParquetError> {
    u16::try_from(required_u64(payload, field)?).map_err(|_| ParquetError::InvalidPartition {
        reason: "stored event integer is outside its field range",
    })
}

fn required_u8(payload: &Value, field: &'static str) -> Result<u8, ParquetError> {
    u8::try_from(required_u64(payload, field)?).map_err(|_| ParquetError::InvalidPartition {
        reason: "stored event integer is outside its field range",
    })
}

fn required_i64(payload: &Value, field: &'static str) -> Result<i64, ParquetError> {
    required_value(payload, field)?
        .as_i64()
        .ok_or(ParquetError::InvalidPartition {
            reason: "stored event signed integer is invalid",
        })
}

fn required_bool(payload: &Value, field: &'static str) -> Result<bool, ParquetError> {
    required_value(payload, field)?
        .as_bool()
        .ok_or(ParquetError::InvalidPartition {
            reason: "stored event boolean is invalid",
        })
}

fn required_side(payload: &Value, field: &'static str) -> Result<Side, ParquetError> {
    match required_string(payload, field)? {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        _ => Err(ParquetError::InvalidPartition {
            reason: "stored event side is invalid",
        }),
    }
}

fn required_interval(payload: &Value, field: &'static str) -> Result<CandleInterval, ParquetError> {
    match required_string(payload, field)? {
        "15m" => Ok(CandleInterval::FifteenMinutes),
        "1h" => Ok(CandleInterval::OneHour),
        _ => Err(ParquetError::InvalidPartition {
            reason: "stored candle interval is invalid",
        }),
    }
}

fn required_level(payload: &Value, field: &'static str) -> Result<BookLevel, ParquetError> {
    let level = required_value(payload, field)?;
    Ok(BookLevel::new(
        required_price(level, "price")?,
        required_quantity(level, "quantity")?,
    ))
}

fn required_levels(payload: &Value, field: &'static str) -> Result<Vec<BookLevel>, ParquetError> {
    required_value(payload, field)?
        .as_array()
        .ok_or(ParquetError::InvalidPartition {
            reason: "stored book levels are invalid",
        })?
        .iter()
        .map(|level| {
            Ok(BookLevel::new(
                required_price(level, "price")?,
                required_quantity(level, "quantity")?,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{DataProvenance, ParquetStore};

    #[test]
    fn fixed_arrow_schema_hash_is_a_valid_blake3_commitment() {
        let hash = ParquetStore::schema_hash();
        assert!(DataProvenance::new(hash.clone(), hash.clone(), hash).is_ok());
    }
}
