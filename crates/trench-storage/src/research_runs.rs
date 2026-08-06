//! Bounded, externally merged availability runs for offline research.

#![cfg_attr(not(unix), allow(dead_code))]

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Arc, Mutex},
};

#[cfg(unix)]
use std::{fs, path::Component};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

#[cfg(unix)]
use rustix::fs::{
    AtFlags, FileType, Mode, OFlags, RenameFlags, fstat, mkdirat, open, openat, renameat_with,
    unlinkat,
};

use blake3::Hasher;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use trench_core::{
    domain::EventId,
    event::{MarketEvent, TimestampNs},
};

use crate::{
    parquet::{
        DataProvenance, ParquetError, ParquetStore, canonical_event_wire, event_from_canonical_wire,
    },
    recovery_outcomes::{
        MAX_TOTAL_RECOVERY_PROOF_REFERENCES, ReconciledRecoveryOutcome, RecoveryOutcomeStore,
        RecoverySourceReference,
    },
    research_plan::{
        ResearchMemberLocator, ResearchPlanError, ResearchSourcePlanDraft, ResearchSourcePlanWire,
    },
};

const RUN_MAGIC: [u8; 8] = *b"TRNCRUN1";
const RUN_VERSION: u8 = 1;
const DIGEST_BYTES: usize = 67;
const MAX_RUN_RECORDS: u64 = 1_000_000;
const MAX_RUN_WIRE_BYTES: u64 = 512 * 1_024 * 1_024;
const MAX_RUN_HEADER_BYTES: usize = 16 * 1_024;
const MAX_RUN_COUNT: usize = 4_096;
const MAX_MERGE_PASSES: u16 = 2;
const MAX_RECORD_WIRE_BYTES: usize = 96 * 1_024;
const MAX_EVENT_WIRE_BYTES: usize = 64 * 1_024;
const MAX_PLAN_METADATA_BYTES: u64 = 1_048_576;
const PLAN_MANIFEST_FILE: &str = "research-plan.json";
const FINAL_RUN_FILE: &str = "availability.run";

/// The fixed maximum fan-in for every external availability merge.
pub const MAX_RUN_MERGE_INPUTS: usize = 64;

/// Canonical source availability order. Event kind deliberately is not a tie-breaker.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AvailabilityKey {
    received_at: TimestampNs,
    event_time: TimestampNs,
    event_id: EventId,
}

impl AvailabilityKey {
    /// Builds one checked source availability coordinate.
    pub fn new(
        received_at: TimestampNs,
        event_time: TimestampNs,
        event_id: EventId,
    ) -> Result<Self, ResearchRunError> {
        if event_time > received_at {
            return Err(ResearchRunError::InvalidRun {
                reason: "source event time is later than its receipt time",
            });
        }
        Ok(Self {
            received_at,
            event_time,
            event_id,
        })
    }

    /// Returns the local receipt time that controls source availability.
    #[must_use]
    pub const fn received_at(&self) -> TimestampNs {
        self.received_at
    }

    /// Returns the authoritative source time.
    #[must_use]
    pub const fn event_time(&self) -> TimestampNs {
        self.event_time
    }

    /// Returns the canonical normalized event identity.
    #[must_use]
    pub const fn event_id(&self) -> &EventId {
        &self.event_id
    }
}

/// One availability-ordered normalized event with its immutable source witness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailabilityRecord {
    event: MarketEvent,
    member_ordinal: u32,
    member_manifest_digest: String,
    member_set_digest: String,
}

impl AvailabilityRecord {
    /// Creates a record from a directly revalidated normalized event.
    pub(crate) fn new(
        event: MarketEvent,
        member_ordinal: u32,
        member_manifest_digest: String,
        member_set_digest: String,
    ) -> Result<Self, ResearchRunError> {
        let record = Self {
            event,
            member_ordinal,
            member_manifest_digest,
            member_set_digest,
        };
        record.validate()?;
        Ok(record)
    }

    /// Returns the canonical availability key.
    #[must_use]
    pub fn key(&self) -> AvailabilityKey {
        AvailabilityKey {
            received_at: self.event.received_at(),
            event_time: self.event.event_time(),
            event_id: self.event.event_id().clone(),
        }
    }

    /// Returns the normalized source event.
    #[must_use]
    pub const fn event(&self) -> &MarketEvent {
        &self.event
    }

    /// Returns the source member's canonical sorted ordinal.
    #[must_use]
    pub const fn member_ordinal(&self) -> u32 {
        self.member_ordinal
    }

    /// Returns the manifest that committed this exact source event.
    #[must_use]
    pub fn member_manifest_digest(&self) -> &str {
        &self.member_manifest_digest
    }

    /// Returns the pre-run source-member commitment carried end-to-end.
    #[must_use]
    pub fn member_set_digest(&self) -> &str {
        &self.member_set_digest
    }

    fn validate(&self) -> Result<(), ResearchRunError> {
        validate_digest(&self.member_manifest_digest)?;
        validate_digest(&self.member_set_digest)?;
        validate_digest(self.event.event_id().as_str())
    }
}

/// One externally supplied record used to independently reproduce a run digest.
#[derive(Debug, Clone, Copy)]
pub struct AvailabilityDigestRecord<'a> {
    event: &'a MarketEvent,
    member_ordinal: u32,
    member_manifest_digest: &'a str,
    member_set_digest: &'a str,
}

impl<'a> AvailabilityDigestRecord<'a> {
    /// Creates one canonical record input. Inputs must be strictly availability ordered.
    #[must_use]
    pub const fn new(
        event: &'a MarketEvent,
        member_ordinal: u32,
        member_manifest_digest: &'a str,
        member_set_digest: &'a str,
    ) -> Self {
        Self {
            event,
            member_ordinal,
            member_manifest_digest,
            member_set_digest,
        }
    }
}

/// The independently reproducible digest and count of an availability-ordered run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailabilityRunDigest {
    record_count: u64,
    digest: String,
}

impl AvailabilityRunDigest {
    /// Returns the canonical number of hashed records.
    #[must_use]
    pub const fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Returns the BLAKE3 digest over encoded availability records.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }
}

/// Independently computes the exact digest of a strictly availability-ordered run.
///
/// This consumes its input in one bounded streaming pass and does not open run files.
pub fn availability_run_digest<'a>(
    records: impl IntoIterator<Item = AvailabilityDigestRecord<'a>>,
) -> Result<AvailabilityRunDigest, ResearchRunError> {
    let mut hasher = Hasher::new_derive_key("trench.research.availability-run.v1");
    let mut previous = None;
    let mut record_count = 0_u64;
    for input in records {
        let record = AvailabilityRecord::new(
            input.event.clone(),
            input.member_ordinal,
            input.member_manifest_digest.to_owned(),
            input.member_set_digest.to_owned(),
        )?;
        let key = record.key();
        if previous
            .as_ref()
            .is_some_and(|previous: &AvailabilityKey| previous >= &key)
        {
            return Err(ResearchRunError::NonMonotonicAvailability);
        }
        let encoded = encode_record(&record)?;
        hasher.update(&(encoded.len() as u32).to_be_bytes());
        hasher.update(&encoded);
        previous = Some(key);
        record_count = record_count
            .checked_add(1)
            .ok_or(ResearchRunError::ResourceLimit)?;
        if record_count > MAX_RUN_RECORDS {
            return Err(ResearchRunError::ResourceLimit);
        }
    }
    if record_count == 0 {
        return Err(ResearchRunError::InvalidRun {
            reason: "availability runs cannot be empty",
        });
    }
    Ok(AvailabilityRunDigest {
        record_count,
        digest: format!("b3:{}", hasher.finalize().to_hex()),
    })
}

/// A reopened, validated immutable run file.
#[derive(Debug, Clone)]
pub struct AvailabilityRun {
    directory: Option<Arc<File>>,
    file_name: Option<String>,
    verified_file: Option<Arc<Mutex<Option<File>>>>,
    digest: String,
    record_count: u64,
    member_set_digest: String,
    member_manifest_digests: Option<Vec<String>>,
}

impl AvailabilityRun {
    /// Opens one staged run through the private directory descriptor that owns it.
    fn open_staged_at(
        directory: Arc<File>,
        file_name: String,
        expected_member_set_digest: &str,
    ) -> Result<Self, ResearchRunError> {
        let header = validate_run_at(&directory, &file_name, expected_member_set_digest, None)?;
        Ok(Self {
            directory: Some(directory),
            file_name: Some(file_name),
            verified_file: None,
            digest: header.output_digest,
            record_count: header.record_count,
            member_set_digest: header.member_set_digest,
            member_manifest_digests: None,
        })
    }

    fn open_bound_at(
        directory: Arc<File>,
        expected_member_set_digest: &str,
        members: &[ResearchMemberLocator],
    ) -> Result<Self, ResearchRunError> {
        let member_manifest_digests = member_manifest_digests(members)?;
        let file = open_private_regular_file_at(&directory, FINAL_RUN_FILE, MAX_RUN_WIRE_BYTES)?;
        let (header, file) = validate_run_file(
            file,
            expected_member_set_digest,
            Some(&member_manifest_digests),
        )?;
        Ok(Self {
            directory: None,
            file_name: None,
            verified_file: Some(Arc::new(Mutex::new(Some(file)))),
            digest: header.output_digest,
            record_count: header.record_count,
            member_set_digest: header.member_set_digest,
            member_manifest_digests: Some(member_manifest_digests),
        })
    }

    /// Returns the digest over the canonical immutable run records.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Returns the validated record count without materializing the run.
    #[must_use]
    pub const fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Returns the commitment carried by every record in this run.
    #[must_use]
    pub fn member_set_digest(&self) -> &str {
        &self.member_set_digest
    }

    /// Consumes the one-shot, descriptor-bound cursor for this immutable run.
    ///
    /// Published runs retain the exact file descriptor revalidated at open time;
    /// a second cursor is rejected rather than reopening a mutable pathname.
    #[must_use]
    pub fn records(&self) -> AvailabilityRecords {
        let reader = match (&self.verified_file, &self.directory, &self.file_name) {
            (Some(file), None, None) => take_verified_reader(
                file,
                &self.member_set_digest,
                self.member_manifest_digests.as_deref(),
            ),
            (None, Some(directory), Some(file_name)) => RunReader::open_at(
                directory,
                file_name,
                &self.member_set_digest,
                self.member_manifest_digests.as_deref(),
            ),
            _ => Err(ResearchRunError::InvalidRun {
                reason: "availability run location is invalid",
            }),
        };
        match reader {
            Ok(reader) => AvailabilityRecords {
                reader: Some(reader),
                pending_error: None,
            },
            Err(error) => AvailabilityRecords {
                reader: None,
                pending_error: Some(error),
            },
        }
    }

    fn inspect_verified_records<F>(&self, inspect: F) -> Result<(), ResearchRunError>
    where
        F: FnOnce(&mut RunReader) -> Result<(), ResearchRunError>,
    {
        let file = self
            .verified_file
            .as_ref()
            .ok_or(ResearchRunError::InvalidRun {
                reason: "final availability run must retain its verified file descriptor",
            })?;
        let mut guard = file.lock().map_err(|_| ResearchRunError::InvalidRun {
            reason: "final availability run descriptor lock is poisoned",
        })?;
        let file = guard.take().ok_or(ResearchRunError::InvalidRun {
            reason: "final availability run cursor is already active or consumed",
        })?;
        let mut reader = RunReader::from_file(
            file,
            &self.member_set_digest,
            self.member_manifest_digests.as_deref(),
        )?;
        let result = inspect(&mut reader).and_then(|()| {
            if reader.next_record()?.is_some() {
                return Err(ResearchRunError::InvalidRun {
                    reason: "final availability run inspection did not consume every record",
                });
            }
            Ok(())
        });
        let rewind = reader.rewind();
        match rewind {
            Ok(file) => {
                *guard = Some(file);
                result
            }
            Err(error) => Err(error),
        }
    }

    fn validate_event_ids(&self, event_ids: &[EventId]) -> Result<(), ResearchRunError> {
        if event_ids.len() > usize::try_from(MAX_RUN_RECORDS).unwrap_or(usize::MAX) {
            return Err(ResearchRunError::ResourceLimit);
        }
        let mut missing = event_ids.iter().collect::<BTreeSet<_>>();
        if missing.is_empty() {
            return Ok(());
        }
        self.inspect_verified_records(|reader| {
            while let Some(record) = reader.next_record()? {
                let key = record.key();
                missing.remove(key.event_id());
            }
            if missing.is_empty() {
                Ok(())
            } else {
                Err(ResearchRunError::InvalidPlan {
                    reason: "witness event identifier is absent from final availability run",
                })
            }
        })
    }

    fn staged_file_name(&self) -> Result<&str, ResearchRunError> {
        self.file_name
            .as_deref()
            .ok_or(ResearchRunError::InvalidRun {
                reason: "run is not a staged descriptor-backed file",
            })
    }

    fn staged_directory(&self) -> Result<&Arc<File>, ResearchRunError> {
        self.directory.as_ref().ok_or(ResearchRunError::InvalidRun {
            reason: "run is not a staged descriptor-backed file",
        })
    }

    fn validate_staged(
        &self,
        expected_member_set_digest: &str,
    ) -> Result<RunHeader, ResearchRunError> {
        validate_run_at(
            self.staged_directory()?,
            self.staged_file_name()?,
            expected_member_set_digest,
            self.member_manifest_digests.as_deref(),
        )
    }

    fn staged_reader(&self) -> Result<RunReader, ResearchRunError> {
        RunReader::open_at(
            self.staged_directory()?,
            self.staged_file_name()?,
            &self.member_set_digest,
            self.member_manifest_digests.as_deref(),
        )
    }
}

/// A streaming result iterator over one immutable availability run.
pub struct AvailabilityRecords {
    reader: Option<RunReader>,
    pending_error: Option<ResearchRunError>,
}

/// Namespace for opening an atomically published immutable research source plan.
pub struct ResearchSourcePlan;

/// A final source plan that has revalidated all source members and its final availability run.
#[derive(Debug, Clone)]
pub struct VerifiedResearchSourcePlan {
    draft: ResearchSourcePlanDraft,
    availability_run: AvailabilityRun,
    recovery_outcomes: Vec<ReconciledRecoveryOutcome>,
    source_plan_digest: String,
    merge_passes: u16,
}

impl VerifiedResearchSourcePlan {
    /// Returns the revalidated immutable Task-2 source selection and coverage proof set.
    #[must_use]
    pub const fn draft(&self) -> &ResearchSourcePlanDraft {
        &self.draft
    }

    /// Returns the only availability-ordered raw source cursor admitted to later compiler tasks.
    #[must_use]
    pub const fn availability_run(&self) -> &AvailabilityRun {
        &self.availability_run
    }

    /// Returns descriptor-revalidated recovery companion outcomes bound to this exact source plan.
    #[must_use]
    pub fn recovery_outcomes(&self) -> &[ReconciledRecoveryOutcome] {
        &self.recovery_outcomes
    }

    /// Returns the final digest computed after the verified final availability run exists.
    #[must_use]
    pub fn source_plan_digest(&self) -> &str {
        &self.source_plan_digest
    }

    /// Returns the number of bounded external merge passes used to construct the final run.
    #[must_use]
    pub const fn merge_passes(&self) -> u16 {
        self.merge_passes
    }

    /// Returns the plan's frozen data provenance.
    #[must_use]
    pub fn provenance(&self) -> &DataProvenance {
        self.draft.provenance()
    }

    /// Descriptor-safely verifies raw-witness event identities against the immutable availability
    /// run without consuming the compiler cursor.
    ///
    /// This is an interim ID-only binding. Witness formats will carry complete member/key
    /// references once every producer can emit them.
    pub fn validate_event_ids(&self, event_ids: &[EventId]) -> Result<(), ResearchRunError> {
        self.availability_run.validate_event_ids(event_ids)
    }
}

impl ResearchSourcePlan {
    /// Opens a published plan only after root-bound source revalidation and final-run verification.
    pub fn open_from(
        store: &ParquetStore,
        final_directory: impl AsRef<Path>,
    ) -> Result<VerifiedResearchSourcePlan, ResearchRunError> {
        let directory = final_directory.as_ref();
        if !directory.is_absolute() {
            return Err(ResearchRunError::InvalidPlan {
                reason: "final plan directory must be absolute",
            });
        }
        let directory = Arc::new(open_private_directory_descriptor(directory)?);
        Self::open_from_directory(store, directory)
    }

    fn open_from_directory(
        store: &ParquetStore,
        directory: Arc<File>,
    ) -> Result<VerifiedResearchSourcePlan, ResearchRunError> {
        require_exact_final_entries_at(&directory)?;
        let bytes = read_private_file_at(&directory, PLAN_MANIFEST_FILE, MAX_PLAN_METADATA_BYTES)?;
        let manifest = parse_final_manifest(&bytes)?;
        let draft = ResearchSourcePlanDraft::from_wire(store, manifest.plan.clone())?;
        let run =
            AvailabilityRun::open_bound_at(directory, draft.member_set_digest(), draft.members())?;
        verify_final_manifest(&manifest, &draft, &run)?;
        let recovery_outcomes = verify_final_run_source_union(store, &draft, &run)?;
        Ok(VerifiedResearchSourcePlan {
            draft,
            availability_run: run,
            recovery_outcomes,
            source_plan_digest: manifest.source_plan_digest,
            merge_passes: manifest.merge_passes,
        })
    }
}

impl ResearchSourcePlanDraft {
    /// Builds, validates, fsyncs, and atomically publishes a final availability-backed source plan.
    pub fn publish_to(
        &self,
        store: &ParquetStore,
        final_directory: impl AsRef<Path>,
    ) -> Result<VerifiedResearchSourcePlan, ResearchRunError> {
        publish_draft(self, store, final_directory.as_ref())
    }
}

impl Iterator for AvailabilityRecords {
    type Item = Result<AvailabilityRecord, ResearchRunError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(error) = self.pending_error.take() {
            return Some(Err(error));
        }
        let reader = self.reader.as_mut()?;
        match reader.next_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(error) => {
                self.reader = None;
                Some(Err(error))
            }
        }
    }
}

/// Writes one bounded per-member initial run. The caller must have directly revalidated rows.
#[cfg(all(test, unix))]
pub(crate) fn write_initial_run(
    path: impl AsRef<Path>,
    member_ordinal: u32,
    member_manifest_digest: String,
    member_set_digest: String,
    events: Vec<MarketEvent>,
) -> Result<AvailabilityRun, ResearchRunError> {
    let path = path.as_ref();
    let parent = path.parent().ok_or(ResearchRunError::InvalidRun {
        reason: "run path requires a parent directory",
    })?;
    let file_name = private_file_name(path)?;
    let directory = Arc::new(open_private_directory_descriptor(parent)?);
    write_initial_run_at(
        directory,
        file_name,
        member_ordinal,
        member_manifest_digest,
        member_set_digest,
        events,
    )
}

fn write_initial_run_at(
    directory: Arc<File>,
    file_name: String,
    member_ordinal: u32,
    member_manifest_digest: String,
    member_set_digest: String,
    events: Vec<MarketEvent>,
) -> Result<AvailabilityRun, ResearchRunError> {
    if events.is_empty() || events.len() > usize::try_from(MAX_RUN_RECORDS).unwrap_or(usize::MAX) {
        return Err(ResearchRunError::ResourceLimit);
    }
    let mut records = events
        .into_iter()
        .map(|event| {
            AvailabilityRecord::new(
                event,
                member_ordinal,
                member_manifest_digest.clone(),
                member_set_digest.clone(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    records.sort_by_key(AvailabilityRecord::key);
    let mut iterator = records.into_iter();
    write_run_at(&directory, &file_name, &member_set_digest, &[], move || {
        Ok(iterator.next())
    })
}

/// Merges between two and 64 already validated runs without global event materialization.
#[cfg(all(test, unix))]
pub(crate) fn merge_runs(
    output: impl AsRef<Path>,
    inputs: &[AvailabilityRun],
    expected_member_set_digest: &str,
) -> Result<AvailabilityRun, ResearchRunError> {
    let output = output.as_ref();
    let parent = output.parent().ok_or(ResearchRunError::InvalidRun {
        reason: "run path requires a parent directory",
    })?;
    let output_name = private_file_name(output)?;
    let directory = Arc::new(open_private_directory_descriptor(parent)?);
    merge_runs_at(directory, output_name, inputs, expected_member_set_digest)
}

fn merge_runs_at(
    directory: Arc<File>,
    output_name: String,
    inputs: &[AvailabilityRun],
    expected_member_set_digest: &str,
) -> Result<AvailabilityRun, ResearchRunError> {
    if !(2..=MAX_RUN_MERGE_INPUTS).contains(&inputs.len()) {
        return Err(ResearchRunError::InvalidRun {
            reason: "external merge fan-in is outside the fixed range",
        });
    }
    let input_digests = inputs
        .iter()
        .map(|run| {
            if run.member_set_digest != expected_member_set_digest {
                Err(ResearchRunError::InvalidRun {
                    reason: "input run carries another member-set commitment",
                })
            } else {
                let header = run.validate_staged(expected_member_set_digest)?;
                if header.output_digest != run.digest {
                    return Err(ResearchRunError::InvalidRun {
                        reason: "input run digest changed after it was opened",
                    });
                }
                Ok(header.output_digest)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut merger = MergeCursor::new(inputs)?;
    write_run_at(
        &directory,
        &output_name,
        expected_member_set_digest,
        &input_digests,
        move || merger.next_record(),
    )
}

fn take_verified_reader(
    file: &Arc<Mutex<Option<File>>>,
    expected_member_set_digest: &str,
    member_manifest_digests: Option<&[String]>,
) -> Result<RunReader, ResearchRunError> {
    let mut guard = file.lock().map_err(|_| ResearchRunError::InvalidRun {
        reason: "final availability run descriptor lock is poisoned",
    })?;
    let file = guard.take().ok_or(ResearchRunError::InvalidRun {
        reason: "final availability run cursor was already consumed",
    })?;
    RunReader::from_file(file, expected_member_set_digest, member_manifest_digests)
}

#[cfg(unix)]
fn publish_draft(
    draft: &ResearchSourcePlanDraft,
    store: &ParquetStore,
    final_directory: &Path,
) -> Result<VerifiedResearchSourcePlan, ResearchRunError> {
    if !final_directory.is_absolute() {
        return Err(ResearchRunError::InvalidPlan {
            reason: "final plan directory must be absolute",
        });
    }
    let parent = final_directory
        .parent()
        .ok_or(ResearchRunError::InvalidPlan {
            reason: "final plan directory requires a private parent",
        })?;
    let parent_descriptor = open_private_directory_descriptor(parent)?;
    let final_name = final_directory
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty() && *name != "." && *name != "..")
        .ok_or(ResearchRunError::InvalidPlan {
            reason: "final plan directory name is invalid",
        })?;
    let stage_name = format!(".{final_name}.stage-{}", &draft.member_set_digest()[3..19]);
    mkdirat(
        &parent_descriptor,
        &stage_name,
        Mode::RUSR | Mode::WUSR | Mode::XUSR,
    )
    .map_err(|source| ResearchRunError::Io(source.into()))?;
    sync_directory_descriptor(&parent_descriptor)?;
    let stage_descriptor = Arc::new(open_private_directory_at(&parent_descriptor, &stage_name)?);
    let result = (|| {
        mkdirat(
            &stage_descriptor,
            "runs",
            Mode::RUSR | Mode::WUSR | Mode::XUSR,
        )
        .map_err(|source| ResearchRunError::Io(source.into()))?;
        sync_directory_descriptor(&stage_descriptor)?;
        let runs_directory = Arc::new(open_private_directory_at(&stage_descriptor, "runs")?);
        let mut active = build_initial_runs(draft, store, runs_directory.clone())?;
        let mut merge_passes = 0_u16;
        let mut pass = 1_usize;
        while active.len() > 1 {
            let mut next = Vec::with_capacity(active.len().div_ceil(MAX_RUN_MERGE_INPUTS));
            for (chunk_index, chunk) in active.chunks(MAX_RUN_MERGE_INPUTS).enumerate() {
                if chunk.len() == 1 {
                    next.push(chunk[0].clone());
                    continue;
                }
                let output_name = format!("run-{pass}-{chunk_index}.bin");
                let merged = merge_runs_at(
                    runs_directory.clone(),
                    output_name,
                    chunk,
                    draft.member_set_digest(),
                )?;
                for input in chunk {
                    unlinkat(&runs_directory, input.staged_file_name()?, AtFlags::empty())
                        .map_err(|source| ResearchRunError::Io(source.into()))?;
                }
                next.push(merged);
            }
            active = next;
            merge_passes = merge_passes
                .checked_add(1)
                .ok_or(ResearchRunError::ResourceLimit)?;
            pass = pass.checked_add(1).ok_or(ResearchRunError::ResourceLimit)?;
        }
        let final_run = active.pop().ok_or(ResearchRunError::InvalidPlan {
            reason: "source plans require at least one final availability run",
        })?;
        renameat_with(
            final_run.staged_directory()?,
            final_run.staged_file_name()?,
            &stage_descriptor,
            FINAL_RUN_FILE,
            RenameFlags::NOREPLACE,
        )
        .map_err(|source| ResearchRunError::Io(source.into()))?;
        unlinkat(&stage_descriptor, "runs", AtFlags::REMOVEDIR)
            .map_err(|source| ResearchRunError::Io(source.into()))?;
        sync_directory_descriptor(&stage_descriptor)?;
        let final_run = AvailabilityRun::open_staged_at(
            stage_descriptor.clone(),
            FINAL_RUN_FILE.to_owned(),
            draft.member_set_digest(),
        )?;
        let manifest = FinalPlanManifest::new(draft.wire(), &final_run, merge_passes)?;
        write_final_manifest_at(&stage_descriptor, PLAN_MANIFEST_FILE, &manifest)?;
        sync_directory_descriptor(&stage_descriptor)?;
        let _staged = ResearchSourcePlan::open_from_directory(store, stage_descriptor.clone())?;
        match renameat_with(
            &parent_descriptor,
            &stage_name,
            &parent_descriptor,
            final_name,
            RenameFlags::NOREPLACE,
        ) {
            Ok(()) => {
                sync_directory_descriptor(&parent_descriptor)?;
                ResearchSourcePlan::open_from(store, final_directory)
            }
            Err(source) if source == rustix::io::Errno::EXIST => {
                let existing = open_private_directory_at(&parent_descriptor, final_name)?;
                if !final_directories_are_identical_at(&stage_descriptor, &existing)? {
                    return Err(ResearchRunError::ConflictingFinalPlan);
                }
                remove_private_tree_at(&parent_descriptor, &stage_name)?;
                sync_directory_descriptor(&parent_descriptor)?;
                ResearchSourcePlan::open_from(store, final_directory)
            }
            Err(source) => Err(ResearchRunError::Io(source.into())),
        }
    })();
    if result.is_err() {
        let _ = remove_private_tree_at(&parent_descriptor, &stage_name);
        let _ = sync_directory_descriptor(&parent_descriptor);
    }
    result
}

#[cfg(not(unix))]
fn publish_draft(
    _draft: &ResearchSourcePlanDraft,
    _store: &ParquetStore,
    _final_directory: &Path,
) -> Result<VerifiedResearchSourcePlan, ResearchRunError> {
    Err(ResearchRunError::UnsupportedPlatform)
}

fn build_initial_runs(
    draft: &ResearchSourcePlanDraft,
    store: &ParquetStore,
    runs_directory: Arc<File>,
) -> Result<Vec<AvailabilityRun>, ResearchRunError> {
    if draft.members().is_empty() || draft.members().len() > MAX_RUN_COUNT {
        return Err(ResearchRunError::ResourceLimit);
    }
    let mut runs = Vec::with_capacity(draft.members().len());
    for (ordinal, locator) in draft.members().iter().enumerate() {
        let ordinal = u32::try_from(ordinal).map_err(|_| ResearchRunError::ResourceLimit)?;
        let opened = locator.open(store)?;
        if opened.manifest().manifest_digest() != locator.partition_manifest_digest() {
            return Err(ResearchRunError::InvalidPlan {
                reason: "direct source member digest no longer matches its locator",
            });
        }
        let output_name = format!("run-0-{ordinal}.bin");
        runs.push(write_initial_run_at(
            runs_directory.clone(),
            output_name,
            ordinal,
            opened.manifest().manifest_digest(),
            draft.member_set_digest().to_owned(),
            opened.read_all()?,
        )?);
    }
    Ok(runs)
}

fn member_manifest_digests(
    members: &[ResearchMemberLocator],
) -> Result<Vec<String>, ResearchRunError> {
    if members.is_empty()
        || members.len() > MAX_RUN_COUNT
        || members.len() > usize::try_from(u32::MAX).unwrap_or(usize::MAX)
    {
        return Err(ResearchRunError::ResourceLimit);
    }
    members
        .iter()
        .map(|member| {
            let digest = member.partition_manifest_digest().to_owned();
            validate_digest(&digest)?;
            Ok(digest)
        })
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FinalPlanManifest {
    version: u8,
    plan: ResearchSourcePlanWire,
    final_run_digest: String,
    final_run_records: u64,
    merge_passes: u16,
    source_plan_digest: String,
}

impl FinalPlanManifest {
    fn new(
        plan: ResearchSourcePlanWire,
        final_run: &AvailabilityRun,
        merge_passes: u16,
    ) -> Result<Self, ResearchRunError> {
        let final_run_digest = final_run.digest().to_owned();
        let final_run_records = final_run.record_count();
        let source_plan_digest =
            source_plan_digest(&plan, &final_run_digest, final_run_records, merge_passes)?;
        Ok(Self {
            version: RUN_VERSION,
            plan,
            final_run_digest,
            final_run_records,
            merge_passes,
            source_plan_digest,
        })
    }
}

fn source_plan_digest(
    plan: &ResearchSourcePlanWire,
    final_run_digest: &str,
    final_run_records: u64,
    merge_passes: u16,
) -> Result<String, ResearchRunError> {
    validate_digest(final_run_digest)?;
    let canonical = serde_json::to_vec(&SourcePlanDigestWire {
        version: RUN_VERSION,
        plan,
        final_run_digest,
        final_run_records,
        merge_passes,
    })?;
    let mut hasher = Hasher::new_derive_key("trench.research.source-plan.v1");
    hasher.update(&(canonical.len() as u64).to_be_bytes());
    hasher.update(&canonical);
    Ok(format!("b3:{}", hasher.finalize().to_hex()))
}

#[derive(Serialize)]
struct SourcePlanDigestWire<'a> {
    version: u8,
    plan: &'a ResearchSourcePlanWire,
    final_run_digest: &'a str,
    final_run_records: u64,
    merge_passes: u16,
}

#[cfg(all(test, unix))]
fn write_final_manifest(path: &Path, manifest: &FinalPlanManifest) -> Result<(), ResearchRunError> {
    let parent = path.parent().ok_or(ResearchRunError::InvalidPlan {
        reason: "final plan manifest requires a parent directory",
    })?;
    let file_name = private_file_name(path)?;
    let directory = open_private_directory_descriptor(parent)?;
    write_final_manifest_at(&directory, &file_name, manifest)
}

fn write_final_manifest_at(
    directory: &File,
    file_name: &str,
    manifest: &FinalPlanManifest,
) -> Result<(), ResearchRunError> {
    let bytes = serde_json::to_vec(manifest)?;
    if bytes.len() > usize::try_from(MAX_PLAN_METADATA_BYTES).unwrap_or(usize::MAX) {
        return Err(ResearchRunError::ResourceLimit);
    }
    let mut file = create_private_file_at(directory, file_name)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

fn parse_final_manifest(bytes: &[u8]) -> Result<FinalPlanManifest, ResearchRunError> {
    let manifest = serde_json::from_slice::<FinalPlanManifest>(bytes)?;
    if serde_json::to_vec(&manifest)? != bytes {
        return Err(ResearchRunError::InvalidPlan {
            reason: "final plan manifest is not canonical JSON",
        });
    }
    if manifest.version != RUN_VERSION
        || manifest.final_run_records == 0
        || manifest.merge_passes > MAX_MERGE_PASSES
    {
        return Err(ResearchRunError::InvalidPlan {
            reason: "final plan manifest fields are invalid",
        });
    }
    validate_digest(&manifest.final_run_digest)?;
    validate_digest(&manifest.source_plan_digest)?;
    Ok(manifest)
}

fn verify_final_manifest(
    manifest: &FinalPlanManifest,
    draft: &ResearchSourcePlanDraft,
    final_run: &AvailabilityRun,
) -> Result<(), ResearchRunError> {
    if manifest.plan != draft.wire()
        || manifest.final_run_digest != final_run.digest
        || manifest.final_run_records != final_run.record_count
        || manifest.source_plan_digest
            != source_plan_digest(
                &manifest.plan,
                &manifest.final_run_digest,
                manifest.final_run_records,
                manifest.merge_passes,
            )?
    {
        return Err(ResearchRunError::InvalidPlan {
            reason: "final plan manifest does not bind the revalidated source plan and run",
        });
    }
    Ok(())
}

fn verify_final_run_source_union(
    store: &ParquetStore,
    draft: &ResearchSourcePlanDraft,
    final_run: &AvailabilityRun,
) -> Result<Vec<ReconciledRecoveryOutcome>, ResearchRunError> {
    let mut expected = Vec::with_capacity(draft.members().len());
    for (ordinal, locator) in draft.members().iter().enumerate() {
        let ordinal = u32::try_from(ordinal).map_err(|_| ResearchRunError::ResourceLimit)?;
        let opened = locator.open(store)?;
        if opened.manifest().manifest_digest() != locator.partition_manifest_digest() {
            return Err(ResearchRunError::InvalidPlan {
                reason: "direct source member drifted while verifying the final run",
            });
        }
        let mut records = opened
            .read_all()?
            .into_iter()
            .map(|event| {
                AvailabilityRecord::new(
                    event,
                    ordinal,
                    locator.partition_manifest_digest().to_owned(),
                    draft.member_set_digest().to_owned(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        records.sort_by_key(AvailabilityRecord::key);
        expected.push(member_records_digest(records.into_iter())?);
    }
    let mut actual = (0..draft.members().len())
        .map(|_| MemberRecordsHasher::new())
        .collect::<Vec<_>>();
    let outcome_store = RecoveryOutcomeStore::open(store)?;
    let mut recovery_outcomes = Vec::with_capacity(draft.recovery_outcomes().len());
    let mut required_references = BTreeSet::new();
    for locator in draft.recovery_outcomes() {
        let outcome = outcome_store.open_member(locator)?;
        for reference in outcome.source_references() {
            required_references.insert(reference.clone());
            if required_references.len() > MAX_TOTAL_RECOVERY_PROOF_REFERENCES {
                return Err(ResearchRunError::ResourceLimit);
            }
        }
        recovery_outcomes.push(outcome);
    }
    let mut observed_references = BTreeMap::new();
    final_run.inspect_verified_records(|reader| {
        while let Some(record) = reader.next_record()? {
            let state = actual
                .get_mut(
                    usize::try_from(record.member_ordinal())
                        .map_err(|_| ResearchRunError::ResourceLimit)?,
                )
                .ok_or(ResearchRunError::InvalidRun {
                    reason: "final run record member ordinal is outside the selected source set",
                })?;
            state.update(&record)?;
            let reference = RecoverySourceReference::new(
                record.member_manifest_digest().to_owned(),
                record.key(),
            )?;
            if required_references.contains(&reference)
                && observed_references
                    .insert(reference, record.event().clone())
                    .is_some()
            {
                return Err(ResearchRunError::InvalidPlan {
                    reason: "verified final run repeats a recovery source reference",
                });
            }
        }
        Ok(())
    })?;
    let actual = actual
        .into_iter()
        .map(MemberRecordsHasher::finish)
        .collect::<Vec<_>>();
    if expected != actual {
        return Err(ResearchRunError::InvalidPlan {
            reason: "final run is not the complete exact union of directly reopened source members",
        });
    }
    for outcome in &recovery_outcomes {
        outcome.verify_result_from_raw(&observed_references)?;
    }
    Ok(recovery_outcomes)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MemberRecordsDigest {
    record_count: u64,
    digest: String,
}

impl MemberRecordsDigest {
    fn from_hasher(record_count: u64, hasher: Hasher) -> Self {
        Self {
            record_count,
            digest: format!("b3:{}", hasher.finalize().to_hex()),
        }
    }
}

struct MemberRecordsHasher {
    record_count: u64,
    hasher: Hasher,
}

impl MemberRecordsHasher {
    fn new() -> Self {
        Self {
            record_count: 0,
            hasher: Hasher::new_derive_key("trench.research.member-run.v1"),
        }
    }

    fn update(&mut self, record: &AvailabilityRecord) -> Result<(), ResearchRunError> {
        let encoded = encode_record(record)?;
        self.hasher.update(&(encoded.len() as u32).to_be_bytes());
        self.hasher.update(&encoded);
        self.record_count = self
            .record_count
            .checked_add(1)
            .ok_or(ResearchRunError::ResourceLimit)?;
        Ok(())
    }

    fn finish(self) -> MemberRecordsDigest {
        MemberRecordsDigest::from_hasher(self.record_count, self.hasher)
    }
}

fn member_records_digest(
    records: impl IntoIterator<Item = AvailabilityRecord>,
) -> Result<MemberRecordsDigest, ResearchRunError> {
    let mut hasher = Hasher::new_derive_key("trench.research.member-run.v1");
    let mut record_count = 0_u64;
    for record in records {
        let encoded = encode_record(&record)?;
        hasher.update(&(encoded.len() as u32).to_be_bytes());
        hasher.update(&encoded);
        record_count = record_count
            .checked_add(1)
            .ok_or(ResearchRunError::ResourceLimit)?;
    }
    Ok(MemberRecordsDigest::from_hasher(record_count, hasher))
}

#[cfg(unix)]
fn open_private_directory_descriptor(path: &Path) -> Result<File, ResearchRunError> {
    if !path.is_absolute() {
        return Err(ResearchRunError::InvalidPlan {
            reason: "final plan directory must be absolute",
        });
    }
    let mut directory = File::from(
        open(
            "/",
            OFlags::RDONLY
                | OFlags::DIRECTORY
                | OFlags::NOFOLLOW
                | OFlags::NONBLOCK
                | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|source| ResearchRunError::Io(source.into()))?,
    );
    ensure_directory_descriptor(&directory)?;
    for component in path.components() {
        let Component::Normal(component) = component else {
            if matches!(component, Component::RootDir) {
                continue;
            }
            return Err(ResearchRunError::InvalidPlan {
                reason: "final plan directory contains a non-normal component",
            });
        };
        directory = File::from(
            openat(
                &directory,
                component,
                OFlags::RDONLY
                    | OFlags::DIRECTORY
                    | OFlags::NOFOLLOW
                    | OFlags::NONBLOCK
                    | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|source| ResearchRunError::Io(source.into()))?,
        );
        ensure_directory_descriptor(&directory)?;
    }
    ensure_private_directory_descriptor(&directory)?;
    Ok(directory)
}

#[cfg(not(unix))]
fn open_private_directory_descriptor(_path: &Path) -> Result<File, ResearchRunError> {
    Err(ResearchRunError::UnsupportedPlatform)
}

#[cfg(unix)]
fn ensure_directory_descriptor(directory: &File) -> Result<(), ResearchRunError> {
    let metadata = fstat(directory).map_err(|source| ResearchRunError::Io(source.into()))?;
    if !FileType::from_raw_mode(metadata.st_mode).is_dir() {
        return Err(ResearchRunError::InvalidPlan {
            reason: "final plan path contains a non-directory component",
        });
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_private_directory_descriptor(directory: &File) -> Result<(), ResearchRunError> {
    let metadata = fstat(directory).map_err(|source| ResearchRunError::Io(source.into()))?;
    if !FileType::from_raw_mode(metadata.st_mode).is_dir()
        || metadata.st_uid != rustix::process::geteuid().as_raw()
        || metadata.st_mode & 0o777 != 0o700
    {
        return Err(ResearchRunError::InvalidPlan {
            reason: "final plan directory must be an effective-user owned mode-0700 directory",
        });
    }
    Ok(())
}

#[cfg(unix)]
fn open_private_directory_at(parent: &File, name: &str) -> Result<File, ResearchRunError> {
    let directory = File::from(
        openat(
            parent,
            name,
            OFlags::RDONLY
                | OFlags::DIRECTORY
                | OFlags::NOFOLLOW
                | OFlags::NONBLOCK
                | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|source| ResearchRunError::Io(source.into()))?,
    );
    ensure_private_directory_descriptor(&directory)?;
    Ok(directory)
}

#[cfg(unix)]
fn open_private_regular_file_at(
    directory: &File,
    name: &str,
    limit: u64,
) -> Result<File, ResearchRunError> {
    let file = File::from(
        openat(
            directory,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|source| ResearchRunError::Io(source.into()))?,
    );
    let metadata = fstat(&file).map_err(|source| ResearchRunError::Io(source.into()))?;
    let bytes = u64::try_from(metadata.st_size).map_err(|_| ResearchRunError::ResourceLimit)?;
    if !FileType::from_raw_mode(metadata.st_mode).is_file()
        || metadata.st_uid != rustix::process::geteuid().as_raw()
        || metadata.st_mode & 0o777 != 0o600
        || bytes > limit
    {
        return Err(ResearchRunError::InvalidPlan {
            reason: "final plan payload must be a bounded private regular file",
        });
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_private_regular_file_at(
    _directory: &File,
    _name: &str,
    _limit: u64,
) -> Result<File, ResearchRunError> {
    Err(ResearchRunError::UnsupportedPlatform)
}

fn read_private_file_at(
    directory: &File,
    name: &str,
    limit: u64,
) -> Result<Vec<u8>, ResearchRunError> {
    let mut file = open_private_regular_file_at(directory, name, limit)?;
    let length = file.metadata()?.len();
    let mut bytes =
        Vec::with_capacity(usize::try_from(length).map_err(|_| ResearchRunError::ResourceLimit)?);
    file.read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).ok() != Some(length) {
        return Err(ResearchRunError::InvalidPlan {
            reason: "final plan payload changed while being read",
        });
    }
    Ok(bytes)
}

#[cfg(unix)]
fn directory_names(directory: &File) -> Result<Vec<std::ffi::OsString>, ResearchRunError> {
    let path = format!("/proc/self/fd/{}", directory.as_raw_fd());
    let mut entries = fs::read_dir(path)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort();
    Ok(entries)
}

#[cfg(not(unix))]
fn directory_names(_directory: &File) -> Result<Vec<std::ffi::OsString>, ResearchRunError> {
    Err(ResearchRunError::UnsupportedPlatform)
}

fn require_exact_final_entries_at(directory: &File) -> Result<(), ResearchRunError> {
    let expected = [
        std::ffi::OsString::from(FINAL_RUN_FILE),
        std::ffi::OsString::from(PLAN_MANIFEST_FILE),
    ];
    if directory_names(directory)? != expected {
        return Err(ResearchRunError::InvalidPlan {
            reason: "final plan directory has incomplete or unexpected entries",
        });
    }
    let _plan =
        open_private_regular_file_at(directory, PLAN_MANIFEST_FILE, MAX_PLAN_METADATA_BYTES)?;
    let _run = open_private_regular_file_at(directory, FINAL_RUN_FILE, MAX_RUN_WIRE_BYTES)?;
    Ok(())
}

#[cfg(unix)]
fn final_directories_are_identical_at(
    staged_directory: &File,
    existing_directory: &File,
) -> Result<bool, ResearchRunError> {
    require_exact_final_entries_at(staged_directory)?;
    require_exact_final_entries_at(existing_directory)?;
    for name in [PLAN_MANIFEST_FILE, FINAL_RUN_FILE] {
        let limit = if name == FINAL_RUN_FILE {
            MAX_RUN_WIRE_BYTES
        } else {
            MAX_PLAN_METADATA_BYTES
        };
        let left = read_private_file_at(staged_directory, name, limit)?;
        let right = read_private_file_at(existing_directory, name, limit)?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(unix)]
fn remove_private_tree_at(parent: &File, name: &str) -> Result<(), ResearchRunError> {
    let directory = open_private_directory_at(parent, name)?;
    remove_private_directory_contents(&directory)?;
    unlinkat(parent, name, AtFlags::REMOVEDIR).map_err(|source| ResearchRunError::Io(source.into()))
}

#[cfg(unix)]
fn remove_private_directory_contents(directory: &File) -> Result<(), ResearchRunError> {
    for name in directory_names(directory)? {
        let file = File::from(
            openat(
                directory,
                &name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|source| ResearchRunError::Io(source.into()))?,
        );
        let metadata = fstat(&file).map_err(|source| ResearchRunError::Io(source.into()))?;
        if FileType::from_raw_mode(metadata.st_mode).is_dir() {
            ensure_private_directory_descriptor(&file)?;
            remove_private_directory_contents(&file)?;
            unlinkat(directory, &name, AtFlags::REMOVEDIR)
                .map_err(|source| ResearchRunError::Io(source.into()))?;
        } else {
            unlinkat(directory, &name, AtFlags::empty())
                .map_err(|source| ResearchRunError::Io(source.into()))?;
        }
    }
    Ok(())
}

fn sync_directory_descriptor(directory: &File) -> Result<(), ResearchRunError> {
    directory.sync_all()?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunHeader {
    version: u8,
    record_count: u64,
    record_bytes: u64,
    min_key: RunKeyWire,
    max_key: RunKeyWire,
    input_run_digests: Vec<String>,
    member_set_digest: String,
    output_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunKeyWire {
    received_at_ns: i64,
    event_time_ns: i64,
    event_id: String,
}

impl RunKeyWire {
    fn from_key(key: &AvailabilityKey) -> Self {
        Self {
            received_at_ns: key.received_at.value(),
            event_time_ns: key.event_time.value(),
            event_id: key.event_id.as_str().to_owned(),
        }
    }

    fn key(&self) -> Result<AvailabilityKey, ResearchRunError> {
        Ok(AvailabilityKey {
            received_at: TimestampNs::new(i128::from(self.received_at_ns)).map_err(|_| {
                ResearchRunError::InvalidRun {
                    reason: "run key receipt timestamp is invalid",
                }
            })?,
            event_time: TimestampNs::new(i128::from(self.event_time_ns)).map_err(|_| {
                ResearchRunError::InvalidRun {
                    reason: "run key event timestamp is invalid",
                }
            })?,
            event_id: EventId::new(self.event_id.clone()).map_err(|_| {
                ResearchRunError::InvalidRun {
                    reason: "run key event identifier is invalid",
                }
            })?,
        })
    }
}

#[cfg(unix)]
fn write_run_at<F>(
    directory: &Arc<File>,
    file_name: &str,
    member_set_digest: &str,
    input_run_digests: &[String],
    mut next: F,
) -> Result<AvailabilityRun, ResearchRunError>
where
    F: FnMut() -> Result<Option<AvailabilityRecord>, ResearchRunError>,
{
    let file_name = private_file_name(Path::new(file_name))?;
    let body_name = format!(".{file_name}.body.tmp");
    validate_digest(member_set_digest)?;
    if input_run_digests.len() > MAX_RUN_MERGE_INPUTS {
        return Err(ResearchRunError::ResourceLimit);
    }
    for digest in input_run_digests {
        validate_digest(digest)?;
    }
    let write_result = (|| {
        let mut body = create_private_file_at(directory, &body_name)?;
        let mut digest = Hasher::new_derive_key("trench.research.availability-run.v1");
        let mut previous = None;
        let mut minimum = None;
        let mut maximum = None;
        let mut record_count = 0_u64;
        let mut record_bytes = 0_u64;
        while let Some(record) = next()? {
            record.validate()?;
            if record.member_set_digest != member_set_digest {
                return Err(ResearchRunError::InvalidRun {
                    reason: "run record carries another member-set commitment",
                });
            }
            let key = record.key();
            if previous
                .as_ref()
                .is_some_and(|previous: &AvailabilityKey| previous >= &key)
            {
                return Err(ResearchRunError::NonMonotonicAvailability);
            }
            let encoded = encode_record(&record)?;
            record_count = record_count
                .checked_add(1)
                .ok_or(ResearchRunError::ResourceLimit)?;
            record_bytes = record_bytes
                .checked_add(
                    u64::try_from(encoded.len()).map_err(|_| ResearchRunError::ResourceLimit)?,
                )
                .ok_or(ResearchRunError::ResourceLimit)?;
            if record_count > MAX_RUN_RECORDS || record_bytes > MAX_RUN_WIRE_BYTES {
                return Err(ResearchRunError::ResourceLimit);
            }
            body.write_all(&(encoded.len() as u32).to_be_bytes())?;
            body.write_all(&encoded)?;
            digest.update(&(encoded.len() as u32).to_be_bytes());
            digest.update(&encoded);
            if minimum.is_none() {
                minimum = Some(RunKeyWire::from_key(&key));
            }
            maximum = Some(RunKeyWire::from_key(&key));
            previous = Some(key);
        }
        if record_count == 0 {
            return Err(ResearchRunError::InvalidRun {
                reason: "availability runs cannot be empty",
            });
        }
        body.sync_all()?;
        let header = RunHeader {
            version: RUN_VERSION,
            record_count,
            record_bytes,
            min_key: minimum.expect("nonempty run has a minimum key"),
            max_key: maximum.expect("nonempty run has a maximum key"),
            input_run_digests: input_run_digests.to_vec(),
            member_set_digest: member_set_digest.to_owned(),
            output_digest: format!("b3:{}", digest.finalize().to_hex()),
        };
        let header_bytes = serde_json::to_vec(&header)?;
        if header_bytes.len() > MAX_RUN_HEADER_BYTES {
            return Err(ResearchRunError::ResourceLimit);
        }
        body.seek(SeekFrom::Start(0))?;
        let mut output = create_private_file_at(directory, &file_name)?;
        output.write_all(&RUN_MAGIC)?;
        output.write_all(&(header_bytes.len() as u32).to_be_bytes())?;
        output.write_all(&header_bytes)?;
        io::copy(&mut body, &mut output)?;
        output.sync_all()?;
        drop(output);
        unlinkat(directory, &body_name, AtFlags::empty())
            .map_err(|source| ResearchRunError::Io(source.into()))?;
        AvailabilityRun::open_staged_at(directory.clone(), file_name.clone(), member_set_digest)
    })();
    if write_result.is_err() {
        let _ = unlinkat(directory, &body_name, AtFlags::empty());
        let _ = unlinkat(directory, &file_name, AtFlags::empty());
    }
    write_result
}

#[cfg(not(unix))]
fn write_run_at<F>(
    _directory: &Arc<File>,
    _file_name: &str,
    _member_set_digest: &str,
    _input_run_digests: &[String],
    _next: F,
) -> Result<AvailabilityRun, ResearchRunError>
where
    F: FnMut() -> Result<Option<AvailabilityRecord>, ResearchRunError>,
{
    Err(ResearchRunError::UnsupportedPlatform)
}

fn validate_run_at(
    directory: &File,
    file_name: &str,
    expected_member_set_digest: &str,
    member_manifest_digests: Option<&[String]>,
) -> Result<RunHeader, ResearchRunError> {
    validate_digest(expected_member_set_digest)?;
    let mut reader = RunReader::open_at(
        directory,
        file_name,
        expected_member_set_digest,
        member_manifest_digests,
    )?;
    let header = reader.header.clone();
    while reader.next_record()?.is_some() {}
    Ok(header)
}

fn validate_run_file(
    file: File,
    expected_member_set_digest: &str,
    member_manifest_digests: Option<&[String]>,
) -> Result<(RunHeader, File), ResearchRunError> {
    let mut reader =
        RunReader::from_file(file, expected_member_set_digest, member_manifest_digests)?;
    let header = reader.header.clone();
    while reader.next_record()?.is_some() {}
    Ok((header, reader.rewind()?))
}

struct RunReader {
    file: File,
    header: RunHeader,
    remaining: u64,
    expected_member_set_digest: String,
    member_manifest_digests: Option<Vec<String>>,
    previous: Option<AvailabilityKey>,
    minimum: Option<RunKeyWire>,
    maximum: Option<RunKeyWire>,
    record_bytes: u64,
    digest: Hasher,
}

impl RunReader {
    fn open_at(
        directory: &File,
        file_name: &str,
        expected_member_set_digest: &str,
        member_manifest_digests: Option<&[String]>,
    ) -> Result<Self, ResearchRunError> {
        let file = open_private_regular_file_at(directory, file_name, MAX_RUN_WIRE_BYTES)?;
        Self::from_file(file, expected_member_set_digest, member_manifest_digests)
    }

    fn from_file(
        mut file: File,
        expected_member_set_digest: &str,
        member_manifest_digests: Option<&[String]>,
    ) -> Result<Self, ResearchRunError> {
        let mut magic = [0_u8; RUN_MAGIC.len()];
        file.read_exact(&mut magic)?;
        if magic != RUN_MAGIC {
            return Err(ResearchRunError::InvalidRun {
                reason: "run magic is invalid",
            });
        }
        let mut header_length = [0_u8; 4];
        file.read_exact(&mut header_length)?;
        let header_length = usize::try_from(u32::from_be_bytes(header_length))
            .map_err(|_| ResearchRunError::ResourceLimit)?;
        if header_length == 0 || header_length > MAX_RUN_HEADER_BYTES {
            return Err(ResearchRunError::ResourceLimit);
        }
        let mut header_bytes = vec![0_u8; header_length];
        file.read_exact(&mut header_bytes)?;
        let header = serde_json::from_slice::<RunHeader>(&header_bytes)?;
        if serde_json::to_vec(&header)? != header_bytes {
            return Err(ResearchRunError::InvalidRun {
                reason: "run header is not canonical JSON",
            });
        }
        validate_header(&header, expected_member_set_digest)?;
        Ok(Self {
            file,
            remaining: header.record_count,
            header,
            expected_member_set_digest: expected_member_set_digest.to_owned(),
            member_manifest_digests: member_manifest_digests.map(ToOwned::to_owned),
            previous: None,
            minimum: None,
            maximum: None,
            record_bytes: 0,
            digest: Hasher::new_derive_key("trench.research.availability-run.v1"),
        })
    }

    fn next_record(&mut self) -> Result<Option<AvailabilityRecord>, ResearchRunError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut length = [0_u8; 4];
        self.file.read_exact(&mut length)?;
        let length = usize::try_from(u32::from_be_bytes(length))
            .map_err(|_| ResearchRunError::ResourceLimit)?;
        if length == 0 || length > MAX_RECORD_WIRE_BYTES {
            return Err(ResearchRunError::ResourceLimit);
        }
        let mut encoded = vec![0_u8; length];
        self.file.read_exact(&mut encoded)?;
        let record = decode_record(&encoded)?;
        if record.member_set_digest != self.expected_member_set_digest {
            return Err(ResearchRunError::InvalidRun {
                reason: "run record has another member-set commitment",
            });
        }
        if let Some(member_manifest_digests) = &self.member_manifest_digests
            && member_manifest_digests.get(
                usize::try_from(record.member_ordinal)
                    .map_err(|_| ResearchRunError::ResourceLimit)?,
            ) != Some(&record.member_manifest_digest)
        {
            return Err(ResearchRunError::InvalidRun {
                reason: "run record does not bind its canonical source member ordinal",
            });
        }
        let key = record.key();
        if self.previous.as_ref().is_some_and(|prior| prior >= &key) {
            return Err(ResearchRunError::NonMonotonicAvailability);
        }
        if self.minimum.is_none() {
            self.minimum = Some(RunKeyWire::from_key(&key));
        }
        self.maximum = Some(RunKeyWire::from_key(&key));
        self.previous = Some(key);
        self.digest.update(&(length as u32).to_be_bytes());
        self.digest.update(&encoded);
        self.record_bytes = self
            .record_bytes
            .checked_add(u64::try_from(length).map_err(|_| ResearchRunError::ResourceLimit)?)
            .ok_or(ResearchRunError::ResourceLimit)?;
        if self.record_bytes > MAX_RUN_WIRE_BYTES {
            return Err(ResearchRunError::ResourceLimit);
        }
        self.remaining -= 1;
        if self.remaining == 0 {
            self.verify_complete()?;
        }
        Ok(Some(record))
    }

    fn verify_complete(&mut self) -> Result<(), ResearchRunError> {
        if self.record_bytes != self.header.record_bytes
            || self.minimum.as_ref() != Some(&self.header.min_key)
            || self.maximum.as_ref() != Some(&self.header.max_key)
            || format!("b3:{}", self.digest.finalize().to_hex()) != self.header.output_digest
        {
            return Err(ResearchRunError::InvalidRun {
                reason: "run metadata does not match its immutable records",
            });
        }
        if self.file.read(&mut [0_u8; 1])? != 0 {
            return Err(ResearchRunError::InvalidRun {
                reason: "run contains trailing bytes",
            });
        }
        Ok(())
    }

    fn rewind(mut self) -> Result<File, ResearchRunError> {
        self.file.seek(SeekFrom::Start(0))?;
        Ok(self.file)
    }
}

struct MergeCursor {
    readers: Vec<RunReader>,
    heap: BinaryHeap<HeapEntry>,
}

impl MergeCursor {
    fn new(inputs: &[AvailabilityRun]) -> Result<Self, ResearchRunError> {
        let mut readers = inputs
            .iter()
            .map(AvailabilityRun::staged_reader)
            .collect::<Result<Vec<_>, _>>()?;
        let mut heap = BinaryHeap::new();
        for (source, reader) in readers.iter_mut().enumerate() {
            if let Some(record) = reader.next_record()? {
                heap.push(HeapEntry { record, source });
            }
        }
        Ok(Self { readers, heap })
    }

    fn next_record(&mut self) -> Result<Option<AvailabilityRecord>, ResearchRunError> {
        let Some(HeapEntry { record, source }) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some(next) = self.readers[source].next_record()? {
            self.heap.push(HeapEntry {
                record: next,
                source,
            });
        }
        Ok(Some(record))
    }
}

struct HeapEntry {
    record: AvailabilityRecord,
    source: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.record.key() == other.record.key() && self.source == other.source
    }
}

impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .record
            .key()
            .cmp(&self.record.key())
            .then_with(|| other.source.cmp(&self.source))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn encode_record(record: &AvailabilityRecord) -> Result<Vec<u8>, ResearchRunError> {
    let event_wire = canonical_event_wire(&record.event)?;
    if event_wire.len() > MAX_EVENT_WIRE_BYTES {
        return Err(ResearchRunError::ResourceLimit);
    }
    let event_id = record.event.event_id().as_str().as_bytes();
    if event_id.len() != DIGEST_BYTES {
        return Err(ResearchRunError::InvalidRun {
            reason: "event identifier is not a BLAKE3 digest",
        });
    }
    let mut encoded = Vec::with_capacity(4 + DIGEST_BYTES * 3 + 4 + event_wire.len());
    encoded.extend_from_slice(&record.member_ordinal.to_be_bytes());
    encoded.extend_from_slice(record.member_manifest_digest.as_bytes());
    encoded.extend_from_slice(record.member_set_digest.as_bytes());
    encoded.extend_from_slice(event_id);
    encoded.extend_from_slice(&(event_wire.len() as u32).to_be_bytes());
    encoded.extend_from_slice(&event_wire);
    Ok(encoded)
}

fn decode_record(encoded: &[u8]) -> Result<AvailabilityRecord, ResearchRunError> {
    const FIXED: usize = 4 + DIGEST_BYTES * 3 + 4;
    if encoded.len() < FIXED {
        return Err(ResearchRunError::InvalidRun {
            reason: "run record is truncated",
        });
    }
    let member_ordinal = u32::from_be_bytes(encoded[0..4].try_into().expect("fixed slice"));
    let member_manifest_digest = string_field(&encoded[4..4 + DIGEST_BYTES])?;
    let member_set_start = 4 + DIGEST_BYTES;
    let member_set_digest =
        string_field(&encoded[member_set_start..member_set_start + DIGEST_BYTES])?;
    let event_id_start = member_set_start + DIGEST_BYTES;
    let event_id = string_field(&encoded[event_id_start..event_id_start + DIGEST_BYTES])?;
    let event_length_start = event_id_start + DIGEST_BYTES;
    let event_length = usize::try_from(u32::from_be_bytes(
        encoded[event_length_start..event_length_start + 4]
            .try_into()
            .expect("fixed slice"),
    ))
    .map_err(|_| ResearchRunError::ResourceLimit)?;
    if event_length > MAX_EVENT_WIRE_BYTES || encoded.len() != FIXED + event_length {
        return Err(ResearchRunError::InvalidRun {
            reason: "run record event length is invalid",
        });
    }
    let event = event_from_canonical_wire(&encoded[FIXED..])?;
    if event.event_id().as_str() != event_id {
        return Err(ResearchRunError::InvalidRun {
            reason: "run record event identifier disagrees with event wire",
        });
    }
    AvailabilityRecord::new(
        event,
        member_ordinal,
        member_manifest_digest,
        member_set_digest,
    )
}

fn validate_header(
    header: &RunHeader,
    expected_member_set_digest: &str,
) -> Result<(), ResearchRunError> {
    if header.version != RUN_VERSION
        || header.record_count == 0
        || header.record_count > MAX_RUN_RECORDS
        || header.record_bytes == 0
        || header.record_bytes > MAX_RUN_WIRE_BYTES
        || header.input_run_digests.len() > MAX_RUN_MERGE_INPUTS
        || header.member_set_digest != expected_member_set_digest
    {
        return Err(ResearchRunError::InvalidRun {
            reason: "run header fields are outside the fixed contract",
        });
    }
    validate_digest(&header.member_set_digest)?;
    validate_digest(&header.output_digest)?;
    for digest in &header.input_run_digests {
        validate_digest(digest)?;
    }
    let min = header.min_key.key()?;
    let max = header.max_key.key()?;
    if min > max {
        return Err(ResearchRunError::InvalidRun {
            reason: "run key bounds are reversed",
        });
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<(), ResearchRunError> {
    let Some(hex) = value.strip_prefix("b3:") else {
        return Err(ResearchRunError::InvalidRun {
            reason: "digest lacks the BLAKE3 prefix",
        });
    };
    if value.len() != DIGEST_BYTES
        || hex.len() != blake3::OUT_LEN * 2
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ResearchRunError::InvalidRun {
            reason: "digest is not canonical lowercase BLAKE3",
        });
    }
    Ok(())
}

fn string_field(bytes: &[u8]) -> Result<String, ResearchRunError> {
    let value = std::str::from_utf8(bytes).map_err(|_| ResearchRunError::InvalidRun {
        reason: "run record digest is not UTF-8",
    })?;
    validate_digest(value)?;
    Ok(value.to_owned())
}

fn private_file_name(path: &Path) -> Result<String, ResearchRunError> {
    let Some(name) = path.file_name() else {
        return Err(ResearchRunError::InvalidRun {
            reason: "run file name must be one normal path component",
        });
    };
    name.to_str()
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .ok_or(ResearchRunError::InvalidRun {
            reason: "run file name must be valid UTF-8",
        })
}

#[cfg(unix)]
fn create_private_file_at(directory: &File, file_name: &str) -> Result<File, ResearchRunError> {
    let file = File::from(
        openat(
            directory,
            file_name,
            OFlags::RDWR
                | OFlags::CREATE
                | OFlags::EXCL
                | OFlags::NOFOLLOW
                | OFlags::NONBLOCK
                | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|source| ResearchRunError::Io(source.into()))?,
    );
    set_private_file_permissions(&file)?;
    Ok(file)
}

#[cfg(not(unix))]
fn create_private_file_at(_directory: &File, _file_name: &str) -> Result<File, ResearchRunError> {
    Err(ResearchRunError::UnsupportedPlatform)
}

#[cfg(unix)]
fn set_private_file_permissions(file: &File) -> Result<(), ResearchRunError> {
    rustix::fs::fchmod(file, rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR)
        .map_err(|source| ResearchRunError::Io(io::Error::from(source)))
}

#[cfg(not(unix))]
fn set_private_file_permissions(_file: &File) -> Result<(), ResearchRunError> {
    Err(ResearchRunError::UnsupportedPlatform)
}

/// Availability-run construction or validation failure.
#[derive(Debug, Error)]
pub enum ResearchRunError {
    /// Revalidating the immutable Task-2 source selection failed.
    #[error(transparent)]
    Plan(#[from] ResearchPlanError),
    /// Canonical normalized-event encoding or decoding failed.
    #[error(transparent)]
    Storage(#[from] ParquetError),
    /// A descriptor-bound recovery companion member was invalid or drifted.
    #[error(transparent)]
    RecoveryOutcome(#[from] crate::recovery_outcomes::RecoveryOutcomeError),
    /// Canonical JSON metadata failed to encode or decode.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Filesystem I/O failed while staging or reading one run.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// A run violated its immutable fixed format.
    #[error("invalid availability run: {reason}")]
    InvalidRun { reason: &'static str },
    /// A final source-plan directory was malformed, incomplete, or did not bind its run.
    #[error("invalid final research source plan: {reason}")]
    InvalidPlan { reason: &'static str },
    /// An existing final directory did not exactly match the newly staged immutable plan.
    #[error("existing final research source plan conflicts with this staged plan")]
    ConflictingFinalPlan,
    /// A run contained equal or descending canonical availability keys.
    #[error("availability run keys are not strictly increasing")]
    NonMonotonicAvailability,
    /// A bounded run input, record, or metadata value exceeded its fixed limit.
    #[error("availability run resource limit exceeded")]
    ResourceLimit,
    /// The runtime cannot safely create private run files on this platform.
    #[error("availability runs require a Unix private filesystem")]
    UnsupportedPlatform,
}

#[cfg(all(test, unix))]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt};

    use rust_decimal_macros::dec;
    use tempfile::TempDir;
    use trench_core::{
        domain::{Market, Price, Quantity, Side},
        event::{TimestampNs, Trade},
        validation::TimeRange,
    };

    use crate::{
        parquet::{DataProvenance, ParquetStore},
        research_plan::{ResearchMemberLocator, ResearchSourcePlanBuilder},
    };

    use super::*;

    fn digest(character: char) -> String {
        format!("b3:{}", character.to_string().repeat(64))
    }

    fn timestamp(value: i64) -> TimestampNs {
        TimestampNs::new(i128::from(value)).expect("fixture timestamp")
    }

    fn range(start: i64, end: i64) -> TimeRange {
        TimeRange::new(timestamp(start), timestamp(end)).expect("fixture range")
    }

    fn secure(directory: &TempDir) {
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private temporary directory");
    }

    fn trade(event_time: i64, received_at: i64, trade_id: u64) -> MarketEvent {
        MarketEvent::trade(
            timestamp(event_time),
            timestamp(received_at),
            Market::new("SOL").expect("fixture market"),
            Trade::new(
                trade_id,
                Side::Buy,
                Price::new(dec!(100)).expect("fixture price"),
                Quantity::new(dec!(1)).expect("fixture quantity"),
            )
            .expect("fixture trade"),
        )
        .expect("fixture event")
    }

    #[test]
    fn initial_and_merged_runs_use_only_the_availability_key() {
        let directory = TempDir::new().expect("temporary run directory");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private temporary run directory");
        let member_set = digest('a');
        let first = write_initial_run(
            directory.path().join("first.run"),
            0,
            digest('b'),
            member_set.clone(),
            vec![trade(20, 30, 2), trade(10, 15, 1)],
        )
        .expect("initial run");
        let second = write_initial_run(
            directory.path().join("second.run"),
            1,
            digest('c'),
            member_set.clone(),
            vec![trade(12, 16, 3)],
        )
        .expect("second initial run");
        let merged = merge_runs(
            directory.path().join("merged.run"),
            &[first, second],
            &member_set,
        )
        .expect("merged run");

        let keys = merged
            .records()
            .collect::<Result<Vec<_>, _>>()
            .expect("validated records")
            .into_iter()
            .map(|record| record.key().received_at().value())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec![15, 16, 30]);
        assert_eq!(merged.record_count(), 3);
    }

    #[test]
    fn merge_rejects_equal_full_availability_keys_after_heap_tie_breaking() {
        let directory = TempDir::new().expect("temporary run directory");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private temporary run directory");
        let member_set = digest('a');
        let duplicate = trade(20, 30, 2);
        let first = write_initial_run(
            directory.path().join("first.run"),
            0,
            digest('b'),
            member_set.clone(),
            vec![duplicate.clone()],
        )
        .expect("first initial run");
        let second = write_initial_run(
            directory.path().join("second.run"),
            1,
            digest('c'),
            member_set.clone(),
            vec![duplicate],
        )
        .expect("second initial run");

        assert!(matches!(
            merge_runs(
                directory.path().join("merged.run"),
                &[first, second],
                &member_set,
            ),
            Err(ResearchRunError::NonMonotonicAvailability)
        ));
    }

    #[test]
    fn descriptor_relative_run_io_rejects_swapped_symlink_and_fifo_entries() {
        let directory = TempDir::new().expect("temporary run directory");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private temporary run directory");
        let member_set = digest('a');
        std::os::unix::fs::symlink("/dev/null", directory.path().join("run.bin"))
            .expect("install symlink target");
        assert!(
            write_initial_run(
                directory.path().join("run.bin"),
                0,
                digest('b'),
                member_set.clone(),
                vec![trade(20, 30, 2)],
            )
            .is_err()
        );

        let directory_descriptor =
            open_private_directory_descriptor(directory.path()).expect("private descriptor");
        rustix::fs::mkfifoat(&directory_descriptor, "forged.run", Mode::RUSR | Mode::WUSR)
            .expect("create fifo");
        assert!(open_private_regular_file_at(&directory_descriptor, "forged.run", 1024).is_err());
    }

    #[test]
    fn reopening_rejects_a_self_consistent_run_that_is_not_the_source_union() {
        let root = TempDir::new().expect("temporary root");
        secure(&root);
        let provenance = DataProvenance::new(digest('a'), digest('b'), ParquetStore::schema_hash())
            .expect("fixture provenance");
        let store = ParquetStore::open(root.path(), provenance).expect("store");
        let source_events = vec![trade(100, 101, 1), trade(200, 201, 2)];
        let manifests = store
            .write_events(&source_events)
            .expect("source partition");
        let locator = ResearchMemberLocator::legacy(&manifests[0]);
        let draft = ResearchSourcePlanBuilder::new(range(0, 500), range(500, 1_000))
            .expect("windows")
            .build(&store, vec![locator.clone()], Vec::new())
            .expect("draft");
        let final_directory = root.path().join("forged-final");
        draft
            .publish_to(&store, &final_directory)
            .expect("published plan");

        let forged = write_initial_run(
            final_directory.join("forged.run"),
            0,
            locator.partition_manifest_digest().to_owned(),
            draft.member_set_digest().to_owned(),
            vec![source_events[0].clone(), trade(300, 301, 3)],
        )
        .expect("self-consistent forged run");
        fs::remove_file(final_directory.join(FINAL_RUN_FILE)).expect("replace final run");
        fs::rename(
            final_directory.join("forged.run"),
            final_directory.join(FINAL_RUN_FILE),
        )
        .expect("install forged run");

        let mut manifest = parse_final_manifest(
            &fs::read(final_directory.join(PLAN_MANIFEST_FILE)).expect("final manifest"),
        )
        .expect("parse manifest");
        manifest.final_run_digest = forged.digest().to_owned();
        manifest.final_run_records = forged.record_count();
        manifest.source_plan_digest = source_plan_digest(
            &manifest.plan,
            &manifest.final_run_digest,
            manifest.final_run_records,
            manifest.merge_passes,
        )
        .expect("rebind manifest");
        fs::remove_file(final_directory.join(PLAN_MANIFEST_FILE)).expect("replace final manifest");
        write_final_manifest(&final_directory.join(PLAN_MANIFEST_FILE), &manifest)
            .expect("write self-consistent manifest");

        assert!(ResearchSourcePlan::open_from(&store, &final_directory).is_err());
    }
}
