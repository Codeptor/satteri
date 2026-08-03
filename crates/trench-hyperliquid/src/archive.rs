//! Bounded local readers for explicitly downloaded official market archives.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};

use blake3::Hasher;
use lz4_flex::frame::FrameDecoder;
use rustix::fs::{FileType, Mode, OFlags, fstat, open, openat};
use serde_json::Value;
use thiserror::Error;
use time::OffsetDateTime;
use trench_core::domain::{EventId, Market};
use trench_core::event::{BookLevel, MarketEvent, MarketEventKind, TimestampNs};

use crate::ws::normalize_l2_book_wire_for_market;

const HOUR_MILLIS: i64 = 3_600_000;
const MAX_MANIFEST_REQUIREMENTS: usize = 32_768;
const MAX_MANIFEST_SOURCES: usize = 16_384;
const MAX_OPEN_SOURCES: usize = 256;
const MAX_COMPRESSED_SOURCE_BYTES: u64 = 1_073_741_824;
const MAX_TOTAL_COMPRESSED_BYTES: u64 = 8 * 1_024 * 1_024 * 1_024;
const MAX_DECOMPRESSED_SOURCE_BYTES: u64 = 4 * 1_024 * 1_024 * 1_024;
const MAX_TOTAL_DECOMPRESSED_BYTES: u64 = 8 * 1_024 * 1_024 * 1_024;
const MAX_RECORD_BYTES: usize = 1_024 * 1_024;
const MAX_EVENTS_PER_SOURCE: usize = 5_000_000;
const MAX_TOTAL_EVENTS: usize = 100_000;
const DIGEST_BUFFER_BYTES: usize = 64 * 1_024;
const CONTENT_DIGEST_DOMAIN: &str = "trench.archive.content.v1";
const LZ4_FRAME_MAGIC: [u8; 4] = [0x04, 0x22, 0x4d, 0x18];
const LZ4_CONTENT_CHECKSUM_FLAG: u8 = 0b0000_0100;

/// The market-data channel contained by a local archive object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ArchiveDataKind {
    /// Full L2 snapshots from the documented `l2Book` archive directory.
    L2Book,
    /// A completeness requirement unavailable from the documented archive.
    ///
    /// This variant may be required or reported missing, but it cannot name a
    /// historical source object because the official archive documents L2 only.
    Bbo,
}

impl ArchiveDataKind {
    const fn label(self) -> &'static str {
        match self {
            Self::L2Book => "l2Book",
            Self::Bbo => "bbo",
        }
    }
}

impl fmt::Display for ArchiveDataKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.label())
    }
}

/// One exact hourly market-data interval requested from the archive.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArchiveSpan {
    market: Market,
    data_kind: ArchiveDataKind,
    start_ms: i64,
    end_ms: i64,
}

impl ArchiveSpan {
    /// Creates one aligned, half-open UTC archive hour.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveError::InvalidSpan`] unless the interval is exactly one
    /// nonnegative, hour-aligned millisecond range.
    pub fn new(
        market: Market,
        data_kind: ArchiveDataKind,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Self, ArchiveError> {
        if start_ms < 0
            || start_ms.rem_euclid(HOUR_MILLIS) != 0
            || end_ms.checked_sub(start_ms) != Some(HOUR_MILLIS)
        {
            return Err(ArchiveError::InvalidSpan { start_ms, end_ms });
        }
        Ok(Self {
            market,
            data_kind,
            start_ms,
            end_ms,
        })
    }

    /// Returns the native perpetual market.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the expected source channel.
    #[must_use]
    pub const fn data_kind(&self) -> ArchiveDataKind {
        self.data_kind
    }

    /// Returns the inclusive UTC start in Unix milliseconds.
    #[must_use]
    pub const fn start_ms(&self) -> i64 {
        self.start_ms
    }

    /// Returns the exclusive UTC end in Unix milliseconds.
    #[must_use]
    pub const fn end_ms(&self) -> i64 {
        self.end_ms
    }

    fn official_relative_path(&self) -> Result<PathBuf, ArchiveError> {
        if self.data_kind != ArchiveDataKind::L2Book {
            return Err(ArchiveError::UnsupportedArchiveDataKind { span: self.clone() });
        }
        let timestamp_ns = i128::from(self.start_ms) * 1_000_000;
        let time = OffsetDateTime::from_unix_timestamp_nanos(timestamp_ns).map_err(|_| {
            ArchiveError::InvalidSpan {
                start_ms: self.start_ms,
                end_ms: self.end_ms,
            }
        })?;
        let date = time.date();
        Ok(PathBuf::from(format!(
            "market_data/{:04}{:02}{:02}/{}/{}/{}.lz4",
            date.year(),
            u8::from(date.month()),
            date.day(),
            time.hour(),
            "l2Book",
            self.market.as_str(),
        )))
    }
}

impl fmt::Display for ArchiveSpan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{}:[{}, {})",
            self.market.as_str(),
            self.data_kind,
            self.start_ms,
            self.end_ms
        )
    }
}

/// A BLAKE3 digest with an unambiguous display representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArchiveDigest([u8; blake3::OUT_LEN]);

impl ArchiveDigest {
    /// Hashes bytes supplied by a caller that is creating an explicit manifest.
    #[must_use]
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    fn from_hasher(hasher: Hasher) -> Self {
        Self(*hasher.finalize().as_bytes())
    }
}

impl fmt::Display for ArchiveDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "b3:{}",
            blake3::Hash::from_bytes(self.0).to_hex()
        )
    }
}

/// Verified compressed metadata for one explicitly downloaded local object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveSource {
    span: ArchiveSpan,
    relative_path: PathBuf,
    compressed_bytes: u64,
    compressed_digest: ArchiveDigest,
}

impl ArchiveSource {
    /// Describes one local object without opening it or performing I/O.
    #[must_use]
    pub fn new(
        span: ArchiveSpan,
        relative_path: PathBuf,
        compressed_bytes: u64,
        compressed_digest: ArchiveDigest,
    ) -> Self {
        Self {
            span,
            relative_path,
            compressed_bytes,
            compressed_digest,
        }
    }

    /// Returns the span represented by this source.
    #[must_use]
    pub const fn span(&self) -> &ArchiveSpan {
        &self.span
    }
}

/// Declares whether a source span is required for a replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveRequirement {
    span: ArchiveSpan,
    required: bool,
}

impl ArchiveRequirement {
    /// Requires an exact source object for the span.
    #[must_use]
    pub fn required(span: ArchiveSpan) -> Self {
        Self {
            span,
            required: true,
        }
    }

    /// Records an expected-but-optional source span.
    #[must_use]
    pub fn optional(span: ArchiveSpan) -> Self {
        Self {
            span,
            required: false,
        }
    }
}

/// Frozen, caller-supplied inputs for a local archive replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveManifest {
    as_of_ms: i64,
    received_at: TimestampNs,
    requirements: Vec<ArchiveRequirement>,
    sources: Vec<ArchiveSource>,
}

impl ArchiveManifest {
    /// Validates the complete local import manifest.
    ///
    /// `as_of_ms` is explicit replay time, never the host clock.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid as-of time, duplicate source spans, or a
    /// source that was not declared as a requirement.
    pub fn new(
        as_of_ms: i64,
        requirements: impl IntoIterator<Item = ArchiveRequirement>,
        sources: impl IntoIterator<Item = ArchiveSource>,
    ) -> Result<Self, ArchiveError> {
        let received_at =
            timestamp_from_millis(as_of_ms).ok_or(ArchiveError::InvalidAsOf { as_of_ms })?;
        let mut requirement_entries = Vec::new();
        for requirement in requirements {
            if requirement_entries.len() == MAX_MANIFEST_REQUIREMENTS {
                return Err(ArchiveError::TooManyRequirements {
                    count: requirement_entries.len().saturating_add(1),
                    max_requirements: MAX_MANIFEST_REQUIREMENTS,
                });
            }
            requirement_entries.push(requirement);
        }
        let mut source_entries = Vec::new();
        let mut total_compressed_bytes = 0_u64;
        for source in sources {
            if source_entries.len() == MAX_MANIFEST_SOURCES {
                return Err(ArchiveError::TooManySources {
                    count: source_entries.len().saturating_add(1),
                    max_sources: MAX_MANIFEST_SOURCES,
                });
            }
            total_compressed_bytes = total_compressed_bytes
                .checked_add(source.compressed_bytes)
                .ok_or(ArchiveError::TotalCompressedBytesTooLarge {
                    max_bytes: MAX_TOTAL_COMPRESSED_BYTES,
                })?;
            if total_compressed_bytes > MAX_TOTAL_COMPRESSED_BYTES {
                return Err(ArchiveError::TotalCompressedBytesTooLarge {
                    max_bytes: MAX_TOTAL_COMPRESSED_BYTES,
                });
            }
            source_entries.push(source);
        }
        let mut requested = BTreeSet::new();
        for requirement in &requirement_entries {
            if !requested.insert(requirement.span.clone()) {
                return Err(ArchiveError::DuplicateRequirement {
                    span: requirement.span.clone(),
                });
            }
        }
        let mut supplied = BTreeSet::new();
        for source in &source_entries {
            if source.span.data_kind != ArchiveDataKind::L2Book {
                return Err(ArchiveError::UnsupportedArchiveDataKind {
                    span: source.span.clone(),
                });
            }
            if !requested.contains(&source.span) {
                return Err(ArchiveError::UndeclaredSource {
                    span: source.span.clone(),
                });
            }
            if !supplied.insert(source.span.clone()) {
                return Err(ArchiveError::DuplicateSource {
                    span: source.span.clone(),
                });
            }
        }
        Ok(Self {
            as_of_ms,
            received_at,
            requirements: requirement_entries,
            sources: source_entries,
        })
    }
}

/// A bounded local archive reader with every source verified at open time.
#[derive(Debug)]
pub struct ArchiveReader {
    manifest: ArchiveManifest,
    sources: Vec<ResolvedSource>,
    limits: ArchiveLimits,
}

impl ArchiveReader {
    /// Opens only sources named by `manifest` beneath the resolved `source_root`.
    ///
    /// The resolved root is opened once, then each declared path component is
    /// opened relative to that descriptor with no-follow flags. Absolute paths,
    /// traversal, symlinks, non-regular files, manifest-layout mismatches, byte
    /// mismatches, and digest mismatches are rejected before decompression.
    ///
    /// # Errors
    ///
    /// Returns an error when a required source is absent or any source fails
    /// the local-path and compressed-metadata verification.
    pub fn open(
        source_root: impl AsRef<Path>,
        manifest: ArchiveManifest,
    ) -> Result<Self, ArchiveError> {
        Self::open_with_limits(source_root, manifest, ArchiveLimits::default())
    }

    fn open_with_limits(
        source_root: impl AsRef<Path>,
        manifest: ArchiveManifest,
        limits: ArchiveLimits,
    ) -> Result<Self, ArchiveError> {
        if manifest.sources.len() > MAX_OPEN_SOURCES {
            return Err(ArchiveError::TooManyOpenSources {
                count: manifest.sources.len(),
                max_sources: MAX_OPEN_SOURCES,
            });
        }
        let root = fs::canonicalize(source_root.as_ref()).map_err(|source| ArchiveError::Root {
            path: source_root.as_ref().to_path_buf(),
            source,
        })?;
        let root_fd = open(
            &root,
            OFlags::RDONLY
                | OFlags::DIRECTORY
                | OFlags::NOFOLLOW
                | OFlags::NONBLOCK
                | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|source| ArchiveError::Root {
            path: root.clone(),
            source: source.into(),
        })?;
        let metadata = fstat(&root_fd).map_err(|source| ArchiveError::Root {
            path: root.clone(),
            source: source.into(),
        })?;
        if !FileType::from_raw_mode(metadata.st_mode).is_dir() {
            return Err(ArchiveError::RootNotDirectory { path: root });
        }
        let root_fd = File::from(root_fd);

        let supplied = manifest
            .sources
            .iter()
            .map(|source| source.span.clone())
            .collect::<BTreeSet<_>>();
        for requirement in &manifest.requirements {
            if requirement.required && !supplied.contains(&requirement.span) {
                return Err(ArchiveError::MissingRequiredSource {
                    span: requirement.span.clone(),
                });
            }
        }

        let sources = manifest
            .sources
            .iter()
            .map(|source| resolve_source(&root, &root_fd, source))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            manifest,
            sources,
            limits,
        })
    }

    /// Streams all verified sources, returning deterministic event order and
    /// exact span accounting.
    ///
    /// # Errors
    ///
    /// Rejects malformed or truncated LZ4, oversized decoded input, unknown
    /// record versions, foreign timestamps, invalid live-wire records, and
    /// conflicting event identities.
    pub fn read_all(self) -> Result<ArchiveBatch, ArchiveError> {
        let Self {
            manifest,
            sources,
            limits,
        } = self;
        let present_spans = sources
            .iter()
            .map(|source| source.span.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut events = BTreeMap::<EventId, (MarketEvent, ArchiveSpan)>::new();
        let mut usage = ArchiveResourceUsage::default();
        for source in sources {
            let span = source.span.clone();
            let decoded = read_source(
                source,
                manifest.as_of_ms,
                manifest.received_at,
                limits,
                &mut usage,
            )?;
            for event in decoded {
                let event_id = event.event_id().clone();
                if let Some((previous, previous_span)) = events.get(&event_id) {
                    if previous != &event {
                        return Err(ArchiveError::ConflictingRecord {
                            event_id: event_id.as_str().to_owned(),
                            first_span: previous_span.clone(),
                            second_span: span.clone(),
                        });
                    }
                    continue;
                }
                events.insert(event_id, (event, span.clone()));
            }
        }

        let mut events = events
            .into_values()
            .map(|(event, _)| event)
            .collect::<Vec<_>>();
        events.sort();
        let supplied = present_spans.iter().cloned().collect::<BTreeSet<_>>();
        let missing_spans = manifest
            .requirements
            .iter()
            .filter(|requirement| !supplied.contains(&requirement.span))
            .map(|requirement| requirement.span.clone())
            .collect::<Vec<_>>();
        Ok(ArchiveBatch {
            content_digest: content_digest(&events)?,
            events,
            present_spans,
            missing_spans,
            conflicting_spans: Vec::new(),
        })
    }
}

/// Deterministic archive result and source-span accounting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveBatch {
    content_digest: ArchiveDigest,
    events: Vec<MarketEvent>,
    present_spans: Vec<ArchiveSpan>,
    missing_spans: Vec<ArchiveSpan>,
    conflicting_spans: Vec<ArchiveSpan>,
}

impl ArchiveBatch {
    /// Returns normalized events in canonical deterministic order.
    #[must_use]
    pub fn events(&self) -> &[MarketEvent] {
        &self.events
    }

    /// Returns verified source spans supplied to this replay.
    #[must_use]
    pub fn present_spans(&self) -> &[ArchiveSpan] {
        &self.present_spans
    }

    /// Returns optional requested spans that were not supplied.
    #[must_use]
    pub fn missing_spans(&self) -> &[ArchiveSpan] {
        &self.missing_spans
    }

    /// Returns conflicting spans. This is empty on success because conflicts
    /// are rejected rather than silently retained.
    #[must_use]
    pub fn conflicting_spans(&self) -> &[ArchiveSpan] {
        &self.conflicting_spans
    }

    /// Returns the BLAKE3 digest over sorted normalized event identities.
    #[must_use]
    pub const fn content_digest(&self) -> ArchiveDigest {
        self.content_digest
    }
}

/// Fixed resource ceilings applied while decoding one immutable archive batch.
#[derive(Debug, Clone, Copy)]
struct ArchiveLimits {
    max_total_decoded_bytes: u64,
    max_total_events: usize,
}

impl Default for ArchiveLimits {
    fn default() -> Self {
        Self {
            max_total_decoded_bytes: MAX_TOTAL_DECOMPRESSED_BYTES,
            max_total_events: MAX_TOTAL_EVENTS,
        }
    }
}

#[derive(Debug, Default)]
struct ArchiveResourceUsage {
    decoded_bytes: u64,
    decoded_events: usize,
}

impl ArchiveResourceUsage {
    fn record_decoded_bytes(
        &mut self,
        path: &Path,
        bytes: usize,
        limits: ArchiveLimits,
    ) -> Result<(), ArchiveError> {
        let bytes = u64::try_from(bytes).map_err(|_| ArchiveError::TotalDecodedBytesTooLarge {
            path: path.to_path_buf(),
            max_bytes: limits.max_total_decoded_bytes,
        })?;
        self.decoded_bytes = self.decoded_bytes.checked_add(bytes).ok_or_else(|| {
            ArchiveError::TotalDecodedBytesTooLarge {
                path: path.to_path_buf(),
                max_bytes: limits.max_total_decoded_bytes,
            }
        })?;
        if self.decoded_bytes > limits.max_total_decoded_bytes {
            return Err(ArchiveError::TotalDecodedBytesTooLarge {
                path: path.to_path_buf(),
                max_bytes: limits.max_total_decoded_bytes,
            });
        }
        Ok(())
    }

    fn record_event(&mut self, path: &Path, limits: ArchiveLimits) -> Result<(), ArchiveError> {
        self.decoded_events = self.decoded_events.checked_add(1).ok_or_else(|| {
            ArchiveError::TotalDecodedEventsTooLarge {
                path: path.to_path_buf(),
                max_events: limits.max_total_events,
            }
        })?;
        if self.decoded_events > limits.max_total_events {
            return Err(ArchiveError::TotalDecodedEventsTooLarge {
                path: path.to_path_buf(),
                max_events: limits.max_total_events,
            });
        }
        Ok(())
    }
}

/// A local source, archive structure, decompression, or record failure.
#[derive(Debug, Error)]
pub enum ArchiveError {
    /// The supplied root could not be resolved.
    #[error("cannot resolve archive root `{path}`: {source}")]
    Root {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The resolved root was not a directory.
    #[error("archive root `{path}` is not a directory")]
    RootNotDirectory { path: PathBuf },
    /// The requested interval was not an exact UTC archive hour.
    #[error("archive span [{start_ms}, {end_ms}) must be one nonnegative aligned hour")]
    InvalidSpan { start_ms: i64, end_ms: i64 },
    /// As-of time could not be converted to a core timestamp.
    #[error("archive as-of time `{as_of_ms}` is outside the supported timestamp range")]
    InvalidAsOf { as_of_ms: i64 },
    /// The manifest exceeded the bounded number of source requirements.
    #[error("archive manifest has {count} requirements, exceeding {max_requirements}")]
    TooManyRequirements {
        count: usize,
        max_requirements: usize,
    },
    /// The manifest exceeded the bounded number of declared source objects.
    #[error("archive manifest has {count} sources, exceeding {max_sources}")]
    TooManySources { count: usize, max_sources: usize },
    /// Declared compressed sources exceeded the total immutable reader budget.
    #[error("archive manifest compressed bytes exceed {max_bytes}")]
    TotalCompressedBytesTooLarge { max_bytes: u64 },
    /// Opening the declared sources would retain too many file descriptors.
    #[error("archive reader would retain {count} source descriptors, exceeding {max_sources}")]
    TooManyOpenSources { count: usize, max_sources: usize },
    /// A requirement named the same span more than once.
    #[error("duplicate archive requirement for `{span}")]
    DuplicateRequirement { span: ArchiveSpan },
    /// A manifest included more than one source for one exact span.
    #[error("duplicate archive source for `{span}")]
    DuplicateSource { span: ArchiveSpan },
    /// A source did not belong to a requested span.
    #[error("archive source was not declared as a requirement: `{span}")]
    UndeclaredSource { span: ArchiveSpan },
    /// A manifest tried to source data unavailable from the documented archive.
    #[error("the official historical archive has no source object for `{span}")]
    UnsupportedArchiveDataKind { span: ArchiveSpan },
    /// A required source object was not named in the manifest.
    #[error("missing required archive source for `{span}")]
    MissingRequiredSource { span: ArchiveSpan },
    /// The manifest path was absolute, empty, or contained non-normal components.
    #[error("unsafe archive source path `{path}`")]
    UnsafePath { path: PathBuf },
    /// The declared path did not match the fixed official archive object layout.
    #[error("archive source path `{declared}` does not match official layout `{expected}`")]
    PathManifestMismatch {
        declared: PathBuf,
        expected: PathBuf,
    },
    /// A source path component was a symlink.
    #[error("archive source path contains symlink `{path}`")]
    Symlink { path: PathBuf },
    /// The source could not be opened or inspected after manifest validation.
    #[error("cannot inspect archive source `{path}`: {source}")]
    SourceIo {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The declared source was not a regular file.
    #[error("archive source `{path}` is not a regular file")]
    SourceNotFile { path: PathBuf },
    /// A compressed source exceeded the fixed reader bound.
    #[error("archive source `{path}` has {bytes} compressed bytes, exceeding {max_bytes}")]
    CompressedSourceTooLarge {
        path: PathBuf,
        bytes: u64,
        max_bytes: u64,
    },
    /// Compressed byte metadata disagreed with the local file.
    #[error("archive source `{path}` has {actual} bytes, expected {expected}")]
    CompressedLengthMismatch {
        path: PathBuf,
        expected: u64,
        actual: u64,
    },
    /// Compressed content digest disagreed with the explicit manifest.
    #[error("archive source `{path}` has digest {actual}, expected {expected}")]
    CompressedDigestMismatch {
        path: PathBuf,
        expected: ArchiveDigest,
        actual: ArchiveDigest,
    },
    /// The LZ4 frame was unreadable or truncated.
    #[error("cannot decompress archive source `{path}`: {source}")]
    Decompression {
        path: PathBuf,
        source: std::io::Error,
    },
    /// Decoded source bytes exceeded the fixed streaming bound.
    #[error("archive source `{path}` decoded beyond {max_bytes} bytes")]
    DecompressedSourceTooLarge { path: PathBuf, max_bytes: u64 },
    /// All decoded sources together exceeded the bounded replay byte budget.
    #[error("archive sources decoded beyond {max_bytes} bytes while reading `{path}`")]
    TotalDecodedBytesTooLarge { path: PathBuf, max_bytes: u64 },
    /// All decoded sources together exceeded the bounded event budget.
    #[error("archive sources decoded beyond {max_events} events while reading `{path}`")]
    TotalDecodedEventsTooLarge { path: PathBuf, max_events: usize },
    /// One JSON line exceeded the fixed record bound.
    #[error("archive source `{path}` line {line} exceeds {max_bytes} bytes")]
    RecordTooLarge {
        path: PathBuf,
        line: usize,
        max_bytes: usize,
    },
    /// A record did not contain a supported version.
    #[error("archive source `{path}` line {line} uses unsupported record version")]
    UnknownRecordVersion { path: PathBuf, line: usize },
    /// A record did not match the source channel or live wire shape.
    #[error("invalid {kind} record in `{path}` line {line}")]
    InvalidRecord {
        path: PathBuf,
        line: usize,
        kind: ArchiveDataKind,
    },
    /// A record time was outside the exact source hour.
    #[error("archive source `{path}` line {line} has time {time_ms} outside `{span}")]
    RecordOutsideSpan {
        path: PathBuf,
        line: usize,
        time_ms: i64,
        span: ArchiveSpan,
    },
    /// A record was newer than the manifest's explicit replay time.
    #[error("archive source `{path}` line {line} has future time {time_ms} after as-of {as_of_ms}")]
    FutureRecord {
        path: PathBuf,
        line: usize,
        time_ms: i64,
        as_of_ms: i64,
    },
    /// Two records with one canonical identity had different normalized content.
    #[error("conflicting archive records for {event_id} in `{first_span}` and `{second_span}")]
    ConflictingRecord {
        event_id: String,
        first_span: ArchiveSpan,
        second_span: ArchiveSpan,
    },
    /// The archive normalization boundary emitted a non-book market event.
    #[error("archive normalization emitted unsupported event kind for {event_id}")]
    UnexpectedEventKind { event_id: String },
}

#[derive(Debug)]
struct ResolvedSource {
    compressed_bytes: u64,
    compressed_digest: ArchiveDigest,
    file: File,
    path: PathBuf,
    span: ArchiveSpan,
}

fn resolve_source(
    root: &Path,
    root_fd: &File,
    source: &ArchiveSource,
) -> Result<ResolvedSource, ArchiveError> {
    validate_relative_path(&source.relative_path)?;
    let expected = source.span.official_relative_path()?;
    if source.relative_path != expected {
        return Err(ArchiveError::PathManifestMismatch {
            declared: source.relative_path.clone(),
            expected,
        });
    }

    let path = root.join(&source.relative_path);
    let mut directory = root_fd
        .try_clone()
        .map_err(|source| ArchiveError::SourceIo {
            path: root.to_path_buf(),
            source,
        })?;
    let mut components = source.relative_path.components().peekable();
    let file = loop {
        let Some(component) = components.next() else {
            return Err(ArchiveError::UnsafePath {
                path: source.relative_path.clone(),
            });
        };
        let Component::Normal(component) = component else {
            return Err(ArchiveError::UnsafePath {
                path: source.relative_path.clone(),
            });
        };
        let flags = if components.peek().is_some() {
            OFlags::RDONLY
                | OFlags::DIRECTORY
                | OFlags::NOFOLLOW
                | OFlags::NONBLOCK
                | OFlags::CLOEXEC
        } else {
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC
        };
        let opened = openat(&directory, component, flags, Mode::empty())
            .map_err(|source| secure_open_error(&path, source))?;
        if components.peek().is_none() {
            break File::from(opened);
        }
        directory = File::from(opened);
    };
    let metadata = fstat(&file).map_err(|source| ArchiveError::SourceIo {
        path: path.clone(),
        source: source.into(),
    })?;
    if !FileType::from_raw_mode(metadata.st_mode).is_file() {
        return Err(ArchiveError::SourceNotFile { path });
    }
    let file_bytes = u64::try_from(metadata.st_size)
        .map_err(|_| ArchiveError::SourceNotFile { path: path.clone() })?;
    if file_bytes > MAX_COMPRESSED_SOURCE_BYTES {
        return Err(ArchiveError::CompressedSourceTooLarge {
            path,
            bytes: file_bytes,
            max_bytes: MAX_COMPRESSED_SOURCE_BYTES,
        });
    }
    if file_bytes != source.compressed_bytes {
        return Err(ArchiveError::CompressedLengthMismatch {
            path,
            expected: source.compressed_bytes,
            actual: file_bytes,
        });
    }
    let mut file = file;
    let actual = digest_file(&mut file, &path)?;
    if actual != source.compressed_digest {
        return Err(ArchiveError::CompressedDigestMismatch {
            path,
            expected: source.compressed_digest,
            actual,
        });
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|source| ArchiveError::SourceIo {
            path: path.clone(),
            source,
        })?;
    Ok(ResolvedSource {
        compressed_bytes: source.compressed_bytes,
        compressed_digest: source.compressed_digest,
        file,
        path,
        span: source.span.clone(),
    })
}

fn secure_open_error(path: &Path, source: rustix::io::Errno) -> ArchiveError {
    if source == rustix::io::Errno::LOOP {
        return ArchiveError::Symlink {
            path: path.to_path_buf(),
        };
    }
    let source = std::io::Error::from(source);
    ArchiveError::SourceIo {
        path: path.to_path_buf(),
        source,
    }
}

fn validate_relative_path(path: &Path) -> Result<(), ArchiveError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ArchiveError::UnsafePath {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn digest_file(file: &mut File, path: &Path) -> Result<ArchiveDigest, ArchiveError> {
    let mut hasher = Hasher::new();
    let mut buffer = [0_u8; DIGEST_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| ArchiveError::SourceIo {
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            return Ok(ArchiveDigest::from_hasher(hasher));
        }
        hasher.update(&buffer[..read]);
    }
}

fn read_source(
    source: ResolvedSource,
    as_of_ms: i64,
    received_at: TimestampNs,
    limits: ArchiveLimits,
    usage: &mut ArchiveResourceUsage,
) -> Result<Vec<MarketEvent>, ArchiveError> {
    let ResolvedSource {
        compressed_bytes,
        compressed_digest,
        file,
        path,
        span,
    } = source;
    let decoder = FrameDecoder::new(DigestingReader::new(file));
    let mut reader = BufReader::with_capacity(DIGEST_BUFFER_BYTES, decoder);
    let mut decoded_bytes = 0_u64;
    let mut line_number = 0_usize;
    let mut events = Vec::new();
    while let Some(record) = read_bounded_line(
        &mut reader,
        &path,
        line_number.saturating_add(1),
        &mut decoded_bytes,
        limits,
        usage,
    )? {
        line_number = line_number.saturating_add(1);
        if record.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let event = decode_record(&record, &path, &span, line_number, as_of_ms, received_at)?;
        if events.len() == MAX_EVENTS_PER_SOURCE {
            return Err(ArchiveError::DecompressedSourceTooLarge {
                path: path.clone(),
                max_bytes: MAX_DECOMPRESSED_SOURCE_BYTES,
            });
        }
        usage.record_event(&path, limits)?;
        events.push(event);
    }
    let mut decoder = reader.into_inner();
    let trailing = decoder
        .fill_buf()
        .map_err(|source| ArchiveError::Decompression {
            path: path.clone(),
            source,
        })?;
    if !trailing.is_empty() {
        return Err(incomplete_lz4_frame(&path));
    }
    let mut compressed = decoder.into_inner();
    compressed.drain_to_end(&path)?;
    if !compressed.has_complete_lz4_terminal() {
        return Err(incomplete_lz4_frame(&path));
    }
    verify_streamed_source(&path, compressed_bytes, compressed_digest, compressed)?;
    Ok(events)
}

struct DigestingReader<R> {
    bytes_read: u64,
    first_bytes: [u8; 5],
    first_bytes_len: usize,
    hasher: Hasher,
    inner: R,
    trailing_bytes: [u8; 8],
    trailing_bytes_len: usize,
}

impl<R> DigestingReader<R> {
    fn new(inner: R) -> Self {
        Self {
            bytes_read: 0,
            first_bytes: [0; 5],
            first_bytes_len: 0,
            hasher: Hasher::new(),
            inner,
            trailing_bytes: [0; 8],
            trailing_bytes_len: 0,
        }
    }

    fn drain_to_end(&mut self, path: &Path) -> Result<(), ArchiveError>
    where
        R: Read,
    {
        let mut buffer = [0_u8; DIGEST_BUFFER_BYTES];
        while self
            .read(&mut buffer)
            .map_err(|source| ArchiveError::SourceIo {
                path: path.to_path_buf(),
                source,
            })?
            != 0
        {}
        Ok(())
    }

    fn has_complete_lz4_terminal(&self) -> bool {
        if self.first_bytes_len < self.first_bytes.len() || self.first_bytes[..4] != LZ4_FRAME_MAGIC
        {
            return false;
        }
        let terminal_length = if self.first_bytes[4] & LZ4_CONTENT_CHECKSUM_FLAG != 0 {
            8
        } else {
            4
        };
        if self.trailing_bytes_len < terminal_length {
            return false;
        }
        let terminal_start = self.trailing_bytes_len - terminal_length;
        self.trailing_bytes[terminal_start..terminal_start + 4]
            .iter()
            .all(|byte| *byte == 0)
    }

    fn finish(self) -> (u64, ArchiveDigest) {
        (self.bytes_read, ArchiveDigest::from_hasher(self.hasher))
    }

    fn observe(&mut self, bytes: &[u8]) {
        let prefix_remaining = self.first_bytes.len() - self.first_bytes_len;
        let prefix_len = prefix_remaining.min(bytes.len());
        self.first_bytes[self.first_bytes_len..self.first_bytes_len + prefix_len]
            .copy_from_slice(&bytes[..prefix_len]);
        self.first_bytes_len += prefix_len;

        let trailing_capacity = self.trailing_bytes.len();
        if bytes.len() >= trailing_capacity {
            self.trailing_bytes
                .copy_from_slice(&bytes[bytes.len() - trailing_capacity..]);
            self.trailing_bytes_len = trailing_capacity;
            return;
        }
        let retained = self.trailing_bytes_len.min(trailing_capacity - bytes.len());
        self.trailing_bytes.copy_within(
            self.trailing_bytes_len - retained..self.trailing_bytes_len,
            0,
        );
        self.trailing_bytes[retained..retained + bytes.len()].copy_from_slice(bytes);
        self.trailing_bytes_len = retained + bytes.len();
    }
}

impl<R: Read> Read for DigestingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.bytes_read = self.bytes_read.saturating_add(read as u64);
        self.hasher.update(&buffer[..read]);
        self.observe(&buffer[..read]);
        Ok(read)
    }
}

fn incomplete_lz4_frame(path: &Path) -> ArchiveError {
    ArchiveError::Decompression {
        path: path.to_path_buf(),
        source: std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "archive LZ4 frame did not end with a complete terminal marker",
        ),
    }
}

fn verify_streamed_source(
    path: &Path,
    expected_bytes: u64,
    expected_digest: ArchiveDigest,
    compressed: DigestingReader<File>,
) -> Result<(), ArchiveError> {
    let (actual_bytes, actual_digest) = compressed.finish();
    if actual_bytes != expected_bytes {
        return Err(ArchiveError::CompressedLengthMismatch {
            path: path.to_path_buf(),
            expected: expected_bytes,
            actual: actual_bytes,
        });
    }
    if actual_digest != expected_digest {
        return Err(ArchiveError::CompressedDigestMismatch {
            path: path.to_path_buf(),
            expected: expected_digest,
            actual: actual_digest,
        });
    }
    Ok(())
}

fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    path: &Path,
    line: usize,
    decoded_bytes: &mut u64,
    limits: ArchiveLimits,
    usage: &mut ArchiveResourceUsage,
) -> Result<Option<Vec<u8>>, ArchiveError> {
    let mut record = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .map_err(|source| ArchiveError::Decompression {
                path: path.to_path_buf(),
                source,
            })?;
        if available.is_empty() {
            return if record.is_empty() {
                Ok(None)
            } else {
                Ok(Some(record))
            };
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position.saturating_add(1));
        *decoded_bytes = decoded_bytes
            .checked_add(u64::try_from(take).map_err(|_| {
                ArchiveError::DecompressedSourceTooLarge {
                    path: path.to_path_buf(),
                    max_bytes: MAX_DECOMPRESSED_SOURCE_BYTES,
                }
            })?)
            .ok_or_else(|| ArchiveError::DecompressedSourceTooLarge {
                path: path.to_path_buf(),
                max_bytes: MAX_DECOMPRESSED_SOURCE_BYTES,
            })?;
        if *decoded_bytes > MAX_DECOMPRESSED_SOURCE_BYTES {
            return Err(ArchiveError::DecompressedSourceTooLarge {
                path: path.to_path_buf(),
                max_bytes: MAX_DECOMPRESSED_SOURCE_BYTES,
            });
        }
        usage.record_decoded_bytes(path, take, limits)?;
        if record.len().saturating_add(take) > MAX_RECORD_BYTES {
            return Err(ArchiveError::RecordTooLarge {
                path: path.to_path_buf(),
                line,
                max_bytes: MAX_RECORD_BYTES,
            });
        }
        let has_newline = available[take - 1] == b'\n';
        record.extend_from_slice(&available[..take]);
        reader.consume(take);
        if has_newline {
            record.pop();
            return Ok(Some(record));
        }
    }
}

fn decode_record(
    raw: &[u8],
    path: &Path,
    span: &ArchiveSpan,
    line: usize,
    as_of_ms: i64,
    received_at: TimestampNs,
) -> Result<MarketEvent, ArchiveError> {
    let record = serde_json::from_slice::<Value>(raw).map_err(|_| ArchiveError::InvalidRecord {
        path: path.to_path_buf(),
        line,
        kind: span.data_kind,
    })?;
    let data = extract_record_data(record, path, span, line)?;
    validate_record_version(&data, path, line)?;
    let time_ms =
        data.get("time")
            .and_then(Value::as_i64)
            .ok_or_else(|| ArchiveError::InvalidRecord {
                path: path.to_path_buf(),
                line,
                kind: span.data_kind,
            })?;
    if time_ms > as_of_ms {
        return Err(ArchiveError::FutureRecord {
            path: path.to_path_buf(),
            line,
            time_ms,
            as_of_ms,
        });
    }
    if !(span.start_ms..span.end_ms).contains(&time_ms) {
        return Err(ArchiveError::RecordOutsideSpan {
            path: path.to_path_buf(),
            line,
            time_ms,
            span: span.clone(),
        });
    }
    if span.data_kind != ArchiveDataKind::L2Book {
        return Err(ArchiveError::UnsupportedArchiveDataKind { span: span.clone() });
    }
    normalize_l2_book_wire_for_market(data, &span.market, received_at).map_err(|_| {
        ArchiveError::InvalidRecord {
            path: path.to_path_buf(),
            line,
            kind: span.data_kind,
        }
    })
}

fn extract_record_data(
    record: Value,
    path: &Path,
    span: &ArchiveSpan,
    line: usize,
) -> Result<Value, ArchiveError> {
    validate_record_version(&record, path, line)?;
    let Some(channel) = record.get("channel") else {
        return Ok(record);
    };
    if channel.as_str() != Some("l2Book") {
        return Err(ArchiveError::InvalidRecord {
            path: path.to_path_buf(),
            line,
            kind: span.data_kind,
        });
    }
    record
        .get("data")
        .cloned()
        .ok_or_else(|| ArchiveError::InvalidRecord {
            path: path.to_path_buf(),
            line,
            kind: span.data_kind,
        })
}

fn validate_record_version(record: &Value, path: &Path, line: usize) -> Result<(), ArchiveError> {
    if record
        .get("version")
        .is_some_and(|version| version.as_u64() != Some(1))
    {
        return Err(ArchiveError::UnknownRecordVersion {
            path: path.to_path_buf(),
            line,
        });
    }
    Ok(())
}

fn timestamp_from_millis(value: i64) -> Option<TimestampNs> {
    let nanoseconds = i128::from(value).checked_mul(1_000_000)?;
    TimestampNs::new(nanoseconds).ok()
}

fn content_digest(events: &[MarketEvent]) -> Result<ArchiveDigest, ArchiveError> {
    let mut hasher = Hasher::new_derive_key(CONTENT_DIGEST_DOMAIN);
    update_digest_field(&mut hasher, &(events.len() as u64).to_be_bytes());
    for event in events {
        update_digest_field(&mut hasher, event.event_id().as_str().as_bytes());
        update_digest_field(&mut hasher, &event.event_time().value().to_be_bytes());
        update_digest_field(&mut hasher, &event.received_at().value().to_be_bytes());
        update_digest_field(&mut hasher, event.market().as_str().as_bytes());
        match event.kind() {
            MarketEventKind::BookSnapshot(snapshot) => {
                hasher.update(&[0]);
                update_digest_field(&mut hasher, &snapshot.sequence().to_be_bytes());
                update_digest_field(&mut hasher, &(snapshot.bids().len() as u64).to_be_bytes());
                snapshot
                    .bids()
                    .iter()
                    .for_each(|level| update_book_level_digest(&mut hasher, level));
                update_digest_field(&mut hasher, &(snapshot.asks().len() as u64).to_be_bytes());
                snapshot
                    .asks()
                    .iter()
                    .for_each(|level| update_book_level_digest(&mut hasher, level));
            }
            _ => {
                return Err(ArchiveError::UnexpectedEventKind {
                    event_id: event.event_id().as_str().to_owned(),
                });
            }
        }
    }
    Ok(ArchiveDigest::from_hasher(hasher))
}

fn update_book_level_digest(hasher: &mut Hasher, level: &BookLevel) {
    update_digest_field(hasher, level.price().value().to_string().as_bytes());
    update_digest_field(hasher, level.quantity().value().to_string().as_bytes());
}

fn update_digest_field(hasher: &mut Hasher, field: &[u8]) {
    hasher.update(&(field.len() as u64).to_be_bytes());
    hasher.update(field);
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::BufReader;
    use std::path::{Path, PathBuf};

    use tempfile::TempDir;
    use trench_core::domain::Market;
    use trench_core::event::MarketEventKind;

    use super::{
        ArchiveDataKind, ArchiveDigest, ArchiveError, ArchiveLimits, ArchiveManifest,
        ArchiveReader, ArchiveRequirement, ArchiveResourceUsage, ArchiveSource, ArchiveSpan,
        read_bounded_line,
    };

    const HOUR_START_MS: i64 = 1_694_854_800_000;
    const HOUR_END_MS: i64 = 1_694_858_400_000;
    const AS_OF_MS: i64 = 1_694_862_000_000;
    const L2_FIXTURE: &[u8] = include_bytes!("../../../tests/fixtures/archive/l2-sample.lz4");

    fn market() -> Market {
        Market::new("SOL").expect("fixture market must be valid")
    }

    fn span(data_kind: ArchiveDataKind) -> ArchiveSpan {
        ArchiveSpan::new(market(), data_kind, HOUR_START_MS, HOUR_END_MS)
            .expect("fixture span must be valid")
    }

    fn installed_l2_source(root: &TempDir) -> ArchiveSource {
        let relative_path = PathBuf::from("market_data/20230916/9/l2Book/SOL.lz4");
        let destination = root.path().join(&relative_path);
        fs::create_dir_all(
            destination
                .parent()
                .expect("fixture archive path must have a parent"),
        )
        .expect("create fixture archive directories");
        fs::write(&destination, L2_FIXTURE).expect("write immutable L2 fixture");
        ArchiveSource::new(
            span(ArchiveDataKind::L2Book),
            relative_path,
            u64::try_from(L2_FIXTURE.len()).expect("fixture length fits u64"),
            ArchiveDigest::of_bytes(L2_FIXTURE),
        )
    }

    #[test]
    fn reader_normalizes_the_documented_l2_fixture() {
        let root = TempDir::new().expect("create archive root");
        let source = installed_l2_source(&root);
        let manifest = ArchiveManifest::new(
            AS_OF_MS,
            [ArchiveRequirement::required(span(ArchiveDataKind::L2Book))],
            [source],
        )
        .expect("fixture manifest must be valid");

        let batch = ArchiveReader::open(root.path(), manifest)
            .expect("fixture source must open")
            .read_all()
            .expect("fixture source must decode");

        assert_eq!(batch.events().len(), 2);
        assert!(matches!(
            batch.events()[0].kind(),
            MarketEventKind::BookSnapshot(_)
        ));
    }

    #[test]
    fn manifest_rejects_an_undocumented_bbo_object() {
        let bbo_span = span(ArchiveDataKind::Bbo);
        let source = ArchiveSource::new(
            bbo_span.clone(),
            PathBuf::from("market_data/20230916/9/bbo/SOL.lz4"),
            0,
            ArchiveDigest::of_bytes(b""),
        );

        let error =
            ArchiveManifest::new(AS_OF_MS, [ArchiveRequirement::required(bbo_span)], [source])
                .expect_err("the documented archive has no BBO object path");

        assert!(matches!(
            error,
            ArchiveError::UnsupportedArchiveDataKind { .. }
        ));
    }

    #[test]
    fn decoded_bytes_are_capped_across_the_opened_batch() {
        let mut reader = BufReader::new(b"bounded\n".as_slice());
        let mut decoded_bytes = 0;
        let mut usage = ArchiveResourceUsage::default();
        let limits = ArchiveLimits {
            max_total_decoded_bytes: 7,
            max_total_events: 1,
        };

        let error = read_bounded_line(
            &mut reader,
            Path::new("archive.lz4"),
            1,
            &mut decoded_bytes,
            limits,
            &mut usage,
        )
        .expect_err("decoded byte caps must apply before a record is retained");

        assert!(matches!(
            error,
            ArchiveError::TotalDecodedBytesTooLarge { max_bytes: 7, .. }
        ));
    }
}
