//! Bounded local readers for explicitly downloaded official market archives.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};

use blake3::Hasher;
use lz4_flex::frame::FrameDecoder;
use serde_json::Value;
use thiserror::Error;
use time::OffsetDateTime;
use trench_core::domain::{EventId, Market};
use trench_core::event::{BookLevel, MarketEvent, MarketEventKind, TimestampNs};

use crate::ws::{normalize_bbo_wire_for_market, normalize_l2_book_wire_for_market};

const HOUR_MILLIS: i64 = 3_600_000;
const MAX_COMPRESSED_SOURCE_BYTES: u64 = 1_073_741_824;
const MAX_DECOMPRESSED_SOURCE_BYTES: usize = 4 * 1_024 * 1_024 * 1_024;
const MAX_RECORD_BYTES: usize = 1_024 * 1_024;
const MAX_EVENTS_PER_SOURCE: usize = 5_000_000;
const DIGEST_BUFFER_BYTES: usize = 64 * 1_024;
const CONTENT_DIGEST_DOMAIN: &str = "trench.archive.content.v1";

/// The market-data channel contained by a local archive object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ArchiveDataKind {
    /// Full L2 snapshots from the documented `l2Book` archive directory.
    L2Book,
    /// Best-bid/best-ask records encoded with the live public `bbo` wire shape.
    Bbo,
}

impl ArchiveDataKind {
    const fn directory(self) -> &'static str {
        match self {
            Self::L2Book => "l2Book",
            Self::Bbo => "bbo",
        }
    }

    const fn channel(self) -> &'static str {
        self.directory()
    }
}

impl fmt::Display for ArchiveDataKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.directory())
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
            self.data_kind.directory(),
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
        let requirements = requirements.into_iter().collect::<Vec<_>>();
        let sources = sources.into_iter().collect::<Vec<_>>();
        let mut requested = BTreeSet::new();
        for requirement in &requirements {
            if !requested.insert(requirement.span.clone()) {
                return Err(ArchiveError::DuplicateRequirement {
                    span: requirement.span.clone(),
                });
            }
        }
        let mut supplied = BTreeSet::new();
        for source in &sources {
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
            requirements,
            sources,
        })
    }
}

/// A bounded local archive reader with every source verified at open time.
#[derive(Debug)]
pub struct ArchiveReader {
    manifest: ArchiveManifest,
    sources: Vec<ResolvedSource>,
}

impl ArchiveReader {
    /// Opens only sources named by `manifest` beneath the resolved `source_root`.
    ///
    /// Every component is checked with `symlink_metadata`; absolute paths,
    /// traversal, symlinks, manifest-layout mismatches, byte mismatches, and
    /// digest mismatches are rejected before any decompression begins.
    ///
    /// # Errors
    ///
    /// Returns an error when a required source is absent or any source fails
    /// the local-path and compressed-metadata verification.
    pub fn open(
        source_root: impl AsRef<Path>,
        manifest: ArchiveManifest,
    ) -> Result<Self, ArchiveError> {
        let root = fs::canonicalize(source_root.as_ref()).map_err(|source| ArchiveError::Root {
            path: source_root.as_ref().to_path_buf(),
            source,
        })?;
        let metadata = fs::metadata(&root).map_err(|source| ArchiveError::Root {
            path: root.clone(),
            source,
        })?;
        if !metadata.is_dir() {
            return Err(ArchiveError::RootNotDirectory { path: root });
        }

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
            .map(|source| resolve_source(&root, source))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { manifest, sources })
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
        let Self { manifest, sources } = self;
        let present_spans = sources
            .iter()
            .map(|source| source.span.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut events = BTreeMap::<EventId, (MarketEvent, ArchiveSpan)>::new();
        for source in sources {
            let span = source.span.clone();
            let decoded = read_source(source, manifest.as_of_ms, manifest.received_at)?;
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
    /// A requirement named the same span more than once.
    #[error("duplicate archive requirement for `{span}")]
    DuplicateRequirement { span: ArchiveSpan },
    /// A manifest included more than one source for one exact span.
    #[error("duplicate archive source for `{span}")]
    DuplicateSource { span: ArchiveSpan },
    /// A source did not belong to a requested span.
    #[error("archive source was not declared as a requirement: `{span}")]
    UndeclaredSource { span: ArchiveSpan },
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
    /// The resolved source escaped the canonical root.
    #[error("archive source `{path}` escaped root `{root}`")]
    SourceOutsideRoot { path: PathBuf, root: PathBuf },
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
    DecompressedSourceTooLarge { path: PathBuf, max_bytes: usize },
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
    file: File,
    path: PathBuf,
    span: ArchiveSpan,
}

fn resolve_source(root: &Path, source: &ArchiveSource) -> Result<ResolvedSource, ArchiveError> {
    validate_relative_path(&source.relative_path)?;
    let expected = source.span.official_relative_path()?;
    if source.relative_path != expected {
        return Err(ArchiveError::PathManifestMismatch {
            declared: source.relative_path.clone(),
            expected,
        });
    }

    let mut path = root.to_path_buf();
    for component in source.relative_path.components() {
        let Component::Normal(component) = component else {
            return Err(ArchiveError::UnsafePath {
                path: source.relative_path.clone(),
            });
        };
        path.push(component);
        let metadata = fs::symlink_metadata(&path).map_err(|source| ArchiveError::SourceIo {
            path: path.clone(),
            source,
        })?;
        if metadata.file_type().is_symlink() {
            return Err(ArchiveError::Symlink { path });
        }
    }
    let canonical = fs::canonicalize(&path).map_err(|source| ArchiveError::SourceIo {
        path: path.clone(),
        source,
    })?;
    if !canonical.starts_with(root) {
        return Err(ArchiveError::SourceOutsideRoot {
            path: canonical,
            root: root.to_path_buf(),
        });
    }
    let mut file = File::open(&canonical).map_err(|source| ArchiveError::SourceIo {
        path: canonical.clone(),
        source,
    })?;
    let metadata = file.metadata().map_err(|source| ArchiveError::SourceIo {
        path: canonical.clone(),
        source,
    })?;
    if !metadata.is_file() {
        return Err(ArchiveError::SourceNotFile { path: canonical });
    }
    if metadata.len() > MAX_COMPRESSED_SOURCE_BYTES {
        return Err(ArchiveError::CompressedSourceTooLarge {
            path: canonical,
            bytes: metadata.len(),
            max_bytes: MAX_COMPRESSED_SOURCE_BYTES,
        });
    }
    if metadata.len() != source.compressed_bytes {
        return Err(ArchiveError::CompressedLengthMismatch {
            path: canonical,
            expected: source.compressed_bytes,
            actual: metadata.len(),
        });
    }
    let actual = digest_file(&mut file, &canonical)?;
    if actual != source.compressed_digest {
        return Err(ArchiveError::CompressedDigestMismatch {
            path: canonical,
            expected: source.compressed_digest,
            actual,
        });
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|source| ArchiveError::SourceIo {
            path: canonical.clone(),
            source,
        })?;
    Ok(ResolvedSource {
        file,
        path: canonical,
        span: source.span.clone(),
    })
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
) -> Result<Vec<MarketEvent>, ArchiveError> {
    let ResolvedSource { file, path, span } = source;
    let decoder = FrameDecoder::new(BufReader::with_capacity(DIGEST_BUFFER_BYTES, file));
    let mut reader = BufReader::with_capacity(DIGEST_BUFFER_BYTES, decoder);
    let mut decoded_bytes = 0_usize;
    let mut line_number = 0_usize;
    let mut events = Vec::new();
    while let Some(record) = read_bounded_line(
        &mut reader,
        &path,
        line_number.saturating_add(1),
        &mut decoded_bytes,
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
        events.push(event);
    }
    Ok(events)
}

fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    path: &Path,
    line: usize,
    decoded_bytes: &mut usize,
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
        *decoded_bytes = decoded_bytes.saturating_add(take);
        if *decoded_bytes > MAX_DECOMPRESSED_SOURCE_BYTES {
            return Err(ArchiveError::DecompressedSourceTooLarge {
                path: path.to_path_buf(),
                max_bytes: MAX_DECOMPRESSED_SOURCE_BYTES,
            });
        }
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
    match span.data_kind {
        ArchiveDataKind::L2Book => {
            normalize_l2_book_wire_for_market(data, &span.market, received_at)
        }
        ArchiveDataKind::Bbo => normalize_bbo_wire_for_market(data, &span.market, received_at),
    }
    .map_err(|_| ArchiveError::InvalidRecord {
        path: path.to_path_buf(),
        line,
        kind: span.data_kind,
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
    if channel.as_str() != Some(span.data_kind.channel()) {
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
            MarketEventKind::Bbo(bbo) => {
                hasher.update(&[1]);
                update_digest_field(&mut hasher, &bbo.sequence().to_be_bytes());
                update_book_level_digest(&mut hasher, &bbo.bid());
                update_book_level_digest(&mut hasher, &bbo.ask());
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
