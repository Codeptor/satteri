use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use lz4_flex::frame::FrameEncoder;
use rust_decimal::Decimal;
use tempfile::TempDir;
use trench_core::domain::Market;
use trench_core::event::MarketEventKind;
use trench_hyperliquid::{
    ArchiveDataKind, ArchiveDigest, ArchiveManifest, ArchiveReader, ArchiveRequirement,
    ArchiveSource, ArchiveSpan,
};

const HOUR_START_MS: i64 = 1_694_854_800_000;
const HOUR_END_MS: i64 = 1_694_858_400_000;
const AS_OF_MS: i64 = 1_694_862_000_000;

fn market(value: &str) -> Market {
    Market::new(value).expect("fixture market must be valid")
}

fn l2_span() -> ArchiveSpan {
    ArchiveSpan::new(
        market("SOL"),
        ArchiveDataKind::L2Book,
        HOUR_START_MS,
        HOUR_END_MS,
    )
    .expect("fixture span must be valid")
}

fn bbo_span() -> ArchiveSpan {
    ArchiveSpan::new(
        market("SOL"),
        ArchiveDataKind::Bbo,
        HOUR_START_MS,
        HOUR_END_MS,
    )
    .expect("fixture span must be valid")
}

fn fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/archive/l2-sample.lz4")
}

#[test]
fn l2_fixture_is_the_expected_immutable_lz4_object() {
    let bytes = fs::read(fixture_path()).expect("read immutable LZ4 fixture");

    assert_eq!(bytes.len(), 173);
    assert_eq!(
        ArchiveDigest::of_bytes(&bytes).to_string(),
        "b3:b7fbf0b0473d3dfb5e32b824360e840978d47f8321b748c493585240b52fed6a"
    );
}

fn install_fixture(root: &TempDir) -> (PathBuf, ArchiveSource) {
    let relative_path = PathBuf::from("market_data/20230916/9/l2Book/SOL.lz4");
    let destination = root.path().join(&relative_path);
    fs::create_dir_all(
        destination
            .parent()
            .expect("fixture archive path has a parent"),
    )
    .expect("create fixture archive directories");
    fs::copy(fixture_path(), &destination).expect("copy immutable LZ4 fixture");
    let bytes = fs::read(&destination).expect("read copied fixture");
    let source = ArchiveSource::new(
        l2_span(),
        relative_path,
        u64::try_from(bytes.len()).expect("fixture length fits u64"),
        ArchiveDigest::of_bytes(&bytes),
    );
    (destination, source)
}

fn install_lz4(root: &TempDir, records: &[u8]) -> (PathBuf, ArchiveSource) {
    let relative_path = PathBuf::from("market_data/20230916/9/l2Book/SOL.lz4");
    let destination = root.path().join(&relative_path);
    fs::create_dir_all(
        destination
            .parent()
            .expect("fixture archive path has a parent"),
    )
    .expect("create fixture archive directories");
    write_lz4(&destination, records);
    let bytes = fs::read(&destination).expect("read compressed fixture");
    let source = ArchiveSource::new(
        l2_span(),
        relative_path,
        u64::try_from(bytes.len()).expect("fixture length fits u64"),
        ArchiveDigest::of_bytes(&bytes),
    );
    (destination, source)
}

fn write_lz4(destination: &Path, records: &[u8]) {
    let file = fs::File::create(destination).expect("create compressed fixture");
    let mut encoder = FrameEncoder::new(file);
    encoder
        .write_all(records)
        .expect("compress archive fixture records");
    encoder.finish().expect("finish compressed fixture");
}

#[test]
fn reader_normalizes_the_official_l2_fixture_in_event_order() {
    let root = TempDir::new().expect("create archive root");
    let (_, source) = install_fixture(&root);
    let span = l2_span();
    let manifest = ArchiveManifest::new(
        AS_OF_MS,
        [ArchiveRequirement::required(span.clone())],
        [source],
    )
    .expect("fixture manifest must be valid");

    let batch = ArchiveReader::open(root.path(), manifest)
        .expect("verified local source must open")
        .read_all()
        .expect("fixture must decode");

    assert_eq!(batch.events().len(), 2);
    assert!(batch.events().windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(batch.events()[0].market(), &market("SOL"));
    assert_eq!(
        batch.events()[0].event_time().value(),
        HOUR_START_MS * 1_000_000
    );
    assert_eq!(
        batch.events()[1].event_time().value(),
        (HOUR_START_MS + 1) * 1_000_000
    );
    let MarketEventKind::BookSnapshot(first) = batch.events()[0].kind() else {
        panic!("fixture must normalize into an L2 snapshot");
    };
    assert_eq!(
        first.sequence(),
        u64::try_from(HOUR_START_MS).expect("positive fixture time")
    );
    assert_eq!(first.bids()[0].price().value(), Decimal::new(1_912_345, 5));
    assert_eq!(first.bids()[0].quantity().value(), Decimal::new(12_500, 4));
    assert_eq!(first.asks()[0].price().value(), Decimal::new(1_922_345, 5));
    assert_eq!(first.asks()[0].quantity().value(), Decimal::new(25_000, 4));
    assert_eq!(batch.present_spans(), &[span]);
    assert!(batch.missing_spans().is_empty());
    assert!(batch.conflicting_spans().is_empty());
    assert_eq!(
        batch.content_digest().to_string(),
        "b3:39687da54f7754e220073a2f84c8c269bec41a39b103814201b958bd097502ae"
    );
}

#[test]
fn reader_rejects_a_truncated_lz4_source_after_metadata_verification() {
    let root = TempDir::new().expect("create archive root");
    let (destination, _) = install_fixture(&root);
    let mut bytes = fs::read(&destination).expect("read copied immutable fixture");
    bytes.truncate(bytes.len() / 2);
    fs::write(&destination, &bytes).expect("truncate copied immutable fixture");
    let source = ArchiveSource::new(
        l2_span(),
        PathBuf::from("market_data/20230916/9/l2Book/SOL.lz4"),
        u64::try_from(bytes.len()).expect("fixture length fits u64"),
        ArchiveDigest::of_bytes(&bytes),
    );
    let manifest = ArchiveManifest::new(
        AS_OF_MS,
        [ArchiveRequirement::required(l2_span())],
        [source],
    )
    .expect("truncated source metadata remains explicit");

    let error = ArchiveReader::open(root.path(), manifest)
        .expect("verified truncated object opens before streaming")
        .read_all()
        .expect_err("truncated LZ4 must be rejected while decoding");

    assert!(matches!(
        error,
        trench_hyperliquid::ArchiveError::Decompression { .. }
    ));
}

#[test]
fn reader_rejects_an_unknown_record_version() {
    let root = TempDir::new().expect("create archive root");
    let (_, source) = install_lz4(
        &root,
        br#"{"version":2,"coin":"SOL","time":1694854800000,"levels":[[{"px":"19.12345","sz":"1.2500","n":2}],[{"px":"19.22345","sz":"2.5000","n":3}]]}
"#,
    );
    let manifest = ArchiveManifest::new(
        AS_OF_MS,
        [ArchiveRequirement::required(l2_span())],
        [source],
    )
    .expect("fixture manifest must be valid");

    let error = ArchiveReader::open(root.path(), manifest)
        .expect("verified source must open")
        .read_all()
        .expect_err("unknown record version must be rejected");

    assert!(matches!(
        error,
        trench_hyperliquid::ArchiveError::UnknownRecordVersion { line: 1, .. }
    ));
}

#[test]
fn reader_rejects_a_manifest_path_with_traversal() {
    let root = TempDir::new().expect("create archive root");
    let source = ArchiveSource::new(
        l2_span(),
        PathBuf::from("../market_data/20230916/9/l2Book/SOL.lz4"),
        0,
        ArchiveDigest::of_bytes(b""),
    );
    let manifest = ArchiveManifest::new(
        AS_OF_MS,
        [ArchiveRequirement::required(l2_span())],
        [source],
    )
    .expect("manifest validation defers local path verification");

    let error = ArchiveReader::open(root.path(), manifest)
        .expect_err("path traversal must be rejected before file access");

    assert!(matches!(
        error,
        trench_hyperliquid::ArchiveError::UnsafePath { .. }
    ));
}

#[test]
fn reader_rejects_a_path_that_does_not_match_the_manifest_span() {
    let root = TempDir::new().expect("create archive root");
    let source = ArchiveSource::new(
        l2_span(),
        PathBuf::from("market_data/20230916/9/l2Book/BTC.lz4"),
        0,
        ArchiveDigest::of_bytes(b""),
    );
    let manifest = ArchiveManifest::new(
        AS_OF_MS,
        [ArchiveRequirement::required(l2_span())],
        [source],
    )
    .expect("manifest validation defers path-layout verification");

    let error = ArchiveReader::open(root.path(), manifest)
        .expect_err("source path must match the declared market/hour/channel span");

    assert!(matches!(
        error,
        trench_hyperliquid::ArchiveError::PathManifestMismatch { .. }
    ));
}

#[test]
fn reader_rejects_a_compressed_digest_mismatch_before_decompression() {
    let root = TempDir::new().expect("create archive root");
    let (destination, _) = install_fixture(&root);
    let bytes = fs::read(&destination).expect("read copied fixture");
    let source = ArchiveSource::new(
        l2_span(),
        PathBuf::from("market_data/20230916/9/l2Book/SOL.lz4"),
        u64::try_from(bytes.len()).expect("fixture length fits u64"),
        ArchiveDigest::of_bytes(b"different compressed object"),
    );
    let manifest = ArchiveManifest::new(
        AS_OF_MS,
        [ArchiveRequirement::required(l2_span())],
        [source],
    )
    .expect("fixture manifest must be valid");

    let error = ArchiveReader::open(root.path(), manifest)
        .expect_err("digest mismatch must be rejected before decompression");

    assert!(matches!(
        error,
        trench_hyperliquid::ArchiveError::CompressedDigestMismatch { .. }
    ));
}

#[cfg(unix)]
#[test]
fn reader_rejects_a_symlinked_source_even_if_it_targets_the_fixture() {
    use std::os::unix::fs::symlink;

    let root = TempDir::new().expect("create archive root");
    let relative_path = PathBuf::from("market_data/20230916/9/l2Book/SOL.lz4");
    let destination = root.path().join(&relative_path);
    fs::create_dir_all(
        destination
            .parent()
            .expect("fixture archive path has a parent"),
    )
    .expect("create fixture archive directories");
    symlink(fixture_path(), &destination).expect("create fixture symlink");
    let bytes = fs::read(fixture_path()).expect("read immutable fixture");
    let source = ArchiveSource::new(
        l2_span(),
        relative_path,
        u64::try_from(bytes.len()).expect("fixture length fits u64"),
        ArchiveDigest::of_bytes(&bytes),
    );
    let manifest = ArchiveManifest::new(
        AS_OF_MS,
        [ArchiveRequirement::required(l2_span())],
        [source],
    )
    .expect("fixture manifest must be valid");

    let error =
        ArchiveReader::open(root.path(), manifest).expect_err("symlinked source must be rejected");

    assert!(matches!(
        error,
        trench_hyperliquid::ArchiveError::Symlink { .. }
    ));
}

#[test]
fn reader_rejects_conflicting_duplicate_event_identities() {
    let root = TempDir::new().expect("create archive root");
    let (_, source) = install_lz4(
        &root,
        br#"{"coin":"SOL","time":1694854800000,"levels":[[{"px":"19.12345","sz":"1.2500","n":2}],[{"px":"19.22345","sz":"2.5000","n":3}]]}
{"coin":"SOL","time":1694854800000,"levels":[[{"px":"19.12344","sz":"1.2500","n":2}],[{"px":"19.22345","sz":"2.5000","n":3}]]}
"#,
    );
    let manifest = ArchiveManifest::new(
        AS_OF_MS,
        [ArchiveRequirement::required(l2_span())],
        [source],
    )
    .expect("fixture manifest must be valid");

    let error = ArchiveReader::open(root.path(), manifest)
        .expect("verified source must open")
        .read_all()
        .expect_err("same event identity with different content must fail");

    assert!(matches!(
        error,
        trench_hyperliquid::ArchiveError::ConflictingRecord { .. }
    ));
}

#[test]
fn reader_rejects_records_newer_than_the_explicit_as_of_time() {
    let root = TempDir::new().expect("create archive root");
    let (_, source) = install_lz4(
        &root,
        br#"{"coin":"SOL","time":1694854800001,"levels":[[{"px":"19.12345","sz":"1.2500","n":2}],[{"px":"19.22345","sz":"2.5000","n":3}]]}
"#,
    );
    let manifest = ArchiveManifest::new(
        HOUR_START_MS,
        [ArchiveRequirement::required(l2_span())],
        [source],
    )
    .expect("fixture manifest must be valid");

    let error = ArchiveReader::open(root.path(), manifest)
        .expect("verified source must open")
        .read_all()
        .expect_err("future record must be rejected against frozen as-of time");

    assert!(matches!(
        error,
        trench_hyperliquid::ArchiveError::FutureRecord {
            time_ms,
            as_of_ms,
            ..
        } if time_ms == HOUR_START_MS + 1 && as_of_ms == HOUR_START_MS
    ));
}

#[test]
fn reader_rejects_a_missing_required_l2_source() {
    let root = TempDir::new().expect("create archive root");
    let manifest = ArchiveManifest::new(AS_OF_MS, [ArchiveRequirement::required(l2_span())], [])
        .expect("fixture manifest must be valid");

    let error = ArchiveReader::open(root.path(), manifest)
        .expect_err("required L2 source must not be silently absent");

    assert!(matches!(
        error,
        trench_hyperliquid::ArchiveError::MissingRequiredSource { ref span } if span == &l2_span()
    ));
}

#[test]
fn reader_rejects_a_missing_required_bbo_source() {
    let root = TempDir::new().expect("create archive root");
    let (_, l2_source) = install_fixture(&root);
    let bbo = bbo_span();
    let manifest = ArchiveManifest::new(
        AS_OF_MS,
        [
            ArchiveRequirement::required(l2_span()),
            ArchiveRequirement::required(bbo.clone()),
        ],
        [l2_source],
    )
    .expect("fixture manifest must be valid");

    let error = ArchiveReader::open(root.path(), manifest)
        .expect_err("required BBO source must not be substituted from L2");

    assert!(matches!(
        error,
        trench_hyperliquid::ArchiveError::MissingRequiredSource { span } if span == bbo
    ));
}

#[test]
fn manifest_rejects_an_undocumented_bbo_archive_object() {
    let bbo = bbo_span();
    let source = ArchiveSource::new(
        bbo.clone(),
        PathBuf::from("market_data/20230916/9/bbo/SOL.lz4"),
        0,
        ArchiveDigest::of_bytes(b""),
    );

    assert!(
        ArchiveManifest::new(AS_OF_MS, [ArchiveRequirement::required(bbo)], [source],).is_err(),
        "the official historical archive documents L2 objects only"
    );
}

#[test]
fn reader_reports_an_optional_absent_bbo_span_exactly() {
    let root = TempDir::new().expect("create archive root");
    let (_, l2_source) = install_fixture(&root);
    let bbo = bbo_span();
    let manifest = ArchiveManifest::new(
        AS_OF_MS,
        [
            ArchiveRequirement::required(l2_span()),
            ArchiveRequirement::optional(bbo.clone()),
        ],
        [l2_source],
    )
    .expect("fixture manifest must be valid");

    let batch = ArchiveReader::open(root.path(), manifest)
        .expect("L2 source is present")
        .read_all()
        .expect("optional BBO absence is not an error");

    assert_eq!(batch.missing_spans(), &[bbo]);
    assert!(batch.conflicting_spans().is_empty());
}

#[test]
fn reader_decodes_the_file_verified_at_open_not_a_later_path_replacement() {
    let root = TempDir::new().expect("create archive root");
    let (destination, source) = install_fixture(&root);
    let manifest = ArchiveManifest::new(
        AS_OF_MS,
        [ArchiveRequirement::required(l2_span())],
        [source],
    )
    .expect("fixture manifest must be valid");
    let reader = ArchiveReader::open(root.path(), manifest)
        .expect("original fixture must verify at open time");

    let replacement = root.path().join("replacement.lz4");
    write_lz4(
        &replacement,
        br#"{"coin":"SOL","time":1694854800000,"levels":[[{"px":"19.32345","sz":"1.2500","n":2}],[{"px":"19.42345","sz":"2.5000","n":3}]]}
"#,
    );
    fs::rename(replacement, destination).expect("replace archive pathname after open");
    let batch = reader
        .read_all()
        .expect("reader must retain the verified source rather than reopening its path");
    let MarketEventKind::BookSnapshot(snapshot) = batch.events()[0].kind() else {
        panic!("fixture must normalize into an L2 snapshot");
    };

    assert_eq!(
        snapshot.bids()[0].price().value(),
        Decimal::new(1_912_345, 5)
    );
}

#[cfg(unix)]
#[test]
fn reader_rejects_a_same_inode_mutation_after_open() {
    use std::os::unix::fs::MetadataExt;

    let root = TempDir::new().expect("create archive root");
    let (destination, source) = install_lz4(
        &root,
        br#"{"coin":"SOL","time":1694854800000,"levels":[[{"px":"19.12345","sz":"1.2500","n":2}],[{"px":"19.22345","sz":"2.5000","n":3}]]}
"#,
    );
    let original_inode = fs::metadata(&destination)
        .expect("inspect original fixture")
        .ino();
    let manifest = ArchiveManifest::new(
        AS_OF_MS,
        [ArchiveRequirement::required(l2_span())],
        [source],
    )
    .expect("fixture manifest must be valid");
    let reader = ArchiveReader::open(root.path(), manifest)
        .expect("original compressed source must verify at open time");

    write_lz4(
        &destination,
        br#"{"coin":"SOL","time":1694854800000,"levels":[[{"px":"19.12345","sz":"1.2500","n":3}],[{"px":"19.22345","sz":"2.5000","n":3}]]}
"#,
    );
    assert_eq!(
        fs::metadata(&destination)
            .expect("inspect mutated fixture")
            .ino(),
        original_inode,
        "the test must overwrite the original inode rather than replace its path"
    );

    let error = reader
        .read_all()
        .expect_err("bytes consumed by decompression must match the verified digest");

    assert!(
        matches!(
            error,
            trench_hyperliquid::ArchiveError::CompressedDigestMismatch { .. }
        ),
        "unexpected mutation error: {error:?}"
    );
}

#[test]
fn content_digest_changes_when_a_normalized_book_payload_changes() {
    let original_root = TempDir::new().expect("create original archive root");
    let (_, original_source) = install_fixture(&original_root);
    let original_manifest = ArchiveManifest::new(
        AS_OF_MS,
        [ArchiveRequirement::required(l2_span())],
        [original_source],
    )
    .expect("original fixture manifest must be valid");
    let original = ArchiveReader::open(original_root.path(), original_manifest)
        .expect("original fixture opens")
        .read_all()
        .expect("original fixture decodes");

    let changed_root = TempDir::new().expect("create changed archive root");
    let (_, changed_source) = install_lz4(
        &changed_root,
        br#"{"coin":"SOL","time":1694854800000,"levels":[[{"px":"19.12346","sz":"1.2500","n":2}],[{"px":"19.22345","sz":"2.5000","n":3}]]}
{"coin":"SOL","time":1694854800001,"levels":[[{"px":"19.12000","sz":"1.5000","n":1}],[{"px":"19.22000","sz":"3.0000","n":4}]]}
"#,
    );
    let changed_manifest = ArchiveManifest::new(
        AS_OF_MS,
        [ArchiveRequirement::required(l2_span())],
        [changed_source],
    )
    .expect("changed fixture manifest must be valid");
    let changed = ArchiveReader::open(changed_root.path(), changed_manifest)
        .expect("changed fixture opens")
        .read_all()
        .expect("changed fixture decodes");

    assert_eq!(
        original.events()[0].event_id(),
        changed.events()[0].event_id()
    );
    assert_ne!(original.content_digest(), changed.content_digest());
}
