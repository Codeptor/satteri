//! Read-only public Hyperliquid market-data adapters.

mod archive;
mod capture;
mod info;
mod normalize;
mod recovery;
mod recovery_producer;
mod ws;

pub use archive::{
    ArchiveBatch, ArchiveDataKind, ArchiveDigest, ArchiveError, ArchiveManifest, ArchiveReader,
    ArchiveRequirement, ArchiveSource, ArchiveSpan,
};
pub use capture::{
    CaptureOperation, ContextCapture, ContextCaptureBatch, ContextCaptureError,
    ContextCaptureRequest, MAX_CONTEXT_EVENTS, MAX_CONTEXT_FUNDING_RECORDS, MAX_CONTEXT_MARKETS,
    MAX_CONTEXT_REQUEST_CONCURRENCY, MAX_DETAILED_CONTEXT_MARKETS, ReceiptClock,
};
pub use info::{
    CandleInterval, INFO_RESPONSE_MAX_BYTES, InfoClient, InfoError, L2BookPrecision, L2Mantissa,
    TimeRange,
};
pub use normalize::{
    AssetContext, BookLevel, Candle, FundingRecord, L2Book, MetaAndAssetContexts, PerpAsset,
    SignedRate, VenueMaxLeverage,
};
#[cfg(debug_assertions)]
#[doc(hidden)]
pub use recovery::recovery_request_from_events_for_test;
pub use recovery::{
    GapRecovery, GapRecoveryRequest, MAX_OUTSTANDING_RECOVERY_REQUESTS,
    MAX_PROCESSED_RECOVERY_REQUESTS, MAX_RECOVERY_LOCAL_TRADES, MAX_RECOVERY_OFFICIAL_CANDLES,
    RecoveryError, RecoveryEvidence, RecoveryResult, RecoverySource, RecoveryStatus,
    RecoveryUnavailable,
};
pub use recovery_producer::{
    MAX_RETAINED_RECOVERY_TRADES_PER_MARKET, RecoveryEvidenceProducer, RecoveryProducerError,
};
pub use ws::{
    GapEvent, GapExhausted, GapOpened, GapReason, RejectedUpdate, RejectionReason,
    TradeIdentityLimit, WsClient, WsConfig, WsError, WsLimits, WsOutput, WsStream, WsTerminal,
};
