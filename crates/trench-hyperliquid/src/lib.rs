//! Read-only public Hyperliquid market-data adapters.

mod archive;
mod info;
mod normalize;
mod recovery;
mod ws;

pub use archive::{
    ArchiveBatch, ArchiveDataKind, ArchiveDigest, ArchiveError, ArchiveManifest, ArchiveReader,
    ArchiveRequirement, ArchiveSource, ArchiveSpan,
};
pub use info::{
    CandleInterval, INFO_RESPONSE_MAX_BYTES, InfoClient, InfoError, L2BookPrecision, L2Mantissa,
    TimeRange,
};
pub use normalize::{
    AssetContext, BookLevel, Candle, FundingRecord, L2Book, MetaAndAssetContexts, PerpAsset,
    SignedRate, VenueMaxLeverage,
};
pub use recovery::{
    GapRecovery, GapRecoveryRequest, MAX_OUTSTANDING_RECOVERY_REQUESTS,
    MAX_PROCESSED_RECOVERY_REQUESTS, MAX_RECOVERY_LOCAL_TRADES, MAX_RECOVERY_OFFICIAL_CANDLES,
    RecoveryError, RecoveryEvidence, RecoveryResult, RecoverySource, RecoveryStatus,
    RecoveryUnavailable,
};
pub use ws::{
    GapEvent, GapExhausted, GapOpened, GapReason, RejectedUpdate, RejectionReason,
    TradeIdentityLimit, WsClient, WsConfig, WsError, WsLimits, WsOutput, WsStream, WsTerminal,
};
