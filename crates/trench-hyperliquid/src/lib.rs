//! Read-only public Hyperliquid market-data adapters.

mod info;
mod normalize;
mod ws;

pub use info::{
    CandleInterval, INFO_RESPONSE_MAX_BYTES, InfoClient, InfoError, L2BookPrecision, L2Mantissa,
    TimeRange,
};
pub use normalize::{
    AssetContext, BookLevel, Candle, FundingRecord, L2Book, MetaAndAssetContexts, PerpAsset,
    SignedRate, VenueMaxLeverage,
};
pub use ws::{WsClient, WsConfig, WsError, WsLimits};
