"""Versioned, bounded MessagePack contract for local ML inference."""

from __future__ import annotations

import math
from enum import StrEnum
from typing import Final, Literal, Never

import msgspec

SCHEMA_VERSION = 1
MAX_FRAME_BYTES = 1_048_576
MAX_IDENTIFIER_LENGTH = 128
MAX_ROWS = 256
MAX_FEATURES = 512
MAX_ARTIFACTS = 16
PROBABILITY_TOLERANCE = 1e-9
MESSAGEPACK_UINT64_MAX = (1 << 64) - 1

VERSION_POLICY = "reject_unknown"
ENCODING = "MessagePack"
TIMESTAMP_UNIT = "nanoseconds_since_unix_epoch_utc"
PROBABILITY_ORDER = ("short", "flat", "long")

SUPPORTED_SLEEVES = ("15m", "1h")

type Sleeve = Literal["15m", "1h"]
type PayloadType = Literal["inference_request", "inference_response"]


class ValidationCode(StrEnum):
    """Stable, non-sensitive validation categories."""

    UNKNOWN_SCHEMA_VERSION = "unknown_schema_version"
    MALFORMED_FRAME = "malformed_frame"
    FRAME_TOO_LARGE = "frame_too_large"
    INVALID_IDENTIFIER = "invalid_identifier"
    INVALID_TIMESTAMP = "invalid_timestamp"
    STALE_REQUEST = "stale_request"
    PAYLOAD_TYPE_MISMATCH = "payload_type_mismatch"
    INVALID_FEATURE = "invalid_feature"
    INVALID_FEATURE_COUNT = "invalid_feature_count"
    INVALID_OUTPUT = "invalid_output"
    INVALID_PROBABILITY = "invalid_probability"
    INVALID_ROW_COUNT = "invalid_row_count"
    DUPLICATE_ROW = "duplicate_row"
    UNSUPPORTED_SLEEVE = "unsupported_sleeve"
    INVALID_ARTIFACTS = "invalid_artifacts"
    DIGEST_MISMATCH = "digest_mismatch"
    CORRELATION_MISMATCH = "correlation_mismatch"
    RESPONSE_ROW_MISMATCH = "response_row_mismatch"


class ValidationError(ValueError):
    """A typed validation failure that never includes wire payload values."""

    def __init__(self, code: ValidationCode, message: str) -> None:
        self.code = code
        super().__init__(message)


class FeatureRow(msgspec.Struct, forbid_unknown_fields=True, frozen=True):
    market: str
    sleeve: Sleeve
    values: tuple[float, ...]


class InferenceRequest(msgspec.Struct, forbid_unknown_fields=True, frozen=True):
    feature_schema_hash: str
    artifact_ids: tuple[str, ...]
    rows: tuple[FeatureRow, ...]


class ForecastRow(msgspec.Struct, forbid_unknown_fields=True, frozen=True):
    correlation_id: str
    market: str
    sleeve: Sleeve
    artifact_id: str
    probabilities: tuple[float, float, float]
    regression_point_estimate: float
    directional_conformal_lower_bound: float


class InferenceResponse(msgspec.Struct, forbid_unknown_fields=True, frozen=True):
    correlation_id: str
    config_hash: str
    feature_schema_hash: str
    rows: tuple[ForecastRow, ...]


type Payload = InferenceRequest | InferenceResponse


class Envelope(msgspec.Struct, forbid_unknown_fields=True, frozen=True):
    schema_version: int
    event_id: str
    event_time_ns: int
    as_of_time_ns: int
    producer_version: str
    run_id: str
    config_hash: str
    payload_type: PayloadType
    payload: Payload


class _RawEnvelope(msgspec.Struct, forbid_unknown_fields=True, frozen=True):
    schema_version: int
    event_id: str
    event_time_ns: int
    as_of_time_ns: int
    producer_version: str
    run_id: str
    config_hash: str
    payload_type: str
    payload: msgspec.Raw


ENVELOPE_FIELDS = Envelope.__struct_fields__
FEATURE_ROW_FIELDS = FeatureRow.__struct_fields__
FORECAST_ROW_FIELDS = ForecastRow.__struct_fields__
INFERENCE_REQUEST_FIELDS = InferenceRequest.__struct_fields__
INFERENCE_RESPONSE_FIELDS = InferenceResponse.__struct_fields__

ENVELOPE_TYPES = (
    "integer",
    "string",
    "integer",
    "integer",
    "string",
    "string",
    "string",
    "closed_enum",
    "closed_union",
)
INFERENCE_REQUEST_TYPES = ("string", "array<string>", "array<FeatureRow>")
FEATURE_ROW_TYPES = ("string", "closed_enum", "array<float64>")
INFERENCE_RESPONSE_TYPES = (
    "string",
    "string",
    "string",
    "array<ForecastRow>",
)
FORECAST_ROW_TYPES = (
    "string",
    "string",
    "closed_enum",
    "string",
    "tuple<float64,float64,float64>",
    "float64",
    "float64",
)
VALIDATION_RULES = (
    "All structures reject unknown fields.",
    "Payload type must match its closed payload variant.",
    "Frames contain exactly one MessagePack object and no trailing bytes.",
    "All identifiers are nonempty and bounded by UTF-8 byte length.",
    "Timestamps are nonnegative integers and as_of_time_ns does not exceed event_time_ns.",
    "Staleness uses only an explicit caller-supplied decision time and request deadline.",
    "Request rows have unique (market,sleeve) keys and equal nonempty feature lengths.",
    "Features, probabilities, and scalar outputs are finite float64 values; integers and "
    "booleans are invalid.",
    "Artifact identifiers are unique and request and response digests correspond exactly.",
    "Response rows are the exact request row and artifact Cartesian product.",
    "Probability vectors are finite, within [0,1], ordered [short,flat,long], and sum to one "
    "within the declared tolerance.",
)

NORMATIVE_CONTRACT: Final[dict[str, object]] = {
    "schema_version": SCHEMA_VERSION,
    "version_policy": VERSION_POLICY,
    "encoding": ENCODING,
    "timestamp_unit": TIMESTAMP_UNIT,
    "probability_order": list(PROBABILITY_ORDER),
    "probability_sum_tolerance": PROBABILITY_TOLERANCE,
    "supported_sleeves": list(SUPPORTED_SLEEVES),
    "maxima": {
        "frame_bytes": MAX_FRAME_BYTES,
        "identifier_bytes": MAX_IDENTIFIER_LENGTH,
        "rows": MAX_ROWS,
        "features_per_row": MAX_FEATURES,
        "artifacts": MAX_ARTIFACTS,
    },
    "envelope_fields": list(ENVELOPE_FIELDS),
    "envelope_types": list(ENVELOPE_TYPES),
    "payload_variants": {
        "inference_request": {
            "fields": list(INFERENCE_REQUEST_FIELDS),
            "types": list(INFERENCE_REQUEST_TYPES),
            "row_fields": list(FEATURE_ROW_FIELDS),
            "row_types": list(FEATURE_ROW_TYPES),
        },
        "inference_response": {
            "fields": list(INFERENCE_RESPONSE_FIELDS),
            "types": list(INFERENCE_RESPONSE_TYPES),
            "row_fields": list(FORECAST_ROW_FIELDS),
            "row_types": list(FORECAST_ROW_TYPES),
        },
    },
    "validation_rules": list(VALIDATION_RULES),
}

_RAW_ENVELOPE_DECODER = msgspec.msgpack.Decoder(type=_RawEnvelope, strict=True)
_REQUEST_DECODER = msgspec.msgpack.Decoder(type=InferenceRequest, strict=True)
_RESPONSE_DECODER = msgspec.msgpack.Decoder(type=InferenceResponse, strict=True)
_ENCODER = msgspec.msgpack.Encoder()


def _fail(code: ValidationCode, message: str) -> Never:
    raise ValidationError(code, message)


def _validate_tuple(value: object) -> None:
    if type(value) is not tuple:
        _fail(ValidationCode.MALFORMED_FRAME, "wire containers must be exact tuples")


def _validate_identifier(value: object) -> None:
    if type(value) is not str or not value.strip():
        _fail(ValidationCode.INVALID_IDENTIFIER, "identifier must be a nonempty string")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        _fail(ValidationCode.INVALID_IDENTIFIER, "identifier must be valid UTF-8")
    if len(encoded) > MAX_IDENTIFIER_LENGTH:
        _fail(ValidationCode.INVALID_IDENTIFIER, "identifier exceeds the byte limit")


def _validate_timestamp(value: object) -> None:
    if type(value) is not int or not 0 <= value <= MESSAGEPACK_UINT64_MAX:
        _fail(
            ValidationCode.INVALID_TIMESTAMP,
            "timestamp must be a nonnegative MessagePack integer",
        )


def _validate_sleeve(value: object) -> None:
    if type(value) is not str or value not in SUPPORTED_SLEEVES:
        _fail(ValidationCode.UNSUPPORTED_SLEEVE, "sleeve is not supported")


def _is_finite_float(value: object) -> bool:
    return type(value) is float and math.isfinite(value)


def _validate_request(payload: InferenceRequest) -> None:
    _validate_identifier(payload.feature_schema_hash)
    _validate_tuple(payload.artifact_ids)
    if not 1 <= len(payload.artifact_ids) <= MAX_ARTIFACTS:
        _fail(ValidationCode.INVALID_ARTIFACTS, "artifact count is outside the allowed range")
    for artifact_id in payload.artifact_ids:
        _validate_identifier(artifact_id)
    if len(set(payload.artifact_ids)) != len(payload.artifact_ids):
        _fail(ValidationCode.INVALID_ARTIFACTS, "artifact identifiers must be unique")

    _validate_tuple(payload.rows)
    if not 1 <= len(payload.rows) <= MAX_ROWS:
        _fail(ValidationCode.INVALID_ROW_COUNT, "request row count is outside the allowed range")

    row_keys: set[tuple[str, str]] = set()
    feature_count: int | None = None
    for row in payload.rows:
        if type(row) is not FeatureRow:
            _fail(ValidationCode.MALFORMED_FRAME, "request rows must be exact feature rows")
        _validate_identifier(row.market)
        _validate_sleeve(row.sleeve)
        _validate_tuple(row.values)
        if not 1 <= len(row.values) <= MAX_FEATURES:
            _fail(
                ValidationCode.INVALID_FEATURE_COUNT,
                "feature count is outside the allowed range",
            )
        if feature_count is None:
            feature_count = len(row.values)
        elif len(row.values) != feature_count:
            _fail(
                ValidationCode.INVALID_FEATURE_COUNT,
                "request rows have different feature counts",
            )
        if any(not _is_finite_float(value) for value in row.values):
            _fail(ValidationCode.INVALID_FEATURE, "feature values must be finite floats")
        key = (row.market, row.sleeve)
        if key in row_keys:
            _fail(ValidationCode.DUPLICATE_ROW, "request row keys must be unique")
        row_keys.add(key)


def _validate_response(payload: InferenceResponse, envelope_config_hash: str) -> None:
    _validate_identifier(payload.correlation_id)
    _validate_identifier(payload.config_hash)
    _validate_identifier(payload.feature_schema_hash)
    if payload.config_hash != envelope_config_hash:
        _fail(ValidationCode.DIGEST_MISMATCH, "response config digest does not match envelope")
    _validate_tuple(payload.rows)
    if not 1 <= len(payload.rows) <= MAX_ROWS * MAX_ARTIFACTS:
        _fail(ValidationCode.INVALID_ROW_COUNT, "response row count is outside the allowed range")

    row_keys: set[tuple[str, str, str]] = set()
    for row in payload.rows:
        if type(row) is not ForecastRow:
            _fail(ValidationCode.MALFORMED_FRAME, "response rows must be exact forecast rows")
        _validate_identifier(row.correlation_id)
        _validate_identifier(row.market)
        _validate_identifier(row.artifact_id)
        _validate_sleeve(row.sleeve)
        if row.correlation_id != payload.correlation_id:
            _fail(ValidationCode.CORRELATION_MISMATCH, "response row correlation does not match")
        if not _is_finite_float(row.regression_point_estimate) or not _is_finite_float(
            row.directional_conformal_lower_bound
        ):
            _fail(ValidationCode.INVALID_OUTPUT, "forecast outputs must be finite floats")
        _validate_tuple(row.probabilities)
        if len(row.probabilities) != 3:
            _fail(
                ValidationCode.INVALID_PROBABILITY,
                "probability vector must contain exactly three values",
            )
        if any(
            not _is_finite_float(value) or not 0.0 <= value <= 1.0 for value in row.probabilities
        ):
            _fail(
                ValidationCode.INVALID_PROBABILITY,
                "probabilities must be finite floats in the closed unit interval",
            )
        if abs(math.fsum(row.probabilities) - 1.0) > PROBABILITY_TOLERANCE:
            _fail(ValidationCode.INVALID_PROBABILITY, "probability sum is outside tolerance")
        key = (row.market, row.sleeve, row.artifact_id)
        if key in row_keys:
            _fail(ValidationCode.DUPLICATE_ROW, "response row keys must be unique")
        row_keys.add(key)


def validate_envelope(
    envelope: Envelope,
    *,
    decision_time_ns: int | None = None,
    request_deadline_ns: int | None = None,
) -> None:
    """Validate an envelope without consulting wall-clock time."""

    if type(envelope) is not Envelope:
        _fail(ValidationCode.MALFORMED_FRAME, "envelope must have the exact wire struct type")
    if type(envelope.schema_version) is not int or envelope.schema_version != SCHEMA_VERSION:
        _fail(ValidationCode.UNKNOWN_SCHEMA_VERSION, "schema version is not supported")
    for identifier in (
        envelope.event_id,
        envelope.producer_version,
        envelope.run_id,
        envelope.config_hash,
    ):
        _validate_identifier(identifier)
    _validate_timestamp(envelope.event_time_ns)
    _validate_timestamp(envelope.as_of_time_ns)
    if envelope.as_of_time_ns > envelope.event_time_ns:
        _fail(ValidationCode.INVALID_TIMESTAMP, "as-of time cannot exceed event time")

    if (decision_time_ns is None) != (request_deadline_ns is None):
        _fail(
            ValidationCode.INVALID_TIMESTAMP,
            "decision time and deadline must be supplied together",
        )
    if decision_time_ns is not None and request_deadline_ns is not None:
        _validate_timestamp(decision_time_ns)
        _validate_timestamp(request_deadline_ns)
        if decision_time_ns < envelope.as_of_time_ns:
            _fail(ValidationCode.INVALID_TIMESTAMP, "decision time cannot precede as-of time")
        if decision_time_ns - envelope.as_of_time_ns > request_deadline_ns:
            _fail(ValidationCode.STALE_REQUEST, "envelope is stale at the supplied decision time")

    if type(envelope.payload_type) is not str:
        _fail(ValidationCode.PAYLOAD_TYPE_MISMATCH, "payload type must be an exact string")
    if envelope.payload_type == "inference_request" and type(envelope.payload) is InferenceRequest:
        _validate_request(envelope.payload)
    elif (
        envelope.payload_type == "inference_response"
        and type(envelope.payload) is InferenceResponse
    ):
        _validate_response(envelope.payload, envelope.config_hash)
    else:
        _fail(ValidationCode.PAYLOAD_TYPE_MISMATCH, "payload type does not match payload")


def encode_envelope(envelope: Envelope) -> bytes:
    """Validate and encode one canonical MessagePack envelope."""

    try:
        validate_envelope(envelope)
        encoded = _ENCODER.encode(envelope)
    except ValidationError:
        raise
    except (msgspec.EncodeError, OverflowError, TypeError, ValueError) as error:
        raise ValidationError(
            ValidationCode.MALFORMED_FRAME,
            "envelope cannot be encoded",
        ) from error
    if len(encoded) > MAX_FRAME_BYTES:
        _fail(ValidationCode.FRAME_TOO_LARGE, "encoded frame exceeds the byte limit")
    return encoded


def decode_envelope(
    frame: bytes,
    *,
    decision_time_ns: int | None = None,
    request_deadline_ns: int | None = None,
) -> Envelope:
    """Decode exactly one bounded frame and revalidate all semantic constraints."""

    if len(frame) > MAX_FRAME_BYTES:
        _fail(ValidationCode.FRAME_TOO_LARGE, "frame exceeds the byte limit")
    try:
        raw = _RAW_ENVELOPE_DECODER.decode(frame)
        if raw.payload_type == "inference_request":
            payload: Payload = _REQUEST_DECODER.decode(raw.payload)
        elif raw.payload_type == "inference_response":
            payload = _RESPONSE_DECODER.decode(raw.payload)
        else:
            _fail(ValidationCode.PAYLOAD_TYPE_MISMATCH, "payload type is not supported")
        envelope = Envelope(
            schema_version=raw.schema_version,
            event_id=raw.event_id,
            event_time_ns=raw.event_time_ns,
            as_of_time_ns=raw.as_of_time_ns,
            producer_version=raw.producer_version,
            run_id=raw.run_id,
            config_hash=raw.config_hash,
            payload_type=raw.payload_type,
            payload=payload,
        )
    except ValidationError:
        raise
    except (msgspec.DecodeError, TypeError, ValueError) as error:
        raise ValidationError(ValidationCode.MALFORMED_FRAME, "frame is malformed") from error

    validate_envelope(
        envelope,
        decision_time_ns=decision_time_ns,
        request_deadline_ns=request_deadline_ns,
    )
    if encode_envelope(envelope) != frame:
        _fail(ValidationCode.MALFORMED_FRAME, "frame is not canonically encoded")
    return envelope


def validate_response_for_request(request: Envelope, response: Envelope) -> None:
    """Require a response to correspond exactly to a validated request."""

    validate_envelope(request)
    validate_envelope(response)
    if type(request.payload) is not InferenceRequest or request.payload_type != "inference_request":
        _fail(ValidationCode.PAYLOAD_TYPE_MISMATCH, "expected an inference request")
    if (
        type(response.payload) is not InferenceResponse
        or response.payload_type != "inference_response"
    ):
        _fail(ValidationCode.PAYLOAD_TYPE_MISMATCH, "expected an inference response")

    request_payload = request.payload
    response_payload = response.payload
    if response_payload.correlation_id != request.event_id:
        _fail(ValidationCode.CORRELATION_MISMATCH, "response correlation does not match request")
    if response.run_id != request.run_id:
        _fail(ValidationCode.CORRELATION_MISMATCH, "response run does not match request")
    if response.as_of_time_ns != request.as_of_time_ns:
        _fail(ValidationCode.CORRELATION_MISMATCH, "response as-of time does not match request")
    if (
        response.config_hash != request.config_hash
        or response_payload.config_hash != request.config_hash
        or response_payload.feature_schema_hash != request_payload.feature_schema_hash
    ):
        _fail(ValidationCode.DIGEST_MISMATCH, "response digests do not match request")

    expected = {
        (row.market, row.sleeve, artifact_id)
        for row in request_payload.rows
        for artifact_id in request_payload.artifact_ids
    }
    actual = {(row.market, row.sleeve, row.artifact_id) for row in response_payload.rows}
    if actual != expected:
        request_artifacts = set(request_payload.artifact_ids)
        response_artifacts = {row.artifact_id for row in response_payload.rows}
        if response_artifacts != request_artifacts:
            _fail(ValidationCode.DIGEST_MISMATCH, "response artifacts do not match request")
        _fail(ValidationCode.RESPONSE_ROW_MISMATCH, "response rows do not match request rows")
