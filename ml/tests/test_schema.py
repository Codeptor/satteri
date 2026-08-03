from __future__ import annotations

import json
import math
from pathlib import Path
from typing import cast

import msgspec
import pytest

import trench_ml.config as config_module
import trench_ml.schema as schema_module
from trench_ml.config import ConfigError, Settings, load_settings
from trench_ml.schema import (
    ENVELOPE_FIELDS,
    FEATURE_ROW_FIELDS,
    FORECAST_ROW_FIELDS,
    INFERENCE_REQUEST_FIELDS,
    INFERENCE_RESPONSE_FIELDS,
    MAX_ARTIFACTS,
    MAX_FEATURES,
    MAX_FRAME_BYTES,
    MAX_IDENTIFIER_LENGTH,
    MAX_ROWS,
    PROBABILITY_TOLERANCE,
    SCHEMA_VERSION,
    SUPPORTED_SLEEVES,
    Envelope,
    FeatureRow,
    ForecastRow,
    InferenceRequest,
    InferenceResponse,
    ValidationCode,
    ValidationError,
    decode_envelope,
    encode_envelope,
    validate_envelope,
    validate_response_for_request,
)

EVENT_TIME_NS = 1_786_000_000_000_000_000
DEADLINE_NS = 250_000_000
EXAMPLE_CONFIG_PATH = Path(__file__).parents[2] / "config" / "ml.example.toml"
EXPECTED_LIGHTGBM_MIN_NUM_LEAVES = 2
EXPECTED_LIGHTGBM_MAX_NUM_LEAVES = 131_072
EXPECTED_LIGHTGBM_MIN_CHILD_SAMPLES_MIN = 0
EXPECTED_LIGHTGBM_MIN_CHILD_SAMPLES_MAX = (1 << 31) - 1
EXPECTED_MAX_LIGHTGBM_GRID_VALUES_PER_DIMENSION = 8
EXPECTED_MAX_LIGHTGBM_GRID_COMBINATIONS = 64


class StringSubclass(str):
    pass


class IntegerSubclass(int):
    pass


class FloatSubclass(float):
    pass


def valid_request() -> Envelope:
    return Envelope(
        schema_version=1,
        event_id="evt-1",
        event_time_ns=EVENT_TIME_NS,
        as_of_time_ns=EVENT_TIME_NS,
        producer_version="test",
        run_id="run-1",
        config_hash="b3:test",
        payload_type="inference_request",
        payload=InferenceRequest(
            feature_schema_hash="b3:features-v1",
            artifact_ids=("b3:champion",),
            rows=(FeatureRow(market="SOL", sleeve="15m", values=(0.1, 0.2)),),
        ),
    )


def valid_response() -> Envelope:
    return Envelope(
        schema_version=1,
        event_id="rsp-1",
        event_time_ns=EVENT_TIME_NS,
        as_of_time_ns=EVENT_TIME_NS,
        producer_version="test",
        run_id="run-1",
        config_hash="b3:test",
        payload_type="inference_response",
        payload=InferenceResponse(
            correlation_id="evt-1",
            config_hash="b3:test",
            feature_schema_hash="b3:features-v1",
            rows=(
                ForecastRow(
                    correlation_id="evt-1",
                    market="SOL",
                    sleeve="15m",
                    artifact_id="b3:champion",
                    probabilities=(0.2, 0.3, 0.5),
                    regression_point_estimate=0.01,
                    directional_conformal_lower_bound=0.001,
                ),
            ),
        ),
    )


def replace_envelope(envelope: Envelope, **changes: object) -> Envelope:
    return msgspec.structs.replace(envelope, **changes)


def assert_code(error: pytest.ExceptionInfo[ValidationError], code: ValidationCode) -> None:
    assert error.value.code is code
    assert "SOL" not in str(error.value)
    assert "0.1" not in str(error.value)


def validate_current(envelope: Envelope) -> None:
    validate_envelope(
        envelope,
        decision_time_ns=EVENT_TIME_NS,
        request_deadline_ns=DEADLINE_NS,
    )


def load_config_with_replacements(
    tmp_path: Path,
    replacements: dict[str, str],
) -> Settings:
    source = EXAMPLE_CONFIG_PATH.read_text(encoding="utf-8")
    for old, new in replacements.items():
        assert old in source
        source = source.replace(old, new)
    config_path = tmp_path / "ml.toml"
    config_path.write_text(source, encoding="utf-8")
    return load_settings(config_path)


def load_modified_config(tmp_path: Path, old: str, new: str) -> Settings:
    return load_config_with_replacements(tmp_path, {old: new})


def test_valid_request_round_trip_is_canonical() -> None:
    request = valid_request()

    encoded = encode_envelope(request)
    decoded = decode_envelope(
        encoded,
        decision_time_ns=EVENT_TIME_NS,
        request_deadline_ns=DEADLINE_NS,
    )

    assert decoded == request
    assert isinstance(cast("InferenceRequest", decoded.payload).rows, tuple)
    assert encode_envelope(decoded) == encoded


def test_valid_response_round_trip_and_request_correlation() -> None:
    request = valid_request()
    response = valid_response()

    decoded = decode_envelope(
        encode_envelope(response),
        decision_time_ns=EVENT_TIME_NS,
        request_deadline_ns=DEADLINE_NS,
    )
    validate_response_for_request(request, decoded)

    assert decoded == response


@pytest.mark.parametrize(
    "envelope",
    [
        valid_request(),
        valid_response(),
        replace_envelope(
            valid_request(),
            event_id="事件-一",
            event_time_ns=(1 << 64) - 1,
            as_of_time_ns=(1 << 64) - 1,
        ),
        replace_envelope(
            valid_response(),
            payload=msgspec.structs.replace(
                cast("InferenceResponse", valid_response().payload),
                rows=(
                    msgspec.structs.replace(
                        cast("InferenceResponse", valid_response().payload).rows[0],
                        sleeve="1h",
                        probabilities=(0.0, 0.0, 1.0),
                    ),
                ),
            ),
        ),
    ],
)
def test_every_accepted_envelope_has_an_object_and_byte_identical_round_trip(
    envelope: Envelope,
) -> None:
    encoded = encode_envelope(envelope)
    decoded = decode_envelope(encoded)

    assert decoded == envelope
    assert type(decoded) is Envelope
    assert encode_envelope(decoded) == encoded


def test_request_messagepack_encoding_has_a_stable_golden_value() -> None:
    assert encode_envelope(valid_request()).hex() == (
        "89ae736368656d615f76657273696f6e01a86576656e745f6964a56576742d31"
        "ad6576656e745f74696d655f6e73cf18c9258990d90000ad61735f6f665f74696d"
        "655f6e73cf18c9258990d90000b070726f64756365725f76657273696f6ea47465"
        "7374a672756e5f6964a572756e2d31ab636f6e6669675f68617368a762333a7465"
        "7374ac7061796c6f61645f74797065b1696e666572656e63655f72657175657374"
        "a77061796c6f616483b3666561747572655f736368656d615f68617368ae62333a"
        "66656174757265732d7631ac61727469666163745f69647391ab62333a6368616d"
        "70696f6ea4726f77739183a66d61726b6574a3534f4ca6736c65657665a331356d"
        "a676616c75657392cb3fb999999999999acb3fc999999999999a"
    )


def test_float_schema_version_is_rejected_before_encoding() -> None:
    request = replace_envelope(valid_request(), schema_version=1.0)

    with pytest.raises(ValidationError) as error:
        encode_envelope(request)

    assert_code(error, ValidationCode.UNKNOWN_SCHEMA_VERSION)


def test_envelope_must_have_the_exact_runtime_struct_type() -> None:
    with pytest.raises(ValidationError) as error:
        encode_envelope(cast("Envelope", object()))

    assert_code(error, ValidationCode.MALFORMED_FRAME)


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"schema_version": IntegerSubclass(1)}, ValidationCode.UNKNOWN_SCHEMA_VERSION),
        ({"event_id": StringSubclass("evt-1")}, ValidationCode.INVALID_IDENTIFIER),
        ({"event_time_ns": IntegerSubclass(EVENT_TIME_NS)}, ValidationCode.INVALID_TIMESTAMP),
        (
            {"payload_type": StringSubclass("inference_request")},
            ValidationCode.PAYLOAD_TYPE_MISMATCH,
        ),
    ],
)
def test_outbound_scalar_fields_require_exact_runtime_types(
    changes: dict[str, object], code: ValidationCode
) -> None:
    request = replace_envelope(valid_request(), **changes)

    with pytest.raises(ValidationError) as error:
        encode_envelope(request)

    assert_code(error, code)


def test_outbound_float_fields_reject_float_subclasses() -> None:
    payload = cast("InferenceRequest", valid_request().payload)
    row = msgspec.structs.replace(payload.rows[0], values=(FloatSubclass(0.1),))
    request = replace_envelope(
        valid_request(),
        payload=msgspec.structs.replace(payload, rows=(row,)),
    )

    with pytest.raises(ValidationError) as error:
        encode_envelope(request)

    assert_code(error, ValidationCode.INVALID_FEATURE)


@pytest.mark.parametrize("field", ["event_time_ns", "as_of_time_ns"])
def test_messagepack_oversized_timestamp_is_rejected_before_encoding(field: str) -> None:
    request = replace_envelope(valid_request(), **{field: 1 << 64})

    with pytest.raises(ValidationError) as error:
        encode_envelope(request)

    assert_code(error, ValidationCode.INVALID_TIMESTAMP)


@pytest.mark.parametrize(
    "envelope",
    [
        replace_envelope(
            valid_request(),
            payload=msgspec.structs.replace(
                cast("InferenceRequest", valid_request().payload),
                artifact_ids=["b3:champion"],
            ),
        ),
        replace_envelope(
            valid_request(),
            payload=msgspec.structs.replace(
                cast("InferenceRequest", valid_request().payload),
                rows=list(cast("InferenceRequest", valid_request().payload).rows),
            ),
        ),
        replace_envelope(
            valid_request(),
            payload=msgspec.structs.replace(
                cast("InferenceRequest", valid_request().payload),
                rows=(
                    msgspec.structs.replace(
                        cast("InferenceRequest", valid_request().payload).rows[0],
                        values=[0.1, 0.2],
                    ),
                ),
            ),
        ),
        replace_envelope(
            valid_response(),
            payload=msgspec.structs.replace(
                cast("InferenceResponse", valid_response().payload),
                rows=list(cast("InferenceResponse", valid_response().payload).rows),
            ),
        ),
        replace_envelope(
            valid_response(),
            payload=msgspec.structs.replace(
                cast("InferenceResponse", valid_response().payload),
                rows=(
                    msgspec.structs.replace(
                        cast("InferenceResponse", valid_response().payload).rows[0],
                        probabilities=[0.2, 0.3, 0.5],
                    ),
                ),
            ),
        ),
        replace_envelope(
            valid_request(),
            payload=msgspec.structs.replace(
                cast("InferenceRequest", valid_request().payload),
                rows=(cast("FeatureRow", object()),),
            ),
        ),
    ],
)
def test_outbound_noncanonical_structs_and_containers_fail_stably(envelope: Envelope) -> None:
    with pytest.raises(ValidationError) as error:
        encode_envelope(envelope)

    assert_code(error, ValidationCode.MALFORMED_FRAME)


def test_unhashable_artifact_identifier_fails_validation_before_uniqueness_hashing() -> None:
    payload = cast("InferenceRequest", valid_request().payload)
    request = replace_envelope(
        valid_request(),
        payload=msgspec.structs.replace(payload, artifact_ids=(cast("str", []),)),
    )

    with pytest.raises(ValidationError) as error:
        encode_envelope(request)

    assert_code(error, ValidationCode.INVALID_IDENTIFIER)


def test_non_utf8_identifier_fails_with_the_stable_identifier_code() -> None:
    request = replace_envelope(valid_request(), event_id="\ud800")

    with pytest.raises(ValidationError) as error:
        encode_envelope(request)

    assert_code(error, ValidationCode.INVALID_IDENTIFIER)


def test_residual_encoder_failure_is_mapped_to_stable_validation_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingEncoder:
        @staticmethod
        def encode(_value: object) -> bytes:
            raise TypeError("encoder internals must not escape")

    monkeypatch.setattr(schema_module, "_ENCODER", FailingEncoder())

    with pytest.raises(ValidationError) as error:
        encode_envelope(valid_request())

    assert_code(error, ValidationCode.MALFORMED_FRAME)


def test_unknown_schema_version_is_rejected() -> None:
    request = replace_envelope(valid_request(), schema_version=2)

    with pytest.raises(ValidationError) as error:
        validate_current(request)

    assert_code(error, ValidationCode.UNKNOWN_SCHEMA_VERSION)


@pytest.mark.parametrize(
    ("path", "field"),
    [
        ((), "extra_envelope_field"),
        (("payload",), "extra_payload_field"),
        (("payload", "rows", 0), "extra_row_field"),
    ],
)
def test_unknown_wire_field_is_rejected(path: tuple[str | int, ...], field: str) -> None:
    raw = msgspec.msgpack.decode(encode_envelope(valid_request()))
    cursor = raw
    for element in path:
        cursor = cursor[element]
    cursor[field] = "forbidden"

    with pytest.raises(ValidationError) as error:
        decode_envelope(msgspec.msgpack.encode(raw))

    assert_code(error, ValidationCode.MALFORMED_FRAME)


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, True])
def test_non_finite_or_boolean_feature_is_rejected(value: float | bool) -> None:
    payload = cast("InferenceRequest", valid_request().payload)
    row = msgspec.structs.replace(payload.rows[0], values=(value,))
    request = replace_envelope(
        valid_request(), payload=msgspec.structs.replace(payload, rows=(row,))
    )

    with pytest.raises(ValidationError) as error:
        validate_current(request)

    assert_code(error, ValidationCode.INVALID_FEATURE)


def test_integer_features_are_rejected_before_encoding() -> None:
    payload = cast("InferenceRequest", valid_request().payload)
    row = msgspec.structs.replace(payload.rows[0], values=(1, 2))
    request = replace_envelope(
        valid_request(), payload=msgspec.structs.replace(payload, rows=(row,))
    )

    with pytest.raises(ValidationError) as error:
        encode_envelope(request)

    assert_code(error, ValidationCode.INVALID_FEATURE)


@pytest.mark.parametrize(
    "field", ["regression_point_estimate", "directional_conformal_lower_bound"]
)
@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, True])
def test_non_finite_or_boolean_forecast_output_is_rejected(field: str, value: float | bool) -> None:
    response = valid_response()
    payload = cast("InferenceResponse", response.payload)
    row = msgspec.structs.replace(payload.rows[0], **{field: value})
    response = replace_envelope(response, payload=msgspec.structs.replace(payload, rows=(row,)))

    with pytest.raises(ValidationError) as error:
        validate_current(response)

    assert_code(error, ValidationCode.INVALID_OUTPUT)


@pytest.mark.parametrize(
    "field", ["regression_point_estimate", "directional_conformal_lower_bound"]
)
def test_integer_forecast_outputs_are_rejected_before_encoding(field: str) -> None:
    response = valid_response()
    payload = cast("InferenceResponse", response.payload)
    row = msgspec.structs.replace(payload.rows[0], **{field: 0})
    response = replace_envelope(response, payload=msgspec.structs.replace(payload, rows=(row,)))

    with pytest.raises(ValidationError) as error:
        encode_envelope(response)

    assert_code(error, ValidationCode.INVALID_OUTPUT)


def test_stale_as_of_time_is_rejected_against_explicit_decision_time() -> None:
    with pytest.raises(ValidationError) as error:
        validate_envelope(
            valid_request(),
            decision_time_ns=EVENT_TIME_NS + DEADLINE_NS + 1,
            request_deadline_ns=DEADLINE_NS,
        )

    assert_code(error, ValidationCode.STALE_REQUEST)


def test_as_of_time_after_event_time_is_rejected() -> None:
    request = replace_envelope(valid_request(), as_of_time_ns=EVENT_TIME_NS + 1)

    with pytest.raises(ValidationError) as error:
        validate_current(request)

    assert_code(error, ValidationCode.INVALID_TIMESTAMP)


@pytest.mark.parametrize("field", ["event_time_ns", "as_of_time_ns"])
@pytest.mark.parametrize("value", [-1, True])
def test_negative_or_boolean_timestamp_is_rejected(field: str, value: int | bool) -> None:
    request = replace_envelope(valid_request(), **{field: value})

    with pytest.raises(ValidationError) as error:
        validate_current(request)

    assert_code(error, ValidationCode.INVALID_TIMESTAMP)


def test_duplicate_market_sleeve_request_rows_are_rejected() -> None:
    payload = cast("InferenceRequest", valid_request().payload)
    request = replace_envelope(
        valid_request(),
        payload=msgspec.structs.replace(payload, rows=(payload.rows[0], payload.rows[0])),
    )

    with pytest.raises(ValidationError) as error:
        validate_current(request)

    assert_code(error, ValidationCode.DUPLICATE_ROW)


def test_unsupported_sleeve_is_rejected() -> None:
    payload = cast("InferenceRequest", valid_request().payload)
    row = msgspec.structs.replace(payload.rows[0], sleeve="4h")
    request = replace_envelope(
        valid_request(), payload=msgspec.structs.replace(payload, rows=(row,))
    )

    with pytest.raises(ValidationError) as error:
        validate_current(request)

    assert_code(error, ValidationCode.UNSUPPORTED_SLEEVE)


@pytest.mark.parametrize(
    "probabilities",
    [
        (math.nan, 0.0, 1.0),
        (math.inf, 0.0, 0.0),
        (-0.1, 0.5, 0.6),
        (1.1, 0.0, 0.0),
        (1e308, 1e308, 0.0),
        (True, 0.0, 0.0),
        (0.2, 0.3, 0.5 + PROBABILITY_TOLERANCE * 2),
    ],
)
def test_invalid_probability_vector_is_rejected(
    probabilities: tuple[float | bool, float, float],
) -> None:
    response = valid_response()
    payload = cast("InferenceResponse", response.payload)
    row = msgspec.structs.replace(payload.rows[0], probabilities=probabilities)
    response = replace_envelope(response, payload=msgspec.structs.replace(payload, rows=(row,)))

    with pytest.raises(ValidationError) as error:
        validate_current(response)

    assert_code(error, ValidationCode.INVALID_PROBABILITY)


def test_probability_sum_inside_declared_tolerance_is_accepted() -> None:
    response = valid_response()
    payload = cast("InferenceResponse", response.payload)
    probabilities = (0.2, 0.3, 0.5 + PROBABILITY_TOLERANCE / 2)
    row = msgspec.structs.replace(payload.rows[0], probabilities=probabilities)
    response = replace_envelope(response, payload=msgspec.structs.replace(payload, rows=(row,)))

    validate_current(response)


def test_integer_probabilities_are_rejected_before_encoding() -> None:
    response = valid_response()
    payload = cast("InferenceResponse", response.payload)
    row = msgspec.structs.replace(payload.rows[0], probabilities=(0, 0, 1))
    response = replace_envelope(response, payload=msgspec.structs.replace(payload, rows=(row,)))

    with pytest.raises(ValidationError) as error:
        encode_envelope(response)

    assert_code(error, ValidationCode.INVALID_PROBABILITY)


def test_integer_feature_wire_frame_is_rejected_instead_of_normalized() -> None:
    canonical_frame = encode_envelope(valid_request())
    raw = msgspec.msgpack.decode(canonical_frame)
    raw["payload"]["rows"][0]["values"] = [1, 2]
    integer_frame = msgspec.msgpack.encode(raw)

    with pytest.raises(ValidationError) as error:
        decode_envelope(integer_frame)

    assert integer_frame != canonical_frame
    assert_code(error, ValidationCode.MALFORMED_FRAME)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("probabilities", [0, 0, 1]),
        ("regression_point_estimate", 0),
        ("directional_conformal_lower_bound", 0),
    ],
)
def test_integer_forecast_wire_frame_is_rejected_instead_of_normalized(
    field: str, value: int | list[int]
) -> None:
    canonical_frame = encode_envelope(valid_response())
    raw = msgspec.msgpack.decode(canonical_frame)
    raw["payload"]["rows"][0][field] = value
    integer_frame = msgspec.msgpack.encode(raw)

    with pytest.raises(ValidationError) as error:
        decode_envelope(integer_frame)

    assert integer_frame != canonical_frame
    assert_code(error, ValidationCode.MALFORMED_FRAME)


@pytest.mark.parametrize("probabilities", [(0.4, 0.6), (0.1, 0.2, 0.3, 0.4)])
def test_probability_vector_must_have_exactly_three_values(
    probabilities: tuple[float, ...],
) -> None:
    response = valid_response()
    payload = cast("InferenceResponse", response.payload)
    row = msgspec.structs.replace(payload.rows[0], probabilities=probabilities)
    response = replace_envelope(response, payload=msgspec.structs.replace(payload, rows=(row,)))

    with pytest.raises(ValidationError) as error:
        validate_current(response)

    assert_code(error, ValidationCode.INVALID_PROBABILITY)


@pytest.mark.parametrize("values", [(), (0.0,) * (MAX_FEATURES + 1)])
def test_empty_or_oversized_feature_vector_is_rejected(values: tuple[float, ...]) -> None:
    payload = cast("InferenceRequest", valid_request().payload)
    row = msgspec.structs.replace(payload.rows[0], values=values)
    request = replace_envelope(
        valid_request(), payload=msgspec.structs.replace(payload, rows=(row,))
    )

    with pytest.raises(ValidationError) as error:
        validate_current(request)

    assert_code(error, ValidationCode.INVALID_FEATURE_COUNT)


def test_empty_request_rows_are_rejected() -> None:
    payload = cast("InferenceRequest", valid_request().payload)
    request = replace_envelope(valid_request(), payload=msgspec.structs.replace(payload, rows=()))

    with pytest.raises(ValidationError) as error:
        validate_current(request)

    assert_code(error, ValidationCode.INVALID_ROW_COUNT)


def test_oversized_request_rows_are_rejected() -> None:
    payload = cast("InferenceRequest", valid_request().payload)
    rows = tuple(
        FeatureRow(market=f"M{index}", sleeve="15m", values=(0.1,)) for index in range(MAX_ROWS + 1)
    )
    request = replace_envelope(valid_request(), payload=msgspec.structs.replace(payload, rows=rows))

    with pytest.raises(ValidationError) as error:
        validate_current(request)

    assert_code(error, ValidationCode.INVALID_ROW_COUNT)


def test_duplicate_or_oversized_artifact_list_is_rejected() -> None:
    payload = cast("InferenceRequest", valid_request().payload)

    for artifact_ids in [
        ("b3:champion", "b3:champion"),
        tuple(f"b3:{index}" for index in range(MAX_ARTIFACTS + 1)),
    ]:
        request = replace_envelope(
            valid_request(),
            payload=msgspec.structs.replace(payload, artifact_ids=artifact_ids),
        )
        with pytest.raises(ValidationError) as error:
            validate_current(request)
        assert_code(error, ValidationCode.INVALID_ARTIFACTS)


@pytest.mark.parametrize(
    "changes",
    [
        {"config_hash": "b3:other"},
        {"feature_schema_hash": "b3:other"},
    ],
)
def test_response_config_or_feature_digest_mismatch_is_rejected(changes: dict[str, str]) -> None:
    response = valid_response()
    payload = cast("InferenceResponse", response.payload)
    response = replace_envelope(response, payload=msgspec.structs.replace(payload, **changes))

    with pytest.raises(ValidationError) as error:
        validate_response_for_request(valid_request(), response)

    assert_code(error, ValidationCode.DIGEST_MISMATCH)


def test_response_artifact_digest_mismatch_is_rejected() -> None:
    response = valid_response()
    payload = cast("InferenceResponse", response.payload)
    row = msgspec.structs.replace(payload.rows[0], artifact_id="b3:other")
    response = replace_envelope(response, payload=msgspec.structs.replace(payload, rows=(row,)))

    with pytest.raises(ValidationError) as error:
        validate_response_for_request(valid_request(), response)

    assert_code(error, ValidationCode.DIGEST_MISMATCH)


@pytest.mark.parametrize("correlation_target", ["response", "row"])
def test_response_correlation_id_mismatch_is_rejected(correlation_target: str) -> None:
    response = valid_response()
    payload = cast("InferenceResponse", response.payload)
    if correlation_target == "response":
        payload = msgspec.structs.replace(payload, correlation_id="evt-other")
    else:
        row = msgspec.structs.replace(payload.rows[0], correlation_id="evt-other")
        payload = msgspec.structs.replace(payload, rows=(row,))
    response = replace_envelope(response, payload=payload)

    with pytest.raises(ValidationError) as error:
        validate_response_for_request(valid_request(), response)

    assert_code(error, ValidationCode.CORRELATION_MISMATCH)


def test_response_as_of_time_must_match_request_exactly() -> None:
    response = replace_envelope(valid_response(), as_of_time_ns=EVENT_TIME_NS - 1)

    with pytest.raises(ValidationError) as error:
        validate_response_for_request(valid_request(), response)

    assert_code(error, ValidationCode.CORRELATION_MISMATCH)


@pytest.mark.parametrize(
    "row_changes",
    [
        {"market": "BTC"},
        {"sleeve": "1h"},
    ],
)
def test_response_row_mismatch_is_rejected(row_changes: dict[str, str]) -> None:
    response = valid_response()
    payload = cast("InferenceResponse", response.payload)
    row = msgspec.structs.replace(payload.rows[0], **row_changes)
    response = replace_envelope(response, payload=msgspec.structs.replace(payload, rows=(row,)))

    with pytest.raises(ValidationError) as error:
        validate_response_for_request(valid_request(), response)

    assert_code(error, ValidationCode.RESPONSE_ROW_MISMATCH)


@pytest.mark.parametrize(
    ("payload_type", "payload"),
    [
        ("inference_response", valid_request().payload),
        ("inference_request", valid_response().payload),
        ("unknown", valid_request().payload),
    ],
)
def test_payload_type_must_match_closed_payload_union(payload_type: str, payload: object) -> None:
    envelope = replace_envelope(valid_request(), payload_type=payload_type, payload=payload)

    with pytest.raises(ValidationError) as error:
        validate_current(envelope)

    assert_code(error, ValidationCode.PAYLOAD_TYPE_MISMATCH)


def test_trailing_messagepack_bytes_are_rejected() -> None:
    frame = encode_envelope(valid_request()) + b"\x00"

    with pytest.raises(ValidationError) as error:
        decode_envelope(frame)

    assert_code(error, ValidationCode.MALFORMED_FRAME)


def test_oversized_frame_is_rejected_before_decode() -> None:
    with pytest.raises(ValidationError) as error:
        decode_envelope(b"\x00" * (MAX_FRAME_BYTES + 1))

    assert_code(error, ValidationCode.FRAME_TOO_LARGE)


@pytest.mark.parametrize("field", ["event_id", "producer_version", "run_id", "config_hash"])
@pytest.mark.parametrize("value", ["", "x" * (MAX_IDENTIFIER_LENGTH + 1)])
def test_empty_or_oversized_envelope_identifier_is_rejected(field: str, value: str) -> None:
    request = replace_envelope(valid_request(), **{field: value})

    with pytest.raises(ValidationError) as error:
        validate_current(request)

    assert_code(error, ValidationCode.INVALID_IDENTIFIER)


def test_normative_contract_matches_python_wire_constants() -> None:
    contract_path = Path(__file__).parents[2] / "schemas" / "inference-v1.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))

    assert getattr(schema_module, "NORMATIVE_CONTRACT", None) == contract

    assert contract["schema_version"] == SCHEMA_VERSION
    assert contract["version_policy"] == "reject_unknown"
    assert contract["timestamp_unit"] == "nanoseconds_since_unix_epoch_utc"
    assert contract["probability_order"] == ["short", "flat", "long"]
    assert contract["supported_sleeves"] == list(SUPPORTED_SLEEVES)
    assert contract["maxima"] == {
        "frame_bytes": MAX_FRAME_BYTES,
        "identifier_bytes": MAX_IDENTIFIER_LENGTH,
        "rows": MAX_ROWS,
        "features_per_row": MAX_FEATURES,
        "artifacts": MAX_ARTIFACTS,
    }
    assert contract["envelope_fields"] == list(ENVELOPE_FIELDS)
    assert contract["payload_variants"]["inference_request"]["fields"] == list(
        INFERENCE_REQUEST_FIELDS
    )
    assert contract["payload_variants"]["inference_request"]["row_fields"] == list(
        FEATURE_ROW_FIELDS
    )
    assert contract["payload_variants"]["inference_response"]["fields"] == list(
        INFERENCE_RESPONSE_FIELDS
    )
    assert contract["payload_variants"]["inference_response"]["row_fields"] == list(
        FORECAST_ROW_FIELDS
    )
    assert contract["probability_sum_tolerance"] == PROBABILITY_TOLERANCE


def test_example_config_parses_into_frozen_strict_settings() -> None:
    settings = load_settings(EXAMPLE_CONFIG_PATH)

    assert settings.runtime.unix_socket_path.endswith(".sock")
    assert settings.runtime.max_frame_bytes == MAX_FRAME_BYTES
    assert settings.runtime.max_rows == MAX_ROWS
    assert settings.runtime.max_features == MAX_FEATURES
    assert settings.runtime.max_artifacts == MAX_ARTIFACTS
    with pytest.raises(AttributeError):
        settings.runtime.thread_count = 2


def test_config_rejects_unknown_fields_without_echoing_values(tmp_path: Path) -> None:
    config_path = tmp_path / "invalid.toml"
    config_path.write_text('[paths]\nunknown_secret = "do-not-echo"\n', encoding="utf-8")

    with pytest.raises(ConfigError) as error:
        load_settings(config_path)

    assert "do-not-echo" not in str(error.value)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("max_frame_bytes = 1048576", "max_frame_bytes = 1048577"),
        ("max_rows = 256", "max_rows = 257"),
        ("max_features = 512", "max_features = 513"),
        ("max_artifacts = 16", "max_artifacts = 17"),
    ],
)
def test_configured_protocol_limits_cannot_exceed_wire_maxima(
    tmp_path: Path, old: str, new: str
) -> None:
    with pytest.raises(ConfigError):
        load_modified_config(tmp_path, old, new)


def test_shadow_artifact_limit_cannot_exceed_artifact_limit(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        load_modified_config(tmp_path, "max_artifacts = 16", "max_artifacts = 1")


def test_zero_shadow_artifact_limit_is_valid(tmp_path: Path) -> None:
    settings = load_modified_config(
        tmp_path,
        "shadow_artifact_limit = 2",
        "shadow_artifact_limit = 0",
    )

    assert settings.runtime.shadow_artifact_limit == 0


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("request_deadline_ms = 250", "request_deadline_ms = 60001"),
        ("thread_count = 1", "thread_count = 257"),
        (
            "deterministic_seed = 20260803",
            "deterministic_seed = 18446744073709551616",
        ),
    ],
)
def test_runtime_resources_have_finite_operational_bounds(
    tmp_path: Path, old: str, new: str
) -> None:
    with pytest.raises(ConfigError):
        load_modified_config(tmp_path, old, new)


def test_unix_socket_path_must_fit_linux_sockaddr_un(tmp_path: Path) -> None:
    oversized_path = "/" + ("x" * 102) + ".sock"

    with pytest.raises(ConfigError):
        load_modified_config(
            tmp_path,
            'unix_socket_path = "/run/trench/trench-ml.sock"',
            f'unix_socket_path = "{oversized_path}"',
        )


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("num_leaves = [15, 31]", "num_leaves = [1]"),
        ("learning_rate = [0.03, 0.05]", "learning_rate = [0.0]"),
        ("feature_fraction = [0.8, 1.0]", "feature_fraction = [0.8, 1.01]"),
        ("bagging_fraction = [0.8, 1.0]", "bagging_fraction = [-0.1, 0.8]"),
    ],
)
def test_lightgbm_grid_values_stay_inside_parameter_domains(
    tmp_path: Path, old: str, new: str
) -> None:
    with pytest.raises(ConfigError):
        load_modified_config(tmp_path, old, new)


def test_lightgbm_native_and_research_grid_bounds_are_exposed() -> None:
    assert (
        getattr(config_module, "LIGHTGBM_MIN_NUM_LEAVES", None) == EXPECTED_LIGHTGBM_MIN_NUM_LEAVES
    )
    assert (
        getattr(config_module, "LIGHTGBM_MAX_NUM_LEAVES", None) == EXPECTED_LIGHTGBM_MAX_NUM_LEAVES
    )
    assert (
        getattr(config_module, "LIGHTGBM_MIN_CHILD_SAMPLES_MIN", None)
        == EXPECTED_LIGHTGBM_MIN_CHILD_SAMPLES_MIN
    )
    assert (
        getattr(config_module, "LIGHTGBM_MIN_CHILD_SAMPLES_MAX", None)
        == EXPECTED_LIGHTGBM_MIN_CHILD_SAMPLES_MAX
    )
    assert (
        getattr(config_module, "MAX_LIGHTGBM_GRID_VALUES_PER_DIMENSION", None)
        == EXPECTED_MAX_LIGHTGBM_GRID_VALUES_PER_DIMENSION
    )
    assert (
        getattr(config_module, "MAX_LIGHTGBM_GRID_COMBINATIONS", None)
        == EXPECTED_MAX_LIGHTGBM_GRID_COMBINATIONS
    )


def test_lightgbm_integer_native_boundaries_are_accepted(tmp_path: Path) -> None:
    settings = load_config_with_replacements(
        tmp_path,
        {
            "num_leaves = [15, 31]": (
                f"num_leaves = [{EXPECTED_LIGHTGBM_MIN_NUM_LEAVES}, "
                f"{EXPECTED_LIGHTGBM_MAX_NUM_LEAVES}]"
            ),
            "min_child_samples = [50, 100]": (
                f"min_child_samples = [{EXPECTED_LIGHTGBM_MIN_CHILD_SAMPLES_MIN}, "
                f"{EXPECTED_LIGHTGBM_MIN_CHILD_SAMPLES_MAX}]"
            ),
        },
    )

    assert settings.lightgbm_grid.num_leaves == (2, 131_072)
    assert settings.lightgbm_grid.min_child_samples == (0, 2_147_483_647)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("num_leaves = [15, 31]", "num_leaves = [131073]"),
        ("min_child_samples = [50, 100]", "min_child_samples = [-1]"),
        ("min_child_samples = [50, 100]", "min_child_samples = [2147483648]"),
    ],
)
def test_lightgbm_integer_values_outside_native_boundaries_are_rejected(
    tmp_path: Path,
    old: str,
    new: str,
) -> None:
    with pytest.raises(ConfigError):
        load_modified_config(tmp_path, old, new)


def test_lightgbm_grid_dimension_at_research_cap_is_accepted(tmp_path: Path) -> None:
    values = ", ".join(str(value) for value in range(2, 10))

    settings = load_config_with_replacements(
        tmp_path,
        {
            "num_leaves = [15, 31]": f"num_leaves = [{values}]",
            "learning_rate = [0.03, 0.05]": "learning_rate = [0.03]",
            "min_child_samples = [50, 100]": "min_child_samples = [50]",
            "feature_fraction = [0.8, 1.0]": "feature_fraction = [0.8]",
            "bagging_fraction = [0.8, 1.0]": "bagging_fraction = [0.8]",
        },
    )

    assert len(settings.lightgbm_grid.num_leaves) == 8


def test_lightgbm_grid_dimension_over_research_cap_is_rejected(tmp_path: Path) -> None:
    values = ", ".join(str(value) for value in range(2, 11))

    with pytest.raises(ConfigError):
        load_modified_config(
            tmp_path,
            "num_leaves = [15, 31]",
            f"num_leaves = [{values}]",
        )


def test_lightgbm_grid_cartesian_product_at_research_cap_is_accepted(tmp_path: Path) -> None:
    settings = load_config_with_replacements(
        tmp_path,
        {
            "num_leaves = [15, 31]": "num_leaves = [2, 3, 4, 5]",
            "learning_rate = [0.03, 0.05]": "learning_rate = [0.01, 0.02, 0.03, 0.04]",
            "min_child_samples = [50, 100]": "min_child_samples = [1, 2]",
            "feature_fraction = [0.8, 1.0]": "feature_fraction = [0.8, 1.0]",
            "bagging_fraction = [0.8, 1.0]": "bagging_fraction = [0.8]",
        },
    )

    dimensions = (
        settings.lightgbm_grid.num_leaves,
        settings.lightgbm_grid.learning_rate,
        settings.lightgbm_grid.min_child_samples,
        settings.lightgbm_grid.feature_fraction,
        settings.lightgbm_grid.bagging_fraction,
    )
    assert math.prod(len(dimension) for dimension in dimensions) == 64


def test_lightgbm_grid_nearest_product_over_research_cap_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        load_config_with_replacements(
            tmp_path,
            {
                "num_leaves = [15, 31]": "num_leaves = [2, 3, 4, 5, 6, 7, 8]",
                "learning_rate = [0.03, 0.05]": ("learning_rate = [0.01, 0.02, 0.03, 0.04, 0.05]"),
                "min_child_samples = [50, 100]": "min_child_samples = [1, 2]",
                "feature_fraction = [0.8, 1.0]": "feature_fraction = [0.8]",
                "bagging_fraction = [0.8, 1.0]": "bagging_fraction = [0.8]",
            },
        )


def test_lightgbm_grid_combinatorial_explosion_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        load_config_with_replacements(
            tmp_path,
            {
                "num_leaves = [15, 31]": "num_leaves = [2, 3, 4]",
                "learning_rate = [0.03, 0.05]": "learning_rate = [0.01, 0.02, 0.03]",
                "min_child_samples = [50, 100]": "min_child_samples = [1, 2, 3]",
                "feature_fraction = [0.8, 1.0]": "feature_fraction = [0.6, 0.8, 1.0]",
                "bagging_fraction = [0.8, 1.0]": "bagging_fraction = [0.6, 0.8, 1.0]",
            },
        )


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("outer_train_days = 730", "outer_train_days = 60"),
        ("inner_train_days = 365", "inner_train_days = 700"),
        ("outer_step_days = 90", "outer_step_days = 91"),
        ("inner_step_days = 60", "inner_step_days = 61"),
        ('start_date = "2021-01-01"', 'start_date = "2025-01-01"'),
    ],
)
def test_fold_windows_and_steps_must_be_feasible(tmp_path: Path, old: str, new: str) -> None:
    with pytest.raises(ConfigError):
        load_modified_config(tmp_path, old, new)


def test_calibration_window_is_separate_from_outer_development_window(tmp_path: Path) -> None:
    source = EXAMPLE_CONFIG_PATH.read_text(encoding="utf-8")
    replacements = {
        "outer_train_days = 730": "outer_train_days = 305",
        "outer_test_days = 90": "outer_test_days = 30",
        "outer_step_days = 90": "outer_step_days = 30",
        "inner_train_days = 365": "inner_train_days = 275",
        "inner_validation_days = 60": "inner_validation_days = 30",
        "inner_step_days = 60": "inner_step_days = 30",
    }
    for old, new in replacements.items():
        assert old in source
        source = source.replace(old, new)
    config_path = tmp_path / "ml.toml"
    config_path.write_text(source, encoding="utf-8")

    settings = load_settings(config_path)

    assert settings.folds.inner_train_days + settings.folds.inner_validation_days == 305
    assert settings.folds.calibration_days == 60
