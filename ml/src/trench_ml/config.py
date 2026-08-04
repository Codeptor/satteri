"""Strict ML configuration parsing without credentials."""

from __future__ import annotations

import math
import tomllib
from datetime import date
from typing import TYPE_CHECKING

import msgspec

from trench_ml.schema import MAX_ARTIFACTS, MAX_FEATURES, MAX_FRAME_BYTES, MAX_ROWS

if TYPE_CHECKING:
    from pathlib import Path

MAX_REQUEST_DEADLINE_MS = 60_000
MAX_THREAD_COUNT = 256
MAX_DETERMINISTIC_SEED = (1 << 64) - 1
MAX_UNIX_SOCKET_PATH_BYTES = 107

# LightGBM 4.7 native Config bounds plus the local deterministic-search budget.
LIGHTGBM_MIN_NUM_LEAVES = 2
LIGHTGBM_MAX_NUM_LEAVES = 131_072
LIGHTGBM_MIN_CHILD_SAMPLES_MIN = 0
LIGHTGBM_MIN_CHILD_SAMPLES_MAX = (1 << 31) - 1
MAX_LIGHTGBM_GRID_VALUES_PER_DIMENSION = 8
MAX_LIGHTGBM_GRID_COMBINATIONS = 64


class ConfigError(ValueError):
    """A non-sensitive configuration validation failure."""


class PathsSettings(msgspec.Struct, forbid_unknown_fields=True, frozen=True):
    feature_dataset: str
    label_dataset: str
    artifact_dir: str
    report_dir: str


class RuntimeSettings(msgspec.Struct, forbid_unknown_fields=True, frozen=True):
    unix_socket_path: str
    request_deadline_ms: int
    deterministic_seed: int
    thread_count: int
    max_frame_bytes: int
    max_rows: int
    max_features: int
    max_artifacts: int
    shadow_artifact_limit: int


class FoldSettings(msgspec.Struct, forbid_unknown_fields=True, frozen=True):
    start_date: str
    end_date: str
    outer_train_days: int
    outer_test_days: int
    outer_step_days: int
    inner_train_days: int
    inner_validation_days: int
    inner_step_days: int
    calibration_days: int
    embargo_bars: int


class LightgbmGrid(msgspec.Struct, forbid_unknown_fields=True, frozen=True):
    num_leaves: tuple[int, ...]
    learning_rate: tuple[float, ...]
    min_child_samples: tuple[int, ...]
    feature_fraction: tuple[float, ...]
    bagging_fraction: tuple[float, ...]


class Settings(msgspec.Struct, forbid_unknown_fields=True, frozen=True):
    paths: PathsSettings
    runtime: RuntimeSettings
    folds: FoldSettings
    lightgbm_grid: LightgbmGrid


def _validate_local_path(value: str) -> None:
    if not value.strip() or "://" in value or "\x00" in value:
        raise ConfigError("ML paths must be nonempty local filesystem paths")


def _validate_positive(values: tuple[int, ...]) -> None:
    if any(type(value) is not int or value <= 0 for value in values):
        raise ConfigError("ML integer limits must be positive")


def _validate_runtime(runtime: RuntimeSettings) -> None:
    _validate_positive(
        (
            runtime.request_deadline_ms,
            runtime.thread_count,
            runtime.max_frame_bytes,
            runtime.max_rows,
            runtime.max_features,
            runtime.max_artifacts,
        )
    )
    if runtime.request_deadline_ms > MAX_REQUEST_DEADLINE_MS:
        raise ConfigError("ML request deadline exceeds the operational bound")
    if runtime.thread_count > MAX_THREAD_COUNT:
        raise ConfigError("ML thread count exceeds the operational bound")
    if (
        runtime.max_frame_bytes > MAX_FRAME_BYTES
        or runtime.max_rows > MAX_ROWS
        or runtime.max_features > MAX_FEATURES
        or runtime.max_artifacts > MAX_ARTIFACTS
    ):
        raise ConfigError("ML runtime limit exceeds the inference protocol maximum")
    if (
        type(runtime.shadow_artifact_limit) is not int
        or not 0 <= runtime.shadow_artifact_limit <= runtime.max_artifacts
    ):
        raise ConfigError("ML shadow artifact limit must fit the artifact limit")
    if (
        type(runtime.deterministic_seed) is not int
        or not 0 <= runtime.deterministic_seed <= MAX_DETERMINISTIC_SEED
    ):
        raise ConfigError("ML deterministic seed must be an unsigned 64-bit integer")


def _validate_folds(folds: FoldSettings) -> None:
    _validate_positive(
        (
            folds.outer_train_days,
            folds.outer_test_days,
            folds.outer_step_days,
            folds.inner_train_days,
            folds.inner_validation_days,
            folds.inner_step_days,
            folds.calibration_days,
            folds.embargo_bars,
        )
    )
    try:
        start_date = date.fromisoformat(folds.start_date)
        end_date = date.fromisoformat(folds.end_date)
    except ValueError as error:
        raise ConfigError("ML fold dates must use ISO-8601 calendar dates") from error
    if start_date >= end_date:
        raise ConfigError("ML fold start date must precede its end date")
    required_days = folds.outer_train_days + folds.calibration_days + folds.outer_test_days
    if (end_date - start_date).days < required_days:
        raise ConfigError("ML fold date range cannot produce one complete outer fold")
    if folds.calibration_days >= folds.outer_train_days:
        raise ConfigError("ML calibration window must fit inside the outer training window")
    if folds.inner_train_days + folds.inner_validation_days > folds.outer_train_days:
        raise ConfigError("ML inner fold must fit inside outer development")
    if folds.outer_step_days > folds.outer_test_days:
        raise ConfigError("ML outer fold step cannot leave gaps between test windows")
    if folds.inner_step_days > folds.inner_validation_days:
        raise ConfigError("ML inner fold step cannot leave gaps between validation windows")


def _validate_lightgbm_grid(grid: LightgbmGrid) -> None:
    dimensions = (
        grid.num_leaves,
        grid.learning_rate,
        grid.min_child_samples,
        grid.feature_fraction,
        grid.bagging_fraction,
    )
    if not all(dimensions):
        raise ConfigError("LightGBM grid dimensions must be nonempty")
    if any(len(dimension) > MAX_LIGHTGBM_GRID_VALUES_PER_DIMENSION for dimension in dimensions):
        raise ConfigError("LightGBM grid dimension exceeds the research limit")
    if math.prod(len(dimension) for dimension in dimensions) > MAX_LIGHTGBM_GRID_COMBINATIONS:
        raise ConfigError("LightGBM grid Cartesian product exceeds the research limit")
    if any(
        type(value) is not int or not LIGHTGBM_MIN_NUM_LEAVES <= value <= LIGHTGBM_MAX_NUM_LEAVES
        for value in grid.num_leaves
    ):
        raise ConfigError("LightGBM num_leaves values are outside the native range")
    if any(
        type(value) is not int
        or not LIGHTGBM_MIN_CHILD_SAMPLES_MIN <= value <= LIGHTGBM_MIN_CHILD_SAMPLES_MAX
        for value in grid.min_child_samples
    ):
        raise ConfigError("LightGBM min_child_samples values are outside the native range")
    if any(
        type(value) is not float or not math.isfinite(value) or value <= 0.0
        for value in grid.learning_rate
    ):
        raise ConfigError("LightGBM learning rates must be finite and positive")
    if any(
        type(value) is not float or not math.isfinite(value) or not 0.0 < value <= 1.0
        for value in (*grid.feature_fraction, *grid.bagging_fraction)
    ):
        raise ConfigError("LightGBM fractions must be finite and inside (0,1]")


def _validate_settings(settings: Settings) -> None:
    for path in (
        settings.paths.feature_dataset,
        settings.paths.label_dataset,
        settings.paths.artifact_dir,
        settings.paths.report_dir,
        settings.runtime.unix_socket_path,
    ):
        _validate_local_path(path)
    if not settings.runtime.unix_socket_path.endswith(".sock"):
        raise ConfigError("ML runtime endpoint must be a Unix socket path")
    if len(settings.runtime.unix_socket_path.encode("utf-8")) > MAX_UNIX_SOCKET_PATH_BYTES:
        raise ConfigError("ML Unix socket path exceeds the platform byte limit")
    _validate_runtime(settings.runtime)
    _validate_folds(settings.folds)
    _validate_lightgbm_grid(settings.lightgbm_grid)


def load_settings(path: Path) -> Settings:
    """Load a strict frozen configuration without exposing its values in errors."""

    try:
        with path.open("rb") as config_file:
            raw = tomllib.load(config_file)
        settings = msgspec.convert(raw, type=Settings, strict=True)
        _validate_settings(settings)
    except ConfigError:
        raise
    except (OSError, tomllib.TOMLDecodeError, msgspec.ValidationError) as error:
        raise ConfigError("invalid ML configuration") from error
    return settings
