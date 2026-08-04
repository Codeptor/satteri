CREATE TABLE engine_event_admissions (
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    ledger_id TEXT NOT NULL CHECK (ledger_id IN ('rules_only', 'ml_champion')),
    event_id TEXT NOT NULL,
    admitted_at_ns INTEGER NOT NULL CHECK (admitted_at_ns >= 0),
    duplicate_attempts INTEGER NOT NULL DEFAULT 0 CHECK (duplicate_attempts >= 0),
    PRIMARY KEY (run_id, ledger_id, event_id),
    FOREIGN KEY (run_id, event_id) REFERENCES events(run_id, event_id)
) STRICT;

CREATE TABLE engine_batch_records (
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    ledger_id TEXT NOT NULL CHECK (ledger_id IN ('rules_only', 'ml_champion')),
    event_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    record_kind TEXT NOT NULL CHECK (record_kind IN (
        'snapshot', 'signal', 'intent', 'risk', 'order', 'fill', 'ledger', 'breaker'
    )),
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    PRIMARY KEY (run_id, ledger_id, event_id, sequence),
    FOREIGN KEY (run_id, event_id) REFERENCES events(run_id, event_id),
    FOREIGN KEY (run_id, ledger_id, event_id)
        REFERENCES engine_event_admissions(run_id, ledger_id, event_id)
) STRICT;

CREATE TABLE engine_checkpoints (
    checkpoint_id TEXT PRIMARY KEY NOT NULL CHECK (length(checkpoint_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    ledger_id TEXT NOT NULL CHECK (ledger_id IN ('rules_only', 'ml_champion')),
    event_id TEXT NOT NULL,
    as_of_time_ns INTEGER NOT NULL CHECK (as_of_time_ns >= 0),
    state_digest TEXT NOT NULL CHECK (length(state_digest) BETWEEN 1 AND 128),
    state_json TEXT NOT NULL CHECK (json_valid(state_json)),
    UNIQUE (run_id, ledger_id, event_id),
    FOREIGN KEY (run_id, event_id) REFERENCES events(run_id, event_id),
    FOREIGN KEY (run_id, ledger_id, event_id)
        REFERENCES engine_event_admissions(run_id, ledger_id, event_id)
) STRICT;

CREATE TRIGGER engine_event_admissions_no_rewrite
BEFORE UPDATE ON engine_event_admissions
WHEN NEW.run_id <> OLD.run_id
  OR NEW.ledger_id <> OLD.ledger_id
  OR NEW.event_id <> OLD.event_id
  OR NEW.admitted_at_ns <> OLD.admitted_at_ns
  OR NEW.duplicate_attempts <> OLD.duplicate_attempts + 1
BEGIN
    SELECT RAISE(ABORT, 'engine admission identity is immutable');
END;

CREATE TRIGGER engine_event_admissions_no_delete
BEFORE DELETE ON engine_event_admissions
BEGIN
    SELECT RAISE(ABORT, 'engine admissions are append-only');
END;

CREATE TRIGGER engine_batch_records_no_update
BEFORE UPDATE ON engine_batch_records
BEGIN
    SELECT RAISE(ABORT, 'engine batch records are immutable');
END;

CREATE TRIGGER engine_batch_records_no_delete
BEFORE DELETE ON engine_batch_records
BEGIN
    SELECT RAISE(ABORT, 'engine batch records are append-only');
END;

CREATE TRIGGER engine_checkpoints_no_update
BEFORE UPDATE ON engine_checkpoints
BEGIN
    SELECT RAISE(ABORT, 'engine checkpoints are immutable');
END;

CREATE TRIGGER engine_checkpoints_no_delete
BEFORE DELETE ON engine_checkpoints
BEGIN
    SELECT RAISE(ABORT, 'engine checkpoints are append-only');
END;
