CREATE TABLE runs (
    run_id TEXT PRIMARY KEY NOT NULL CHECK (length(run_id) BETWEEN 1 AND 128),
    started_at_ns INTEGER NOT NULL CHECK (started_at_ns >= 0)
) STRICT;

CREATE TABLE config_manifests (
    manifest_id TEXT PRIMARY KEY NOT NULL CHECK (length(manifest_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    config_hash TEXT NOT NULL UNIQUE CHECK (length(config_hash) BETWEEN 1 AND 128),
    manifest_json TEXT NOT NULL CHECK (json_valid(manifest_json)),
    created_at_ns INTEGER NOT NULL CHECK (created_at_ns >= 0),
    UNIQUE (run_id, manifest_id)
) STRICT;

CREATE TABLE config_manifest_owners (
    manifest_id TEXT PRIMARY KEY NOT NULL CHECK (length(manifest_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    FOREIGN KEY (run_id, manifest_id)
        REFERENCES config_manifests(run_id, manifest_id) ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER config_manifests_claim_owner
AFTER INSERT ON config_manifests
BEGIN
    INSERT INTO config_manifest_owners (manifest_id, run_id)
    VALUES (NEW.manifest_id, NEW.run_id);
END;

CREATE TRIGGER config_manifests_no_update
BEFORE UPDATE ON config_manifests
BEGIN
    SELECT RAISE(ABORT, 'config manifests are immutable');
END;

CREATE TRIGGER config_manifests_no_delete
BEFORE DELETE ON config_manifests
BEGIN
    SELECT RAISE(ABORT, 'config manifests are immutable');
END;

CREATE TRIGGER config_manifest_owners_no_update
BEFORE UPDATE ON config_manifest_owners
BEGIN
    SELECT RAISE(ABORT, 'config manifest ownership is immutable');
END;

CREATE TRIGGER config_manifest_owners_no_delete
BEFORE DELETE ON config_manifest_owners
BEGIN
    SELECT RAISE(ABORT, 'config manifest ownership is immutable');
END;

CREATE TABLE events (
    event_id TEXT PRIMARY KEY NOT NULL CHECK (length(event_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    event_time_ns INTEGER NOT NULL CHECK (event_time_ns >= 0),
    event_kind TEXT NOT NULL CHECK (length(event_kind) BETWEEN 1 AND 64),
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    UNIQUE (run_id, event_time_ns, event_id),
    UNIQUE (run_id, event_id)
) STRICT;

CREATE TABLE universe_snapshots (
    snapshot_id TEXT PRIMARY KEY NOT NULL CHECK (length(snapshot_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    as_of_time_ns INTEGER NOT NULL CHECK (as_of_time_ns >= 0),
    snapshot_json TEXT NOT NULL CHECK (json_valid(snapshot_json)),
    UNIQUE (run_id, as_of_time_ns, snapshot_id)
) STRICT;

CREATE TABLE feature_snapshots (
    snapshot_id TEXT PRIMARY KEY NOT NULL CHECK (length(snapshot_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    event_id TEXT NOT NULL,
    as_of_time_ns INTEGER NOT NULL CHECK (as_of_time_ns >= 0),
    market TEXT NOT NULL CHECK (length(market) BETWEEN 1 AND 32),
    sleeve TEXT NOT NULL CHECK (sleeve IN ('15m', '1h')),
    schema_hash TEXT NOT NULL CHECK (length(schema_hash) BETWEEN 1 AND 128),
    snapshot_json TEXT NOT NULL CHECK (json_valid(snapshot_json)),
    UNIQUE (run_id, market, sleeve, as_of_time_ns),
    FOREIGN KEY (run_id, event_id) REFERENCES events(run_id, event_id)
) STRICT;

CREATE TABLE signals (
    signal_id TEXT PRIMARY KEY NOT NULL CHECK (length(signal_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    event_id TEXT NOT NULL,
    ledger_id TEXT NOT NULL CHECK (ledger_id IN ('rules_only', 'ml_champion')),
    as_of_time_ns INTEGER NOT NULL CHECK (as_of_time_ns >= 0),
    market TEXT NOT NULL CHECK (length(market) BETWEEN 1 AND 32),
    sleeve TEXT NOT NULL CHECK (sleeve IN ('15m', '1h')),
    direction TEXT NOT NULL CHECK (direction IN ('long', 'flat', 'short')),
    score_decimal TEXT NOT NULL CHECK (typeof(score_decimal) = 'text' AND length(score_decimal) BETWEEN 1 AND 128 AND instr(score_decimal, char(0)) = 0 AND score_decimal NOT GLOB '*[^!-~]*' AND json_valid(score_decimal) AND substr(score_decimal, 1, 1) GLOB '[-0-9]' AND score_decimal NOT GLOB '*[eE]*' AND score_decimal <> '-0' AND (instr(score_decimal, '.') = 0 OR substr(score_decimal, -1, 1) <> '0')),
    explanation_json TEXT NOT NULL CHECK (json_valid(explanation_json)),
    UNIQUE (run_id, ledger_id, event_id, signal_id),
    FOREIGN KEY (run_id, event_id) REFERENCES events(run_id, event_id)
) STRICT;

CREATE TABLE order_intents (
    intent_id TEXT PRIMARY KEY NOT NULL CHECK (length(intent_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    event_id TEXT NOT NULL,
    ledger_id TEXT NOT NULL CHECK (ledger_id IN ('rules_only', 'ml_champion')),
    created_at_ns INTEGER NOT NULL CHECK (created_at_ns >= 0),
    market TEXT NOT NULL CHECK (length(market) BETWEEN 1 AND 32),
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    quantity_decimal TEXT NOT NULL CHECK (typeof(quantity_decimal) = 'text' AND length(quantity_decimal) BETWEEN 1 AND 128 AND instr(quantity_decimal, char(0)) = 0 AND quantity_decimal NOT GLOB '*[^!-~]*' AND json_valid(quantity_decimal) AND substr(quantity_decimal, 1, 1) GLOB '[-0-9]' AND quantity_decimal NOT GLOB '*[eE]*' AND quantity_decimal <> '-0' AND (instr(quantity_decimal, '.') = 0 OR substr(quantity_decimal, -1, 1) <> '0')),
    expected_price_decimal TEXT NOT NULL CHECK (typeof(expected_price_decimal) = 'text' AND length(expected_price_decimal) BETWEEN 1 AND 128 AND instr(expected_price_decimal, char(0)) = 0 AND expected_price_decimal NOT GLOB '*[^!-~]*' AND json_valid(expected_price_decimal) AND substr(expected_price_decimal, 1, 1) GLOB '[-0-9]' AND expected_price_decimal NOT GLOB '*[eE]*' AND expected_price_decimal <> '-0' AND (instr(expected_price_decimal, '.') = 0 OR substr(expected_price_decimal, -1, 1) <> '0')),
    UNIQUE (run_id, ledger_id, event_id, intent_id),
    UNIQUE (run_id, ledger_id, intent_id),
    FOREIGN KEY (run_id, event_id) REFERENCES events(run_id, event_id)
) STRICT;

CREATE TABLE risk_decisions (
    decision_id TEXT PRIMARY KEY NOT NULL CHECK (length(decision_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    event_id TEXT NOT NULL,
    ledger_id TEXT NOT NULL CHECK (ledger_id IN ('rules_only', 'ml_champion')),
    decided_at_ns INTEGER NOT NULL CHECK (decided_at_ns >= 0),
    outcome TEXT NOT NULL CHECK (outcome IN ('accepted', 'rejected')),
    reason_code TEXT NOT NULL CHECK (length(reason_code) BETWEEN 1 AND 64),
    details_json TEXT NOT NULL CHECK (json_valid(details_json)),
    UNIQUE (run_id, ledger_id, event_id, decision_id),
    FOREIGN KEY (run_id, event_id) REFERENCES events(run_id, event_id)
) STRICT;

CREATE TABLE paper_orders (
    order_id TEXT PRIMARY KEY NOT NULL CHECK (length(order_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    intent_id TEXT NOT NULL,
    ledger_id TEXT NOT NULL CHECK (ledger_id IN ('rules_only', 'ml_champion')),
    created_at_ns INTEGER NOT NULL CHECK (created_at_ns >= 0),
    market TEXT NOT NULL CHECK (length(market) BETWEEN 1 AND 32),
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    status TEXT NOT NULL CHECK (status IN ('open', 'partially_filled', 'filled', 'cancelled', 'rejected')),
    quantity_decimal TEXT NOT NULL CHECK (typeof(quantity_decimal) = 'text' AND length(quantity_decimal) BETWEEN 1 AND 128 AND instr(quantity_decimal, char(0)) = 0 AND quantity_decimal NOT GLOB '*[^!-~]*' AND json_valid(quantity_decimal) AND substr(quantity_decimal, 1, 1) GLOB '[-0-9]' AND quantity_decimal NOT GLOB '*[eE]*' AND quantity_decimal <> '-0' AND (instr(quantity_decimal, '.') = 0 OR substr(quantity_decimal, -1, 1) <> '0')),
    limit_price_decimal TEXT NOT NULL CHECK (typeof(limit_price_decimal) = 'text' AND length(limit_price_decimal) BETWEEN 1 AND 128 AND instr(limit_price_decimal, char(0)) = 0 AND limit_price_decimal NOT GLOB '*[^!-~]*' AND json_valid(limit_price_decimal) AND substr(limit_price_decimal, 1, 1) GLOB '[-0-9]' AND limit_price_decimal NOT GLOB '*[eE]*' AND limit_price_decimal <> '-0' AND (instr(limit_price_decimal, '.') = 0 OR substr(limit_price_decimal, -1, 1) <> '0')),
    UNIQUE (run_id, ledger_id, order_id),
    FOREIGN KEY (run_id, ledger_id, intent_id)
        REFERENCES order_intents(run_id, ledger_id, intent_id)
) STRICT;

CREATE TABLE fills (
    fill_id TEXT PRIMARY KEY NOT NULL CHECK (length(fill_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    event_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    ledger_id TEXT NOT NULL CHECK (ledger_id IN ('rules_only', 'ml_champion')),
    fill_time_ns INTEGER NOT NULL CHECK (fill_time_ns >= 0),
    price_decimal TEXT NOT NULL CHECK (typeof(price_decimal) = 'text' AND length(price_decimal) BETWEEN 1 AND 128 AND instr(price_decimal, char(0)) = 0 AND price_decimal NOT GLOB '*[^!-~]*' AND json_valid(price_decimal) AND substr(price_decimal, 1, 1) GLOB '[-0-9]' AND price_decimal NOT GLOB '*[eE]*' AND price_decimal <> '-0' AND (instr(price_decimal, '.') = 0 OR substr(price_decimal, -1, 1) <> '0')),
    quantity_decimal TEXT NOT NULL CHECK (typeof(quantity_decimal) = 'text' AND length(quantity_decimal) BETWEEN 1 AND 128 AND instr(quantity_decimal, char(0)) = 0 AND quantity_decimal NOT GLOB '*[^!-~]*' AND json_valid(quantity_decimal) AND substr(quantity_decimal, 1, 1) GLOB '[-0-9]' AND quantity_decimal NOT GLOB '*[eE]*' AND quantity_decimal <> '-0' AND (instr(quantity_decimal, '.') = 0 OR substr(quantity_decimal, -1, 1) <> '0')),
    fee_decimal TEXT NOT NULL CHECK (typeof(fee_decimal) = 'text' AND length(fee_decimal) BETWEEN 1 AND 128 AND instr(fee_decimal, char(0)) = 0 AND fee_decimal NOT GLOB '*[^!-~]*' AND json_valid(fee_decimal) AND substr(fee_decimal, 1, 1) GLOB '[-0-9]' AND fee_decimal NOT GLOB '*[eE]*' AND fee_decimal <> '-0' AND (instr(fee_decimal, '.') = 0 OR substr(fee_decimal, -1, 1) <> '0')),
    liquidity TEXT NOT NULL CHECK (liquidity IN ('maker', 'taker')),
    UNIQUE (run_id, ledger_id, event_id, fill_id),
    FOREIGN KEY (run_id, event_id) REFERENCES events(run_id, event_id),
    FOREIGN KEY (run_id, ledger_id, order_id)
        REFERENCES paper_orders(run_id, ledger_id, order_id)
) STRICT;

CREATE TABLE positions (
    position_id TEXT PRIMARY KEY NOT NULL CHECK (length(position_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    ledger_id TEXT NOT NULL CHECK (ledger_id IN ('rules_only', 'ml_champion')),
    updated_at_ns INTEGER NOT NULL CHECK (updated_at_ns >= 0),
    market TEXT NOT NULL CHECK (length(market) BETWEEN 1 AND 32),
    side TEXT NOT NULL CHECK (side IN ('long', 'short')),
    status TEXT NOT NULL CHECK (status IN ('open', 'closed')),
    quantity_decimal TEXT NOT NULL CHECK (typeof(quantity_decimal) = 'text' AND length(quantity_decimal) BETWEEN 1 AND 128 AND instr(quantity_decimal, char(0)) = 0 AND quantity_decimal NOT GLOB '*[^!-~]*' AND json_valid(quantity_decimal) AND substr(quantity_decimal, 1, 1) GLOB '[-0-9]' AND quantity_decimal NOT GLOB '*[eE]*' AND quantity_decimal <> '-0' AND (instr(quantity_decimal, '.') = 0 OR substr(quantity_decimal, -1, 1) <> '0')),
    entry_price_decimal TEXT NOT NULL CHECK (typeof(entry_price_decimal) = 'text' AND length(entry_price_decimal) BETWEEN 1 AND 128 AND instr(entry_price_decimal, char(0)) = 0 AND entry_price_decimal NOT GLOB '*[^!-~]*' AND json_valid(entry_price_decimal) AND substr(entry_price_decimal, 1, 1) GLOB '[-0-9]' AND entry_price_decimal NOT GLOB '*[eE]*' AND entry_price_decimal <> '-0' AND (instr(entry_price_decimal, '.') = 0 OR substr(entry_price_decimal, -1, 1) <> '0')),
    realized_pnl_decimal TEXT NOT NULL CHECK (typeof(realized_pnl_decimal) = 'text' AND length(realized_pnl_decimal) BETWEEN 1 AND 128 AND instr(realized_pnl_decimal, char(0)) = 0 AND realized_pnl_decimal NOT GLOB '*[^!-~]*' AND json_valid(realized_pnl_decimal) AND substr(realized_pnl_decimal, 1, 1) GLOB '[-0-9]' AND realized_pnl_decimal NOT GLOB '*[eE]*' AND realized_pnl_decimal <> '-0' AND (instr(realized_pnl_decimal, '.') = 0 OR substr(realized_pnl_decimal, -1, 1) <> '0')),
    unrealized_pnl_decimal TEXT NOT NULL CHECK (typeof(unrealized_pnl_decimal) = 'text' AND length(unrealized_pnl_decimal) BETWEEN 1 AND 128 AND instr(unrealized_pnl_decimal, char(0)) = 0 AND unrealized_pnl_decimal NOT GLOB '*[^!-~]*' AND json_valid(unrealized_pnl_decimal) AND substr(unrealized_pnl_decimal, 1, 1) GLOB '[-0-9]' AND unrealized_pnl_decimal NOT GLOB '*[eE]*' AND unrealized_pnl_decimal <> '-0' AND (instr(unrealized_pnl_decimal, '.') = 0 OR substr(unrealized_pnl_decimal, -1, 1) <> '0')),
    UNIQUE (run_id, ledger_id, market, position_id),
    UNIQUE (run_id, ledger_id, position_id)
) STRICT;

CREATE TABLE funding_entries (
    entry_id TEXT PRIMARY KEY NOT NULL CHECK (length(entry_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    event_id TEXT NOT NULL,
    position_id TEXT NOT NULL,
    ledger_id TEXT NOT NULL CHECK (ledger_id IN ('rules_only', 'ml_champion')),
    funding_time_ns INTEGER NOT NULL CHECK (funding_time_ns >= 0),
    rate_decimal TEXT NOT NULL CHECK (typeof(rate_decimal) = 'text' AND length(rate_decimal) BETWEEN 1 AND 128 AND instr(rate_decimal, char(0)) = 0 AND rate_decimal NOT GLOB '*[^!-~]*' AND json_valid(rate_decimal) AND substr(rate_decimal, 1, 1) GLOB '[-0-9]' AND rate_decimal NOT GLOB '*[eE]*' AND rate_decimal <> '-0' AND (instr(rate_decimal, '.') = 0 OR substr(rate_decimal, -1, 1) <> '0')),
    amount_decimal TEXT NOT NULL CHECK (typeof(amount_decimal) = 'text' AND length(amount_decimal) BETWEEN 1 AND 128 AND instr(amount_decimal, char(0)) = 0 AND amount_decimal NOT GLOB '*[^!-~]*' AND json_valid(amount_decimal) AND substr(amount_decimal, 1, 1) GLOB '[-0-9]' AND amount_decimal NOT GLOB '*[eE]*' AND amount_decimal <> '-0' AND (instr(amount_decimal, '.') = 0 OR substr(amount_decimal, -1, 1) <> '0')),
    UNIQUE (run_id, ledger_id, event_id, entry_id),
    FOREIGN KEY (run_id, event_id) REFERENCES events(run_id, event_id),
    FOREIGN KEY (run_id, ledger_id, position_id)
        REFERENCES positions(run_id, ledger_id, position_id)
) STRICT;

CREATE TABLE equity_snapshots (
    snapshot_id TEXT PRIMARY KEY NOT NULL CHECK (length(snapshot_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    ledger_id TEXT NOT NULL CHECK (ledger_id IN ('rules_only', 'ml_champion')),
    as_of_time_ns INTEGER NOT NULL CHECK (as_of_time_ns >= 0),
    cash_decimal TEXT NOT NULL CHECK (typeof(cash_decimal) = 'text' AND length(cash_decimal) BETWEEN 1 AND 128 AND instr(cash_decimal, char(0)) = 0 AND cash_decimal NOT GLOB '*[^!-~]*' AND json_valid(cash_decimal) AND substr(cash_decimal, 1, 1) GLOB '[-0-9]' AND cash_decimal NOT GLOB '*[eE]*' AND cash_decimal <> '-0' AND (instr(cash_decimal, '.') = 0 OR substr(cash_decimal, -1, 1) <> '0')),
    isolated_margin_decimal TEXT NOT NULL CHECK (typeof(isolated_margin_decimal) = 'text' AND length(isolated_margin_decimal) BETWEEN 1 AND 128 AND instr(isolated_margin_decimal, char(0)) = 0 AND isolated_margin_decimal NOT GLOB '*[^!-~]*' AND json_valid(isolated_margin_decimal) AND substr(isolated_margin_decimal, 1, 1) GLOB '[-0-9]' AND isolated_margin_decimal NOT GLOB '*[eE]*' AND isolated_margin_decimal <> '-0' AND (instr(isolated_margin_decimal, '.') = 0 OR substr(isolated_margin_decimal, -1, 1) <> '0')),
    realized_pnl_decimal TEXT NOT NULL CHECK (typeof(realized_pnl_decimal) = 'text' AND length(realized_pnl_decimal) BETWEEN 1 AND 128 AND instr(realized_pnl_decimal, char(0)) = 0 AND realized_pnl_decimal NOT GLOB '*[^!-~]*' AND json_valid(realized_pnl_decimal) AND substr(realized_pnl_decimal, 1, 1) GLOB '[-0-9]' AND realized_pnl_decimal NOT GLOB '*[eE]*' AND realized_pnl_decimal <> '-0' AND (instr(realized_pnl_decimal, '.') = 0 OR substr(realized_pnl_decimal, -1, 1) <> '0')),
    unrealized_pnl_decimal TEXT NOT NULL CHECK (typeof(unrealized_pnl_decimal) = 'text' AND length(unrealized_pnl_decimal) BETWEEN 1 AND 128 AND instr(unrealized_pnl_decimal, char(0)) = 0 AND unrealized_pnl_decimal NOT GLOB '*[^!-~]*' AND json_valid(unrealized_pnl_decimal) AND substr(unrealized_pnl_decimal, 1, 1) GLOB '[-0-9]' AND unrealized_pnl_decimal NOT GLOB '*[eE]*' AND unrealized_pnl_decimal <> '-0' AND (instr(unrealized_pnl_decimal, '.') = 0 OR substr(unrealized_pnl_decimal, -1, 1) <> '0')),
    equity_decimal TEXT NOT NULL CHECK (typeof(equity_decimal) = 'text' AND length(equity_decimal) BETWEEN 1 AND 128 AND instr(equity_decimal, char(0)) = 0 AND equity_decimal NOT GLOB '*[^!-~]*' AND json_valid(equity_decimal) AND substr(equity_decimal, 1, 1) GLOB '[-0-9]' AND equity_decimal NOT GLOB '*[eE]*' AND equity_decimal <> '-0' AND (instr(equity_decimal, '.') = 0 OR substr(equity_decimal, -1, 1) <> '0')),
    UNIQUE (run_id, ledger_id, as_of_time_ns, snapshot_id)
) STRICT;

CREATE TABLE transition_ids (
    transition_id TEXT PRIMARY KEY NOT NULL CHECK (length(transition_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    owner_table TEXT NOT NULL CHECK (owner_table IN ('breaker_transitions', 'health_transitions')),
    UNIQUE (run_id, transition_id)
) STRICT;

CREATE TRIGGER transition_ids_no_replace
BEFORE INSERT ON transition_ids
WHEN EXISTS (
    SELECT 1 FROM transition_ids WHERE transition_id = NEW.transition_id
)
BEGIN
    SELECT RAISE(ABORT, 'transition ID ownership is immutable');
END;

CREATE TRIGGER transition_ids_no_update
BEFORE UPDATE ON transition_ids
BEGIN
    SELECT RAISE(ABORT, 'transition ID ownership is immutable');
END;

CREATE TRIGGER transition_ids_no_delete
BEFORE DELETE ON transition_ids
BEGIN
    SELECT RAISE(ABORT, 'transition ID ownership is immutable');
END;

CREATE TABLE breaker_transitions (
    transition_id TEXT PRIMARY KEY NOT NULL CHECK (length(transition_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    event_id TEXT NOT NULL,
    ledger_id TEXT NOT NULL CHECK (ledger_id IN ('rules_only', 'ml_champion')),
    transitioned_at_ns INTEGER NOT NULL CHECK (transitioned_at_ns >= 0),
    breaker_kind TEXT NOT NULL CHECK (breaker_kind IN ('daily', 'weekly', 'drawdown', 'cooldown')),
    from_state TEXT NOT NULL CHECK (from_state IN ('clear', 'active', 'latched')),
    to_state TEXT NOT NULL CHECK (to_state IN ('clear', 'active', 'latched')),
    reason_code TEXT NOT NULL CHECK (length(reason_code) BETWEEN 1 AND 64),
    UNIQUE (run_id, ledger_id, transitioned_at_ns, transition_id),
    FOREIGN KEY (run_id, event_id) REFERENCES events(run_id, event_id),
    FOREIGN KEY (run_id, transition_id)
        REFERENCES transition_ids(run_id, transition_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TRIGGER breaker_transitions_claim_id
AFTER INSERT ON breaker_transitions
BEGIN
    INSERT INTO transition_ids (transition_id, run_id, owner_table)
    VALUES (NEW.transition_id, NEW.run_id, 'breaker_transitions');
END;

CREATE TRIGGER breaker_transitions_no_update
BEFORE UPDATE ON breaker_transitions
BEGIN
    SELECT RAISE(ABORT, 'breaker transitions are immutable');
END;

CREATE TRIGGER breaker_transitions_no_delete
BEFORE DELETE ON breaker_transitions
BEGIN
    SELECT RAISE(ABORT, 'breaker transitions are immutable');
END;

CREATE TABLE health_transitions (
    transition_id TEXT PRIMARY KEY NOT NULL CHECK (length(transition_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    observed_at_ns INTEGER NOT NULL CHECK (observed_at_ns >= 0),
    component TEXT NOT NULL CHECK (length(component) BETWEEN 1 AND 64),
    from_state TEXT NOT NULL CHECK (from_state IN ('ready', 'degraded', 'blocked')),
    to_state TEXT NOT NULL CHECK (to_state IN ('ready', 'degraded', 'blocked')),
    reason_code TEXT NOT NULL CHECK (length(reason_code) BETWEEN 1 AND 64),
    UNIQUE (run_id, observed_at_ns, transition_id),
    FOREIGN KEY (run_id, transition_id)
        REFERENCES transition_ids(run_id, transition_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TRIGGER health_transitions_claim_id
AFTER INSERT ON health_transitions
BEGIN
    INSERT INTO transition_ids (transition_id, run_id, owner_table)
    VALUES (NEW.transition_id, NEW.run_id, 'health_transitions');
END;

CREATE TRIGGER health_transitions_no_update
BEFORE UPDATE ON health_transitions
BEGIN
    SELECT RAISE(ABORT, 'health transitions are immutable');
END;

CREATE TRIGGER health_transitions_no_delete
BEFORE DELETE ON health_transitions
BEGIN
    SELECT RAISE(ABORT, 'health transitions are immutable');
END;

CREATE TABLE reconciliation_checkpoints (
    checkpoint_id TEXT PRIMARY KEY NOT NULL CHECK (length(checkpoint_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    ledger_id TEXT NOT NULL CHECK (ledger_id IN ('rules_only', 'ml_champion')),
    as_of_time_ns INTEGER NOT NULL CHECK (as_of_time_ns >= 0),
    position_digest TEXT NOT NULL CHECK (length(position_digest) BETWEEN 1 AND 128),
    ledger_digest TEXT NOT NULL CHECK (length(ledger_digest) BETWEEN 1 AND 128),
    UNIQUE (run_id, ledger_id, as_of_time_ns, checkpoint_id)
) STRICT;

CREATE INDEX events_run_order ON events (run_id, event_time_ns, event_id);
CREATE INDEX universe_snapshots_run_order ON universe_snapshots (run_id, as_of_time_ns, snapshot_id);
CREATE INDEX feature_snapshots_run_order ON feature_snapshots (run_id, as_of_time_ns, snapshot_id);
CREATE INDEX signals_run_order ON signals (run_id, as_of_time_ns, signal_id);
CREATE INDEX order_intents_run_order ON order_intents (run_id, created_at_ns, intent_id);
CREATE INDEX risk_decisions_run_order ON risk_decisions (run_id, decided_at_ns, decision_id);
CREATE INDEX paper_orders_run_order ON paper_orders (run_id, created_at_ns, order_id);
CREATE INDEX fills_run_order ON fills (run_id, fill_time_ns, fill_id);
CREATE INDEX positions_run_order ON positions (run_id, updated_at_ns, position_id);
CREATE INDEX funding_entries_run_order ON funding_entries (run_id, funding_time_ns, entry_id);
CREATE INDEX equity_snapshots_run_order ON equity_snapshots (run_id, as_of_time_ns, snapshot_id);
CREATE INDEX breaker_transitions_run_order ON breaker_transitions (run_id, transitioned_at_ns, transition_id);
CREATE INDEX health_transitions_run_order ON health_transitions (run_id, observed_at_ns, transition_id);
CREATE INDEX reconciliation_checkpoints_run_order ON reconciliation_checkpoints (run_id, as_of_time_ns, checkpoint_id);
