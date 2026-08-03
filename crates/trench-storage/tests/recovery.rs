use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt, symlink};
#[cfg(unix)]
use std::process::Command;

use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};
use tempfile::TempDir;
use trench_storage::sqlite::{
    AtomicAppend, EventInput, LedgerId, RiskRejectionInput, RunInput, SqliteStore, StoreError,
};

const RUN_ID: &str = "run-2026-08-03";
const EVENT_ID: &str = "event-0001";
const DECISION_ID: &str = "risk-0001";
const BREAKER_TRANSITION_ID: &str = "breaker-transition-original";
const HEALTH_TRANSITION_ID: &str = "health-transition-original";
const EVENT_TIME_NS: i64 = 1_785_715_200_123_456_789;

const PUBLIC_JOURNAL_TABLES: [&str; 16] = [
    "breaker_transitions",
    "config_manifests",
    "equity_snapshots",
    "events",
    "feature_snapshots",
    "fills",
    "funding_entries",
    "health_transitions",
    "order_intents",
    "paper_orders",
    "positions",
    "reconciliation_checkpoints",
    "risk_decisions",
    "runs",
    "signals",
    "universe_snapshots",
];

const ASCII_CONTROL_BYTES: [u8; 34] = [
    9, 10, 13, 11, 12, 0, 1, 2, 3, 4, 5, 6, 7, 8, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31, 32, 127,
];
const NON_CANONICAL_DECIMALS: [&str; 7] = ["1e3", "1E3", "+1", "01", "-01", "-0", "1.0"];

#[derive(Clone, Copy)]
struct DecimalColumn {
    name: &'static str,
    canonical: &'static str,
}

#[derive(Clone, Copy)]
struct DecimalTable {
    name: &'static str,
    id_column: &'static str,
    row_id: &'static str,
    columns: &'static [&'static str],
    decimal_columns: &'static [DecimalColumn],
}

const DECIMAL_TABLES: [DecimalTable; 7] = [
    DecimalTable {
        name: "signals",
        id_column: "signal_id",
        row_id: "signal-canonical",
        columns: &[
            "signal_id",
            "run_id",
            "event_id",
            "ledger_id",
            "as_of_time_ns",
            "market",
            "sleeve",
            "direction",
            "score_decimal",
            "explanation_json",
        ],
        decimal_columns: &[DecimalColumn {
            name: "score_decimal",
            canonical: "-0.125",
        }],
    },
    DecimalTable {
        name: "order_intents",
        id_column: "intent_id",
        row_id: "intent-canonical",
        columns: &[
            "intent_id",
            "run_id",
            "event_id",
            "ledger_id",
            "created_at_ns",
            "market",
            "side",
            "quantity_decimal",
            "expected_price_decimal",
        ],
        decimal_columns: &[
            DecimalColumn {
                name: "quantity_decimal",
                canonical: "1.25",
            },
            DecimalColumn {
                name: "expected_price_decimal",
                canonical: "64000.5",
            },
        ],
    },
    DecimalTable {
        name: "paper_orders",
        id_column: "order_id",
        row_id: "order-canonical",
        columns: &[
            "order_id",
            "run_id",
            "intent_id",
            "ledger_id",
            "created_at_ns",
            "market",
            "side",
            "status",
            "quantity_decimal",
            "limit_price_decimal",
        ],
        decimal_columns: &[
            DecimalColumn {
                name: "quantity_decimal",
                canonical: "1.25",
            },
            DecimalColumn {
                name: "limit_price_decimal",
                canonical: "64000.5",
            },
        ],
    },
    DecimalTable {
        name: "fills",
        id_column: "fill_id",
        row_id: "fill-canonical",
        columns: &[
            "fill_id",
            "run_id",
            "event_id",
            "order_id",
            "ledger_id",
            "fill_time_ns",
            "price_decimal",
            "quantity_decimal",
            "fee_decimal",
            "liquidity",
        ],
        decimal_columns: &[
            DecimalColumn {
                name: "price_decimal",
                canonical: "64001.25",
            },
            DecimalColumn {
                name: "quantity_decimal",
                canonical: "0.75",
            },
            DecimalColumn {
                name: "fee_decimal",
                canonical: "0.01",
            },
        ],
    },
    DecimalTable {
        name: "positions",
        id_column: "position_id",
        row_id: "position-canonical",
        columns: &[
            "position_id",
            "run_id",
            "ledger_id",
            "updated_at_ns",
            "market",
            "side",
            "status",
            "quantity_decimal",
            "entry_price_decimal",
            "realized_pnl_decimal",
            "unrealized_pnl_decimal",
        ],
        decimal_columns: &[
            DecimalColumn {
                name: "quantity_decimal",
                canonical: "0.75",
            },
            DecimalColumn {
                name: "entry_price_decimal",
                canonical: "64000.5",
            },
            DecimalColumn {
                name: "realized_pnl_decimal",
                canonical: "-12.5",
            },
            DecimalColumn {
                name: "unrealized_pnl_decimal",
                canonical: "3.25",
            },
        ],
    },
    DecimalTable {
        name: "funding_entries",
        id_column: "entry_id",
        row_id: "funding-canonical",
        columns: &[
            "entry_id",
            "run_id",
            "event_id",
            "position_id",
            "ledger_id",
            "funding_time_ns",
            "rate_decimal",
            "amount_decimal",
        ],
        decimal_columns: &[
            DecimalColumn {
                name: "rate_decimal",
                canonical: "-0.0001",
            },
            DecimalColumn {
                name: "amount_decimal",
                canonical: "-0.25",
            },
        ],
    },
    DecimalTable {
        name: "equity_snapshots",
        id_column: "snapshot_id",
        row_id: "equity-canonical",
        columns: &[
            "snapshot_id",
            "run_id",
            "ledger_id",
            "as_of_time_ns",
            "cash_decimal",
            "isolated_margin_decimal",
            "realized_pnl_decimal",
            "unrealized_pnl_decimal",
            "equity_decimal",
        ],
        decimal_columns: &[
            DecimalColumn {
                name: "cash_decimal",
                canonical: "98.5",
            },
            DecimalColumn {
                name: "isolated_margin_decimal",
                canonical: "5",
            },
            DecimalColumn {
                name: "realized_pnl_decimal",
                canonical: "-12.5",
            },
            DecimalColumn {
                name: "unrealized_pnl_decimal",
                canonical: "3.25",
            },
            DecimalColumn {
                name: "equity_decimal",
                canonical: "94.25",
            },
        ],
    },
];

#[derive(Clone, Copy)]
enum ControlPlacement {
    Suffix,
    Prefix,
    Interior,
}

fn database_path(temp_dir: &TempDir) -> std::path::PathBuf {
    temp_dir.path().join("private").join("journal.sqlite3")
}

async fn open_schema_connection(path: &Path) -> SqliteConnection {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .foreign_keys(true);
    SqliteConnection::connect_with(&options)
        .await
        .expect("database should open for schema verification")
}

async fn open_with_run(path: &Path) -> SqliteStore {
    let mut store = SqliteStore::open(path).await.expect("database should open");
    store
        .create_run(RunInput {
            run_id: RUN_ID,
            started_at_ns: 1_785_715_200_000_000_000,
        })
        .await
        .expect("run should be created");
    store
}

fn event() -> EventInput<'static> {
    EventInput {
        run_id: RUN_ID,
        event_id: EVENT_ID,
        event_time_ns: EVENT_TIME_NS,
        kind: "risk_evaluation",
        payload_json: r#"{"market":"BTC"}"#,
    }
}

fn rejection() -> RiskRejectionInput<'static> {
    RiskRejectionInput {
        run_id: RUN_ID,
        decision_id: DECISION_ID,
        event_id: EVENT_ID,
        ledger_id: LedgerId::RulesOnly,
        decided_at_ns: EVENT_TIME_NS + 1,
        reason_code: "daily_loss_limit",
        details_json: r#"{"remaining_equity":"98.50"}"#,
    }
}

impl ControlPlacement {
    const fn label(self) -> &'static str {
        match self {
            Self::Suffix => "suffix",
            Self::Prefix => "prefix",
            Self::Interior => "interior",
        }
    }
}

fn decimal_with_control(byte: u8, placement: ControlPlacement) -> String {
    let control = char::from(byte);
    match placement {
        ControlPlacement::Suffix => format!("12.5{control}"),
        ControlPlacement::Prefix => format!("{control}12.5"),
        ControlPlacement::Interior => format!("12{control}.5"),
    }
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{identifier}\"")
}

fn copy_insert_statement(table: DecimalTable, decimal_column: &str) -> String {
    let columns = table
        .columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let values = table
        .columns
        .iter()
        .map(|column| {
            if *column == table.id_column {
                "?1".to_owned()
            } else if *column == decimal_column {
                "?2".to_owned()
            } else {
                quote_identifier(column)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "INSERT INTO {table_name} ({columns}) \
         SELECT {values} FROM {table_name} WHERE {id_column} = ?3",
        table_name = quote_identifier(table.name),
        id_column = quote_identifier(table.id_column),
    )
}

async fn seed_canonical_journal(path: &Path) {
    let mut store = open_with_run(path).await;
    store
        .append_event_and_risk_rejection(event(), rejection(), AtomicAppend::Commit)
        .await
        .expect("source event and risk decision should be committed");
    drop(store);

    let mut connection = open_schema_connection(path).await;
    let mut transaction = connection
        .begin()
        .await
        .expect("canonical journal seed should begin");
    sqlx::query(
        "INSERT INTO config_manifests \
         (manifest_id, run_id, config_hash, manifest_json, created_at_ns) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
    )
    .bind("manifest-canonical")
    .bind(RUN_ID)
    .bind("config-hash-canonical")
    .bind("{}")
    .bind(EVENT_TIME_NS)
    .execute(&mut *transaction)
    .await
    .expect("canonical config manifest should be inserted");
    sqlx::query(
        "INSERT INTO universe_snapshots \
         (snapshot_id, run_id, as_of_time_ns, snapshot_json) \
         VALUES (?1, ?2, ?3, ?4)",
    )
    .bind("universe-canonical")
    .bind(RUN_ID)
    .bind(EVENT_TIME_NS + 1)
    .bind("{}")
    .execute(&mut *transaction)
    .await
    .expect("canonical universe snapshot should be inserted");
    sqlx::query(
        "INSERT INTO feature_snapshots \
         (snapshot_id, run_id, event_id, as_of_time_ns, market, sleeve, schema_hash, snapshot_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    )
    .bind("feature-canonical")
    .bind(RUN_ID)
    .bind(EVENT_ID)
    .bind(EVENT_TIME_NS + 2)
    .bind("BTC")
    .bind("15m")
    .bind("schema-hash-canonical")
    .bind("{}")
    .execute(&mut *transaction)
    .await
    .expect("canonical feature snapshot should be inserted");
    sqlx::query(
        "INSERT INTO signals \
         (signal_id, run_id, event_id, ledger_id, as_of_time_ns, market, sleeve, direction, score_decimal, explanation_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )
    .bind("signal-canonical")
    .bind(RUN_ID)
    .bind(EVENT_ID)
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 3)
    .bind("BTC")
    .bind("15m")
    .bind("long")
    .bind("-0.125")
    .bind("{}")
    .execute(&mut *transaction)
    .await
    .expect("canonical signal should be inserted");
    sqlx::query(
        "INSERT INTO order_intents \
         (intent_id, run_id, event_id, ledger_id, created_at_ns, market, side, quantity_decimal, expected_price_decimal) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )
    .bind("intent-canonical")
    .bind(RUN_ID)
    .bind(EVENT_ID)
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 4)
    .bind("BTC")
    .bind("buy")
    .bind("1.25")
    .bind("64000.5")
    .execute(&mut *transaction)
    .await
    .expect("canonical order intent should be inserted");
    sqlx::query(
        "INSERT INTO paper_orders \
         (order_id, run_id, intent_id, ledger_id, created_at_ns, market, side, status, quantity_decimal, limit_price_decimal) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )
    .bind("order-canonical")
    .bind(RUN_ID)
    .bind("intent-canonical")
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 5)
    .bind("BTC")
    .bind("buy")
    .bind("open")
    .bind("1.25")
    .bind("64000.5")
    .execute(&mut *transaction)
    .await
    .expect("canonical paper order should be inserted");
    sqlx::query(
        "INSERT INTO fills \
         (fill_id, run_id, event_id, order_id, ledger_id, fill_time_ns, price_decimal, quantity_decimal, fee_decimal, liquidity) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )
    .bind("fill-canonical")
    .bind(RUN_ID)
    .bind(EVENT_ID)
    .bind("order-canonical")
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 6)
    .bind("64001.25")
    .bind("0.75")
    .bind("0.01")
    .bind("taker")
    .execute(&mut *transaction)
    .await
    .expect("canonical fill should be inserted");
    sqlx::query(
        "INSERT INTO positions \
         (position_id, run_id, ledger_id, updated_at_ns, market, side, status, quantity_decimal, entry_price_decimal, realized_pnl_decimal, unrealized_pnl_decimal) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
    )
    .bind("position-canonical")
    .bind(RUN_ID)
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 7)
    .bind("BTC")
    .bind("long")
    .bind("open")
    .bind("0.75")
    .bind("64000.5")
    .bind("-12.5")
    .bind("3.25")
    .execute(&mut *transaction)
    .await
    .expect("canonical position should be inserted");
    sqlx::query(
        "INSERT INTO funding_entries \
         (entry_id, run_id, event_id, position_id, ledger_id, funding_time_ns, rate_decimal, amount_decimal) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    )
    .bind("funding-canonical")
    .bind(RUN_ID)
    .bind(EVENT_ID)
    .bind("position-canonical")
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 8)
    .bind("-0.0001")
    .bind("-0.25")
    .execute(&mut *transaction)
    .await
    .expect("canonical funding entry should be inserted");
    sqlx::query(
        "INSERT INTO equity_snapshots \
         (snapshot_id, run_id, ledger_id, as_of_time_ns, cash_decimal, isolated_margin_decimal, realized_pnl_decimal, unrealized_pnl_decimal, equity_decimal) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )
    .bind("equity-canonical")
    .bind(RUN_ID)
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 9)
    .bind("98.5")
    .bind("5")
    .bind("-12.5")
    .bind("3.25")
    .bind("94.25")
    .execute(&mut *transaction)
    .await
    .expect("canonical equity snapshot should be inserted");
    sqlx::query(
        "INSERT INTO breaker_transitions \
         (transition_id, run_id, event_id, ledger_id, transitioned_at_ns, breaker_kind, from_state, to_state, reason_code) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )
    .bind("breaker-canonical")
    .bind(RUN_ID)
    .bind(EVENT_ID)
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 10)
    .bind("daily")
    .bind("clear")
    .bind("active")
    .bind("daily_loss_limit")
    .execute(&mut *transaction)
    .await
    .expect("canonical breaker transition should be inserted");
    sqlx::query(
        "INSERT INTO health_transitions \
         (transition_id, run_id, observed_at_ns, component, from_state, to_state, reason_code) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind("health-canonical")
    .bind(RUN_ID)
    .bind(EVENT_TIME_NS + 11)
    .bind("storage")
    .bind("ready")
    .bind("blocked")
    .bind("sqlite_failure")
    .execute(&mut *transaction)
    .await
    .expect("canonical health transition should be inserted");
    sqlx::query(
        "INSERT INTO reconciliation_checkpoints \
         (checkpoint_id, run_id, ledger_id, as_of_time_ns, position_digest, ledger_digest) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )
    .bind("checkpoint-canonical")
    .bind(RUN_ID)
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 12)
    .bind("position-digest-canonical")
    .bind("ledger-digest-canonical")
    .execute(&mut *transaction)
    .await
    .expect("canonical reconciliation checkpoint should be inserted");
    transaction
        .commit()
        .await
        .expect("canonical journal seed should commit");
}

async fn seed_transition_pair(path: &Path) {
    let mut store = open_with_run(path).await;
    store
        .append_event_and_risk_rejection(event(), rejection(), AtomicAppend::Commit)
        .await
        .expect("source event should be committed");
    drop(store);

    let mut connection = open_schema_connection(path).await;
    sqlx::query(
        "INSERT INTO breaker_transitions \
         (transition_id, run_id, event_id, ledger_id, transitioned_at_ns, breaker_kind, from_state, to_state, reason_code) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )
    .bind(BREAKER_TRANSITION_ID)
    .bind(RUN_ID)
    .bind(EVENT_ID)
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 2)
    .bind("daily")
    .bind("clear")
    .bind("active")
    .bind("daily_loss_limit")
    .execute(&mut connection)
    .await
    .expect("breaker transition should be inserted");
    sqlx::query(
        "INSERT INTO health_transitions \
         (transition_id, run_id, observed_at_ns, component, from_state, to_state, reason_code) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind(HEALTH_TRANSITION_ID)
    .bind(RUN_ID)
    .bind(EVENT_TIME_NS + 3)
    .bind("storage")
    .bind("ready")
    .bind("blocked")
    .bind("sqlite_failure")
    .execute(&mut connection)
    .await
    .expect("health transition should be inserted");
}

async fn assert_rejected_transition_mutation_preserves_journal(
    path: &Path,
    statement: &str,
    expected_error: &str,
) {
    let mut connection = open_schema_connection(path).await;
    let recursive_triggers: i64 = sqlx::query_scalar("PRAGMA recursive_triggers")
        .fetch_one(&mut connection)
        .await
        .expect("recursive-trigger setting should be readable");
    assert_eq!(
        recursive_triggers, 0,
        "the invariant must hold without recursive DELETE triggers"
    );

    let error = sqlx::query(statement)
        .execute(&mut connection)
        .await
        .expect_err("transition journal mutation must be rejected");
    assert!(
        error.to_string().contains(expected_error),
        "unexpected rejection: {error}"
    );
    drop(connection);

    let reopened = SqliteStore::open(path)
        .await
        .expect("database should reopen after rejected mutation");
    drop(reopened);
    let mut verification = open_schema_connection(path).await;
    let breaker = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            i64,
            String,
            String,
            String,
            String,
        ),
    >(
        "SELECT transition_id, run_id, event_id, ledger_id, transitioned_at_ns, \
                breaker_kind, from_state, to_state, reason_code \
         FROM breaker_transitions",
    )
    .fetch_all(&mut verification)
    .await
    .expect("original breaker transition should remain readable");
    let health = sqlx::query_as::<_, (String, String, i64, String, String, String, String)>(
        "SELECT transition_id, run_id, observed_at_ns, component, from_state, to_state, reason_code \
         FROM health_transitions",
    )
    .fetch_all(&mut verification)
    .await
    .expect("original health transition should remain readable");
    let owners = sqlx::query_as::<_, (String, String, String)>(
        "SELECT transition_id, run_id, owner_table \
         FROM transition_ids ORDER BY transition_id",
    )
    .fetch_all(&mut verification)
    .await
    .expect("transition ownership should remain readable");
    let foreign_key_violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut verification)
        .await
        .expect("foreign-key integrity should be checkable");

    assert_eq!(
        breaker,
        vec![(
            BREAKER_TRANSITION_ID.to_owned(),
            RUN_ID.to_owned(),
            EVENT_ID.to_owned(),
            "rules_only".to_owned(),
            EVENT_TIME_NS + 2,
            "daily".to_owned(),
            "clear".to_owned(),
            "active".to_owned(),
            "daily_loss_limit".to_owned(),
        )]
    );
    assert_eq!(
        health,
        vec![(
            HEALTH_TRANSITION_ID.to_owned(),
            RUN_ID.to_owned(),
            EVENT_TIME_NS + 3,
            "storage".to_owned(),
            "ready".to_owned(),
            "blocked".to_owned(),
            "sqlite_failure".to_owned(),
        )]
    );
    assert_eq!(
        owners,
        vec![
            (
                BREAKER_TRANSITION_ID.to_owned(),
                RUN_ID.to_owned(),
                "breaker_transitions".to_owned(),
            ),
            (
                HEALTH_TRANSITION_ID.to_owned(),
                RUN_ID.to_owned(),
                "health_transitions".to_owned(),
            ),
        ]
    );
    assert!(
        foreign_key_violations.is_empty(),
        "foreign_key_check must remain clean"
    );
}

#[tokio::test]
async fn committed_event_and_rejection_survive_reopen() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = open_with_run(&path).await;

    store
        .append_event_and_risk_rejection(event(), rejection(), AtomicAppend::Commit)
        .await
        .expect("atomic append should commit");
    drop(store);

    let mut reopened = SqliteStore::open(&path)
        .await
        .expect("database should reopen");
    let counts = reopened
        .journal_counts(RUN_ID)
        .await
        .expect("journal counts should be readable");

    assert_eq!((counts.events, counts.risk_decisions), (1, 1));
}

#[tokio::test]
async fn failure_between_inserts_rolls_back_both_rows() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = open_with_run(&path).await;

    let error = store
        .append_event_and_risk_rejection(event(), rejection(), AtomicAppend::FailAfterEvent)
        .await
        .expect_err("injected failure should abort the transaction");
    assert!(matches!(error, StoreError::InjectedFailure));
    drop(store);

    let mut reopened = SqliteStore::open(&path)
        .await
        .expect("database should reopen");
    let counts = reopened
        .journal_counts(RUN_ID)
        .await
        .expect("journal counts should be readable");

    assert_eq!((counts.events, counts.risk_decisions), (0, 0));
}

#[tokio::test]
async fn open_enforces_durable_connection_pragmas() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = SqliteStore::open(&path)
        .await
        .expect("database should open");

    let pragmas = store
        .pragma_settings()
        .await
        .expect("pragmas should be readable");

    assert_eq!(
        (
            pragmas.journal_mode.as_str(),
            pragmas.synchronous,
            pragmas.foreign_keys,
        ),
        ("wal", 2, 1)
    );
}

#[tokio::test]
async fn event_times_round_trip_as_unix_nanoseconds() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = open_with_run(&path).await;
    store
        .append_event_and_risk_rejection(event(), rejection(), AtomicAppend::Commit)
        .await
        .expect("atomic append should commit");

    let stored_time = store
        .event_time_ns(EVENT_ID)
        .await
        .expect("event time should be readable");

    assert_eq!(stored_time, EVENT_TIME_NS);
}

#[tokio::test]
async fn schema_rejects_ascii_controls_in_every_decimal_column() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    seed_canonical_journal(&path).await;

    let mut connection = open_schema_connection(&path).await;
    let public_tables = sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_schema \
         WHERE type = 'table' \
           AND name NOT IN ('_sqlx_migrations', 'config_manifest_owners', 'transition_ids') \
           AND name NOT LIKE 'sqlite_%' \
         ORDER BY name",
    )
    .fetch_all(&mut connection)
    .await
    .expect("public journal tables should be discoverable");
    assert_eq!(
        public_tables,
        PUBLIC_JOURNAL_TABLES.map(str::to_owned),
        "the decimal audit must cover the complete 16-table public journal"
    );

    let actual_decimal_columns = sqlx::query_as::<_, (String, String)>(
        "SELECT tables.name, columns.name \
         FROM sqlite_schema AS tables \
         JOIN pragma_table_info(tables.name) AS columns \
         WHERE tables.type = 'table' AND columns.name GLOB '*_decimal' \
         ORDER BY tables.name, columns.name",
    )
    .fetch_all(&mut connection)
    .await
    .expect("decimal columns should be discoverable");
    let mut expected_decimal_columns = DECIMAL_TABLES
        .iter()
        .flat_map(|table| {
            table
                .decimal_columns
                .iter()
                .map(move |column| (table.name.to_owned(), column.name.to_owned()))
        })
        .collect::<Vec<_>>();
    expected_decimal_columns.sort_unstable();
    assert_eq!(expected_decimal_columns.len(), 19);
    assert_eq!(actual_decimal_columns, expected_decimal_columns);

    for table in DECIMAL_TABLES {
        for decimal_column in table.decimal_columns {
            let select = format!(
                "SELECT {decimal_column} FROM {table_name} WHERE {id_column} = ?1",
                decimal_column = quote_identifier(decimal_column.name),
                table_name = quote_identifier(table.name),
                id_column = quote_identifier(table.id_column),
            );
            let stored = sqlx::query_scalar::<_, String>(&select)
                .bind(table.row_id)
                .fetch_one(&mut connection)
                .await
                .expect("canonical decimal insert control should be readable");
            assert_eq!(stored, decimal_column.canonical);
        }
    }

    let mut invalid_decimals = Vec::new();
    for byte in ASCII_CONTROL_BYTES {
        for placement in [
            ControlPlacement::Suffix,
            ControlPlacement::Prefix,
            ControlPlacement::Interior,
        ] {
            invalid_decimals.push((
                decimal_with_control(byte, placement),
                format!("ASCII byte 0x{byte:02x} at {}", placement.label()),
            ));
        }
    }
    invalid_decimals.extend(
        NON_CANONICAL_DECIMALS
            .map(|value| (value.to_owned(), format!("non-canonical decimal {value:?}"))),
    );

    for table in DECIMAL_TABLES {
        for decimal_column in table.decimal_columns {
            let insert = copy_insert_statement(table, decimal_column.name);
            let inserted_count = format!(
                "SELECT COUNT(*) FROM {table_name} WHERE {id_column} = ?1",
                table_name = quote_identifier(table.name),
                id_column = quote_identifier(table.id_column),
            );
            let update = format!(
                "UPDATE {table_name} \
                 SET {decimal_column} = ?1, ledger_id = 'ml_champion' \
                 WHERE {id_column} = ?2",
                table_name = quote_identifier(table.name),
                decimal_column = quote_identifier(decimal_column.name),
                id_column = quote_identifier(table.id_column),
            );
            let stored_row = format!(
                "SELECT {decimal_column}, ledger_id \
                 FROM {table_name} WHERE {id_column} = ?1",
                decimal_column = quote_identifier(decimal_column.name),
                table_name = quote_identifier(table.name),
                id_column = quote_identifier(table.id_column),
            );

            for (invalid, description) in &invalid_decimals {
                let inserted = sqlx::query(&insert)
                    .bind("invalid-decimal-row")
                    .bind(invalid)
                    .bind(table.row_id)
                    .execute(&mut connection)
                    .await;
                assert!(
                    inserted.is_err(),
                    "{}.{} INSERT accepted {description}",
                    table.name,
                    decimal_column.name,
                );
                let count = sqlx::query_scalar::<_, i64>(&inserted_count)
                    .bind("invalid-decimal-row")
                    .fetch_one(&mut connection)
                    .await
                    .expect("rejected decimal insert should leave no row");
                assert_eq!(
                    count, 0,
                    "{}.{} INSERT was not atomic for {description}",
                    table.name, decimal_column.name,
                );

                let updated = sqlx::query(&update)
                    .bind(invalid)
                    .bind(table.row_id)
                    .execute(&mut connection)
                    .await;
                assert!(
                    updated.is_err(),
                    "{}.{} UPDATE accepted {description}",
                    table.name,
                    decimal_column.name,
                );
                let stored = sqlx::query_as::<_, (String, String)>(&stored_row)
                    .bind(table.row_id)
                    .fetch_one(&mut connection)
                    .await
                    .expect("rejected decimal update should preserve the original row");
                assert_eq!(
                    stored,
                    (decimal_column.canonical.to_owned(), "rules_only".to_owned(),),
                    "{}.{} UPDATE was not atomic for {description}",
                    table.name,
                    decimal_column.name,
                );
            }

            let valid_update = format!(
                "UPDATE {table_name} SET {decimal_column} = ?1 WHERE {id_column} = ?2",
                table_name = quote_identifier(table.name),
                decimal_column = quote_identifier(decimal_column.name),
                id_column = quote_identifier(table.id_column),
            );
            sqlx::query(&valid_update)
                .bind("7.125")
                .bind(table.row_id)
                .execute(&mut connection)
                .await
                .expect("alternate canonical decimal update should succeed");
            sqlx::query(&valid_update)
                .bind(decimal_column.canonical)
                .bind(table.row_id)
                .execute(&mut connection)
                .await
                .expect("canonical decimal update control should restore the original value");
        }
    }

    drop(connection);
    let reopened = SqliteStore::open(&path)
        .await
        .expect("database should reopen after rejected decimal mutations");
    drop(reopened);
    let mut verification = open_schema_connection(&path).await;

    for table in PUBLIC_JOURNAL_TABLES {
        let count = sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*) FROM {}",
            quote_identifier(table)
        ))
        .fetch_one(&mut verification)
        .await
        .expect("journal row count should remain readable");
        assert_eq!(count, 1, "{table} row count changed after rejection");
    }
    let ownership_counts = sqlx::query_as::<_, (i64, i64)>(
        "SELECT \
             (SELECT COUNT(*) FROM config_manifest_owners), \
             (SELECT COUNT(*) FROM transition_ids)",
    )
    .fetch_one(&mut verification)
    .await
    .expect("internal ownership row counts should remain readable");
    assert_eq!(ownership_counts, (1, 2));

    for table in DECIMAL_TABLES {
        for decimal_column in table.decimal_columns {
            let select = format!(
                "SELECT {decimal_column}, ledger_id \
                 FROM {table_name} WHERE {id_column} = ?1",
                decimal_column = quote_identifier(decimal_column.name),
                table_name = quote_identifier(table.name),
                id_column = quote_identifier(table.id_column),
            );
            let stored = sqlx::query_as::<_, (String, String)>(&select)
                .bind(table.row_id)
                .fetch_one(&mut verification)
                .await
                .expect("canonical decimal row should survive reopen");
            assert_eq!(
                stored,
                (decimal_column.canonical.to_owned(), "rules_only".to_owned(),)
            );
        }
    }
    let foreign_key_violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut verification)
        .await
        .expect("foreign-key integrity should be checkable after reopen");

    assert!(
        foreign_key_violations.is_empty(),
        "foreign_key_check must remain clean"
    );
}

#[tokio::test]
async fn insert_or_replace_cannot_modify_immutable_config_manifest() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let store = open_with_run(&path).await;
    drop(store);

    let mut connection = open_schema_connection(&path).await;
    let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&mut connection)
        .await
        .expect("foreign-key setting should be readable");
    let recursive_triggers: i64 = sqlx::query_scalar("PRAGMA recursive_triggers")
        .fetch_one(&mut connection)
        .await
        .expect("recursive-trigger setting should be readable");
    sqlx::query(
        "INSERT INTO config_manifests \
         (manifest_id, run_id, config_hash, manifest_json, created_at_ns) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
    )
    .bind("manifest-0001")
    .bind(RUN_ID)
    .bind("hash-original")
    .bind(r#"{"threshold":"0.60"}"#)
    .bind(EVENT_TIME_NS)
    .execute(&mut connection)
    .await
    .expect("initial manifest should be inserted");

    let replacement = sqlx::query(
        "INSERT OR REPLACE INTO config_manifests \
         (manifest_id, run_id, config_hash, manifest_json, created_at_ns) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
    )
    .bind("manifest-0001")
    .bind(RUN_ID)
    .bind("hash-replaced")
    .bind(r#"{"threshold":"0.99"}"#)
    .bind(EVENT_TIME_NS + 1)
    .execute(&mut connection)
    .await;
    drop(connection);

    let reopened = SqliteStore::open(&path)
        .await
        .expect("database should reopen after rejected replacement");
    drop(reopened);
    let mut verification = open_schema_connection(&path).await;
    let stored = sqlx::query_as::<_, (String, String, i64)>(
        "SELECT config_hash, manifest_json, created_at_ns \
         FROM config_manifests WHERE manifest_id = ?1",
    )
    .bind("manifest-0001")
    .fetch_one(&mut verification)
    .await
    .expect("original manifest should remain readable");

    assert_eq!(
        (
            foreign_keys,
            recursive_triggers,
            replacement.is_err(),
            stored
        ),
        (
            1,
            0,
            true,
            (
                "hash-original".to_owned(),
                r#"{"threshold":"0.60"}"#.to_owned(),
                EVENT_TIME_NS,
            ),
        )
    );
}

#[tokio::test]
async fn transition_ids_are_unique_across_transition_tables() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = open_with_run(&path).await;
    store
        .append_event_and_risk_rejection(event(), rejection(), AtomicAppend::Commit)
        .await
        .expect("source event should be committed");
    drop(store);

    let mut connection = open_schema_connection(&path).await;
    sqlx::query(
        "INSERT INTO breaker_transitions \
         (transition_id, run_id, event_id, ledger_id, transitioned_at_ns, breaker_kind, from_state, to_state, reason_code) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )
    .bind("transition-global-0001")
    .bind(RUN_ID)
    .bind(EVENT_ID)
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 2)
    .bind("daily")
    .bind("clear")
    .bind("active")
    .bind("daily_loss_limit")
    .execute(&mut connection)
    .await
    .expect("breaker transition should be inserted");

    let duplicate = sqlx::query(
        "INSERT INTO health_transitions \
         (transition_id, run_id, observed_at_ns, component, from_state, to_state, reason_code) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind("transition-global-0001")
    .bind(RUN_ID)
    .bind(EVENT_TIME_NS + 3)
    .bind("storage")
    .bind("ready")
    .bind("blocked")
    .bind("sqlite_failure")
    .execute(&mut connection)
    .await;

    assert!(
        duplicate.is_err(),
        "a transition ID owned by breaker_transitions must not be reusable"
    );
    drop(connection);

    let reopened = SqliteStore::open(&path)
        .await
        .expect("database should reopen after rejected duplicate");
    drop(reopened);
    let mut verification = open_schema_connection(&path).await;
    let state = sqlx::query_as::<_, (i64, i64, String)>(
        "SELECT \
             (SELECT COUNT(*) FROM breaker_transitions WHERE transition_id = ?1), \
             (SELECT COUNT(*) FROM health_transitions WHERE transition_id = ?1), \
             (SELECT owner_table FROM transition_ids WHERE transition_id = ?1)",
    )
    .bind("transition-global-0001")
    .fetch_one(&mut verification)
    .await
    .expect("transition ownership should remain readable");

    assert_eq!(state, (1, 0, "breaker_transitions".to_owned()));
}

#[tokio::test]
async fn ignored_invalid_transitions_do_not_claim_registry_ids() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = open_with_run(&path).await;
    store
        .append_event_and_risk_rejection(event(), rejection(), AtomicAppend::Commit)
        .await
        .expect("source event should be committed");
    drop(store);

    let mut connection = open_schema_connection(&path).await;
    for (transition_id, expected_counts) in [
        ("ignored-breaker-transition", (1, 0, "breaker_transitions")),
        ("ignored-health-transition", (0, 1, "health_transitions")),
    ] {
        match expected_counts.2 {
            "breaker_transitions" => {
                sqlx::query(
                    "INSERT OR IGNORE INTO breaker_transitions \
                     (transition_id, run_id, event_id, ledger_id, transitioned_at_ns, breaker_kind, from_state, to_state, reason_code) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                )
                .bind(transition_id)
                .bind(RUN_ID)
                .bind(EVENT_ID)
                .bind("rules_only")
                .bind(EVENT_TIME_NS + 2)
                .bind("invalid")
                .bind("clear")
                .bind("active")
                .bind("daily_loss_limit")
                .execute(&mut connection)
                .await
                .expect("ignored invalid breaker transition should not abort the statement");
            }
            "health_transitions" => {
                sqlx::query(
                    "INSERT OR IGNORE INTO health_transitions \
                     (transition_id, run_id, observed_at_ns, component, from_state, to_state, reason_code) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                )
                .bind(transition_id)
                .bind(RUN_ID)
                .bind(EVENT_TIME_NS + 3)
                .bind("storage")
                .bind("invalid")
                .bind("blocked")
                .bind("sqlite_failure")
                .execute(&mut connection)
                .await
                .expect("ignored invalid health transition should not abort the statement");
            }
            _ => unreachable!("only known transition tables are covered"),
        }

        let rejected_state = sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT \
                 (SELECT COUNT(*) FROM breaker_transitions WHERE transition_id = ?1), \
                 (SELECT COUNT(*) FROM health_transitions WHERE transition_id = ?1), \
                 (SELECT COUNT(*) FROM transition_ids WHERE transition_id = ?1)",
        )
        .bind(transition_id)
        .fetch_one(&mut connection)
        .await
        .expect("ignored transition state should be readable");
        assert_eq!(
            rejected_state,
            (0, 0, 0),
            "ignored invalid transition must not claim a registry ID"
        );

        match expected_counts.2 {
            "breaker_transitions" => {
                sqlx::query(
                    "INSERT INTO breaker_transitions \
                     (transition_id, run_id, event_id, ledger_id, transitioned_at_ns, breaker_kind, from_state, to_state, reason_code) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                )
                .bind(transition_id)
                .bind(RUN_ID)
                .bind(EVENT_ID)
                .bind("rules_only")
                .bind(EVENT_TIME_NS + 2)
                .bind("daily")
                .bind("clear")
                .bind("active")
                .bind("daily_loss_limit")
                .execute(&mut connection)
                .await
                .expect("valid breaker transition should claim an unclaimed ID");
            }
            "health_transitions" => {
                sqlx::query(
                    "INSERT INTO health_transitions \
                     (transition_id, run_id, observed_at_ns, component, from_state, to_state, reason_code) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                )
                .bind(transition_id)
                .bind(RUN_ID)
                .bind(EVENT_TIME_NS + 3)
                .bind("storage")
                .bind("ready")
                .bind("blocked")
                .bind("sqlite_failure")
                .execute(&mut connection)
                .await
                .expect("valid health transition should claim an unclaimed ID");
            }
            _ => unreachable!("only known transition tables are covered"),
        }

        let state = sqlx::query_as::<_, (i64, i64, i64, Option<String>)>(
            "SELECT \
                 (SELECT COUNT(*) FROM breaker_transitions WHERE transition_id = ?1), \
                 (SELECT COUNT(*) FROM health_transitions WHERE transition_id = ?1), \
                 (SELECT COUNT(*) FROM transition_ids WHERE transition_id = ?1), \
                 (SELECT owner_table FROM transition_ids WHERE transition_id = ?1)",
        )
        .bind(transition_id)
        .fetch_one(&mut connection)
        .await
        .expect("transition state should be readable");
        assert_eq!(
            state,
            (
                expected_counts.0,
                expected_counts.1,
                1,
                Some(expected_counts.2.to_owned()),
            )
        );
    }
}

#[tokio::test]
async fn schema_rejects_cross_run_and_cross_ledger_relationships() {
    const SECOND_RUN_ID: &str = "run-2026-08-03-b";
    const SECOND_EVENT_ID: &str = "event-0002";

    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = open_with_run(&path).await;
    store
        .append_event_and_risk_rejection(event(), rejection(), AtomicAppend::Commit)
        .await
        .expect("first run event should be committed");
    drop(store);

    let mut connection = open_schema_connection(&path).await;
    sqlx::query("INSERT INTO runs (run_id, started_at_ns) VALUES (?1, ?2)")
        .bind(SECOND_RUN_ID)
        .bind(EVENT_TIME_NS)
        .execute(&mut connection)
        .await
        .expect("second run should be inserted");
    sqlx::query(
        "INSERT INTO events (event_id, run_id, event_time_ns, event_kind, payload_json) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
    )
    .bind(SECOND_EVENT_ID)
    .bind(SECOND_RUN_ID)
    .bind(EVENT_TIME_NS + 1)
    .bind("risk_evaluation")
    .bind(r#"{"market":"ETH"}"#)
    .execute(&mut connection)
    .await
    .expect("second run event should be inserted");
    sqlx::query(
        "INSERT INTO config_manifests \
         (manifest_id, run_id, config_hash, manifest_json, created_at_ns) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
    )
    .bind("manifest-first-run")
    .bind(RUN_ID)
    .bind("config-first-run")
    .bind("{}")
    .bind(EVENT_TIME_NS)
    .execute(&mut connection)
    .await
    .expect("first-run config manifest should be inserted");
    sqlx::query(
        "INSERT INTO order_intents \
         (intent_id, run_id, event_id, ledger_id, created_at_ns, market, side, quantity_decimal, expected_price_decimal) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )
    .bind("intent-parent")
    .bind(RUN_ID)
    .bind(EVENT_ID)
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 2)
    .bind("BTC")
    .bind("buy")
    .bind("1")
    .bind("100")
    .execute(&mut connection)
    .await
    .expect("same-run order intent should be inserted");
    sqlx::query(
        "INSERT INTO paper_orders \
         (order_id, run_id, intent_id, ledger_id, created_at_ns, market, side, status, quantity_decimal, limit_price_decimal) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )
    .bind("order-parent")
    .bind(RUN_ID)
    .bind("intent-parent")
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 3)
    .bind("BTC")
    .bind("buy")
    .bind("open")
    .bind("1")
    .bind("100")
    .execute(&mut connection)
    .await
    .expect("same-run paper order should be inserted");
    sqlx::query(
        "INSERT INTO positions \
         (position_id, run_id, ledger_id, updated_at_ns, market, side, status, quantity_decimal, entry_price_decimal, realized_pnl_decimal, unrealized_pnl_decimal) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
    )
    .bind("position-parent")
    .bind(RUN_ID)
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 4)
    .bind("BTC")
    .bind("long")
    .bind("open")
    .bind("1")
    .bind("100")
    .bind("0")
    .bind("0")
    .execute(&mut connection)
    .await
    .expect("same-run position should be inserted");
    sqlx::query(
        "INSERT INTO breaker_transitions \
         (transition_id, run_id, event_id, ledger_id, transitioned_at_ns, breaker_kind, from_state, to_state, reason_code) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )
    .bind("transition-second-run")
    .bind(SECOND_RUN_ID)
    .bind(SECOND_EVENT_ID)
    .bind("rules_only")
    .bind(EVENT_TIME_NS + 5)
    .bind("daily")
    .bind("clear")
    .bind("active")
    .bind("daily_loss_limit")
    .execute(&mut connection)
    .await
    .expect("second-run transition should be inserted");

    for (relationship, statement) in [
        (
            "config manifest ownership",
            "INSERT INTO config_manifest_owners (manifest_id, run_id) \
             VALUES ('manifest-first-run', 'run-2026-08-03-b')",
        ),
        (
            "feature event run",
            "INSERT INTO feature_snapshots \
             (snapshot_id, run_id, event_id, as_of_time_ns, market, sleeve, schema_hash, snapshot_json) \
             VALUES ('feature-cross-run', 'run-2026-08-03', 'event-0002', 1785715200123456790, 'BTC', '15m', 'schema-cross', '{}')",
        ),
        (
            "signal event run",
            "INSERT INTO signals \
             (signal_id, run_id, event_id, ledger_id, as_of_time_ns, market, sleeve, direction, score_decimal, explanation_json) \
             VALUES ('signal-cross-run', 'run-2026-08-03', 'event-0002', 'rules_only', 1785715200123456790, 'BTC', '15m', 'long', '1', '{}')",
        ),
        (
            "order intent event run",
            "INSERT INTO order_intents \
             (intent_id, run_id, event_id, ledger_id, created_at_ns, market, side, quantity_decimal, expected_price_decimal) \
             VALUES ('intent-cross-run', 'run-2026-08-03', 'event-0002', 'rules_only', 1785715200123456790, 'BTC', 'buy', '1', '100')",
        ),
        (
            "risk decision event run",
            "INSERT INTO risk_decisions \
             (decision_id, run_id, event_id, ledger_id, decided_at_ns, outcome, reason_code, details_json) \
             VALUES ('risk-cross-run', 'run-2026-08-03', 'event-0002', 'rules_only', 1785715200123456790, 'rejected', 'cross_run', '{}')",
        ),
        (
            "paper order intent ledger",
            "INSERT INTO paper_orders \
             (order_id, run_id, intent_id, ledger_id, created_at_ns, market, side, status, quantity_decimal, limit_price_decimal) \
             VALUES ('order-cross-ledger', 'run-2026-08-03', 'intent-parent', 'ml_champion', 1785715200123456790, 'BTC', 'buy', 'open', '1', '100')",
        ),
        (
            "fill event run",
            "INSERT INTO fills \
             (fill_id, run_id, event_id, order_id, ledger_id, fill_time_ns, price_decimal, quantity_decimal, fee_decimal, liquidity) \
             VALUES ('fill-cross-event', 'run-2026-08-03', 'event-0002', 'order-parent', 'rules_only', 1785715200123456790, '100', '1', '0', 'maker')",
        ),
        (
            "fill order ledger",
            "INSERT INTO fills \
             (fill_id, run_id, event_id, order_id, ledger_id, fill_time_ns, price_decimal, quantity_decimal, fee_decimal, liquidity) \
             VALUES ('fill-cross-ledger', 'run-2026-08-03', 'event-0001', 'order-parent', 'ml_champion', 1785715200123456790, '100', '1', '0', 'maker')",
        ),
        (
            "funding event run",
            "INSERT INTO funding_entries \
             (entry_id, run_id, event_id, position_id, ledger_id, funding_time_ns, rate_decimal, amount_decimal) \
             VALUES ('funding-cross-event', 'run-2026-08-03', 'event-0002', 'position-parent', 'rules_only', 1785715200123456790, '0', '0')",
        ),
        (
            "funding position ledger",
            "INSERT INTO funding_entries \
             (entry_id, run_id, event_id, position_id, ledger_id, funding_time_ns, rate_decimal, amount_decimal) \
             VALUES ('funding-cross-ledger', 'run-2026-08-03', 'event-0001', 'position-parent', 'ml_champion', 1785715200123456790, '0', '0')",
        ),
        (
            "breaker event run",
            "INSERT INTO breaker_transitions \
             (transition_id, run_id, event_id, ledger_id, transitioned_at_ns, breaker_kind, from_state, to_state, reason_code) \
             VALUES ('transition-cross-event', 'run-2026-08-03', 'event-0002', 'rules_only', 1785715200123456790, 'daily', 'clear', 'active', 'cross_run')",
        ),
        (
            "health transition run",
            "INSERT INTO health_transitions \
             (transition_id, run_id, observed_at_ns, component, from_state, to_state, reason_code) \
             VALUES ('transition-second-run', 'run-2026-08-03', 1785715200123456790, 'storage', 'ready', 'blocked', 'cross_run')",
        ),
    ] {
        let result = sqlx::query(statement).execute(&mut connection).await;
        assert!(result.is_err(), "{relationship} must reject cross scope");
    }

    let foreign_key_violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut connection)
        .await
        .expect("foreign-key integrity should be checkable");
    assert!(foreign_key_violations.is_empty());
}

#[tokio::test]
async fn breaker_transition_id_cannot_be_updated_to_health_owned_id() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    seed_transition_pair(&path).await;

    assert_rejected_transition_mutation_preserves_journal(
        &path,
        "UPDATE breaker_transitions \
         SET transition_id = 'health-transition-original' \
         WHERE transition_id = 'breaker-transition-original'",
        "breaker transitions are immutable",
    )
    .await;
}

#[tokio::test]
async fn health_transition_id_cannot_be_updated_to_breaker_owned_id() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    seed_transition_pair(&path).await;

    assert_rejected_transition_mutation_preserves_journal(
        &path,
        "UPDATE health_transitions \
         SET transition_id = 'breaker-transition-original' \
         WHERE transition_id = 'health-transition-original'",
        "health transitions are immutable",
    )
    .await;
}

#[tokio::test]
async fn breaker_and_health_transition_rows_are_append_only() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    seed_transition_pair(&path).await;

    for (statement, expected_error) in [
        (
            "UPDATE breaker_transitions \
             SET reason_code = 'tampered' \
             WHERE transition_id = 'breaker-transition-original'",
            "breaker transitions are immutable",
        ),
        (
            "DELETE FROM breaker_transitions \
             WHERE transition_id = 'breaker-transition-original'",
            "breaker transitions are immutable",
        ),
        (
            "UPDATE health_transitions \
             SET reason_code = 'tampered' \
             WHERE transition_id = 'health-transition-original'",
            "health transitions are immutable",
        ),
        (
            "DELETE FROM health_transitions \
             WHERE transition_id = 'health-transition-original'",
            "health transitions are immutable",
        ),
    ] {
        assert_rejected_transition_mutation_preserves_journal(&path, statement, expected_error)
            .await;
    }
}

#[tokio::test]
async fn conflict_algorithms_cannot_replace_transitions_or_registry_ownership() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    seed_transition_pair(&path).await;

    for (statement, expected_error) in [
        (
            "UPDATE OR REPLACE breaker_transitions \
             SET reason_code = 'replace-update' \
             WHERE transition_id = 'breaker-transition-original'",
            "breaker transitions are immutable",
        ),
        (
            "UPDATE OR REPLACE health_transitions \
             SET reason_code = 'replace-update' \
             WHERE transition_id = 'health-transition-original'",
            "health transitions are immutable",
        ),
        (
            "INSERT OR REPLACE INTO breaker_transitions \
             (transition_id, run_id, event_id, ledger_id, transitioned_at_ns, breaker_kind, from_state, to_state, reason_code) \
             VALUES ('breaker-transition-original', 'run-2026-08-03', 'event-0001', 'rules_only', \
                     1785715200123456792, 'weekly', 'active', 'latched', 'replace-insert')",
            "transition ID ownership is immutable",
        ),
        (
            "INSERT OR REPLACE INTO health_transitions \
             (transition_id, run_id, observed_at_ns, component, from_state, to_state, reason_code) \
             VALUES ('health-transition-original', 'run-2026-08-03', 1785715200123456793, \
                     'market-data', 'blocked', 'degraded', 'replace-insert')",
            "transition ID ownership is immutable",
        ),
        (
            "INSERT OR REPLACE INTO breaker_transitions \
             (transition_id, run_id, event_id, ledger_id, transitioned_at_ns, breaker_kind, from_state, to_state, reason_code) \
             VALUES ('health-transition-original', 'run-2026-08-03', 'event-0001', 'rules_only', \
                     1785715200123456794, 'daily', 'clear', 'active', 'cross-owner-replace')",
            "transition ID ownership is immutable",
        ),
        (
            "INSERT OR REPLACE INTO health_transitions \
             (transition_id, run_id, observed_at_ns, component, from_state, to_state, reason_code) \
             VALUES ('breaker-transition-original', 'run-2026-08-03', 1785715200123456795, \
                     'storage', 'ready', 'blocked', 'cross-owner-replace')",
            "transition ID ownership is immutable",
        ),
        (
            "INSERT OR REPLACE INTO transition_ids (transition_id, run_id, owner_table) \
             VALUES ('breaker-transition-original', 'run-2026-08-03', 'health_transitions')",
            "transition ID ownership is immutable",
        ),
    ] {
        assert_rejected_transition_mutation_preserves_journal(&path, statement, expected_error)
            .await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn open_restricts_parent_and_database_permissions() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let store = SqliteStore::open(&path)
        .await
        .expect("database should open");

    let parent_mode = std::fs::metadata(path.parent().expect("path should have a parent"))
        .expect("parent metadata should be readable")
        .permissions()
        .mode()
        & 0o777;
    let database_mode = std::fs::metadata(&path)
        .expect("database metadata should be readable")
        .permissions()
        .mode()
        & 0o777;
    drop(store);

    assert_eq!((parent_mode, database_mode), (0o700, 0o600));
}

#[cfg(unix)]
#[tokio::test]
async fn open_rejects_caller_owned_cwd_without_mutating() {
    const CHILD_ENV: &str = "TRENCH_STORAGE_CALLER_CWD_CHILD";
    const DATABASE_NAME: &str = "caller-owned.sqlite3";

    if std::env::var_os(CHILD_ENV).is_some() {
        let result = SqliteStore::open(DATABASE_NAME).await;
        assert!(
            matches!(result, Err(StoreError::InvalidPath { .. })),
            "caller-owned working directories must be rejected rather than secured in place"
        );
        return;
    }

    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let caller_owned_cwd = temp_dir.path().join("caller-owned-cwd");
    std::fs::create_dir(&caller_owned_cwd).expect("caller-owned directory should be created");
    std::fs::set_permissions(&caller_owned_cwd, std::fs::Permissions::from_mode(0o755))
        .expect("caller-owned directory mode should be configured");

    let status = Command::new(std::env::current_exe().expect("test executable should be known"))
        .arg("--exact")
        .arg("open_rejects_caller_owned_cwd_without_mutating")
        .current_dir(&caller_owned_cwd)
        .env(CHILD_ENV, "1")
        .status()
        .expect("isolated CWD probe should run");

    let mode = std::fs::metadata(&caller_owned_cwd)
        .expect("caller-owned directory metadata should be readable")
        .permissions()
        .mode()
        & 0o777;
    assert!(
        status.success(),
        "isolated CWD probe should reject the path"
    );
    assert_eq!(mode, 0o755, "caller-owned CWD mode must remain unchanged");
    assert!(
        !caller_owned_cwd.join(DATABASE_NAME).exists(),
        "rejected caller-owned CWD must not receive a database file"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn open_preserves_tmp_and_rejects_symlink_targets() {
    let tmp_before = std::fs::metadata("/tmp")
        .expect("/tmp metadata should be readable")
        .permissions()
        .mode()
        & 0o777;
    let tmp_file =
        tempfile::NamedTempFile::new_in("/tmp").expect("temporary file in /tmp should be created");
    let tmp_result = SqliteStore::open(tmp_file.path()).await;
    let tmp_after = std::fs::metadata("/tmp")
        .expect("/tmp metadata should remain readable")
        .permissions()
        .mode()
        & 0o777;
    assert!(matches!(tmp_result, Err(StoreError::InvalidPath { .. })));
    assert_eq!(tmp_after, tmp_before, "/tmp mode must remain unchanged");

    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let target_parent = temp_dir.path().join("target-parent");
    std::fs::create_dir(&target_parent).expect("symlink target parent should be created");
    std::fs::set_permissions(&target_parent, std::fs::Permissions::from_mode(0o755))
        .expect("symlink target parent mode should be configured");
    let parent_link = temp_dir.path().join("parent-link");
    symlink(&target_parent, &parent_link).expect("parent symlink should be created");

    let parent_result = SqliteStore::open(parent_link.join("journal.sqlite3")).await;
    let target_parent_mode = std::fs::metadata(&target_parent)
        .expect("symlink target parent metadata should be readable")
        .permissions()
        .mode()
        & 0o777;
    assert!(matches!(parent_result, Err(StoreError::InvalidPath { .. })));
    assert_eq!(target_parent_mode, 0o755);
    assert!(
        !target_parent.join("journal.sqlite3").exists(),
        "a symlink parent must not be followed"
    );

    let dedicated = temp_dir.path().join("dedicated");
    std::fs::create_dir(&dedicated).expect("dedicated directory should be created");
    std::fs::set_permissions(&dedicated, std::fs::Permissions::from_mode(0o700))
        .expect("dedicated directory mode should be configured");
    let target_file = dedicated.join("target.sqlite3");
    std::fs::File::create(&target_file).expect("symlink target file should be created");
    std::fs::set_permissions(&target_file, std::fs::Permissions::from_mode(0o600))
        .expect("symlink target file mode should be configured");
    let file_link = dedicated.join("file-link.sqlite3");
    symlink(&target_file, &file_link).expect("database symlink should be created");

    let file_result = SqliteStore::open(&file_link).await;
    let target_file_mode = std::fs::metadata(&target_file)
        .expect("symlink target file metadata should be readable")
        .permissions()
        .mode()
        & 0o777;
    assert!(matches!(file_result, Err(StoreError::InvalidPath { .. })));
    assert_eq!(target_file_mode, 0o600);
}

#[cfg(unix)]
#[tokio::test]
async fn open_reopens_existing_dedicated_database_without_mutating_permissions() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let dedicated = temp_dir.path().join("dedicated");
    std::fs::create_dir(&dedicated).expect("dedicated directory should be created");
    std::fs::set_permissions(&dedicated, std::fs::Permissions::from_mode(0o700))
        .expect("dedicated directory mode should be configured");
    let path = dedicated.join("journal.sqlite3");
    std::fs::File::create(&path).expect("dedicated database file should be created");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
        .expect("dedicated database file mode should be configured");

    let mut store = SqliteStore::open(&path)
        .await
        .expect("existing dedicated database should open");
    store
        .create_run(RunInput {
            run_id: RUN_ID,
            started_at_ns: EVENT_TIME_NS,
        })
        .await
        .expect("existing dedicated database should remain writable");
    drop(store);

    let mut reopened = SqliteStore::open(&path)
        .await
        .expect("existing dedicated database should reopen");
    let counts = reopened
        .journal_counts(RUN_ID)
        .await
        .expect("reopened dedicated database should remain readable");
    let parent_mode = std::fs::metadata(&dedicated)
        .expect("dedicated directory metadata should be readable")
        .permissions()
        .mode()
        & 0o777;
    let file_mode = std::fs::metadata(&path)
        .expect("dedicated database metadata should be readable")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(
        counts,
        trench_storage::sqlite::JournalCounts {
            events: 0,
            risk_decisions: 0
        }
    );
    assert_eq!((parent_mode, file_mode), (0o700, 0o600));
}
