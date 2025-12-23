import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional

from werkzeug.security import generate_password_hash, check_password_hash

# We keep a few normalized columns for matching (mid/tid/card/amount + match_key),
# but we also store the *entire original uploaded row* as JSON so exports can
# recreate the exact uploaded columns without hardcoding.

DB_PATH = Path(__file__).resolve().parent / "recon.db"

SCHEMA_SQL = '''
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS open_portal (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  run_id INTEGER,
  match_key TEXT NOT NULL,
  mid TEXT,
  tid TEXT,
  card_raw TEXT,
  card_first4 TEXT,
  card_last4 TEXT,
  amount REAL,
  source_file TEXT,
  raw_cols TEXT,         -- JSON list of original column names (order preserved)
  raw_json TEXT,         -- JSON object of the full original row
  inserted_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_open_portal_user_key ON open_portal(user_id, match_key);

CREATE TABLE IF NOT EXISTS open_bank (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  run_id INTEGER,
  match_key TEXT NOT NULL,
  merchantid TEXT,
  tid TEXT,
  card_masked TEXT,
  card_first4 TEXT,
  card_last4 TEXT,
  amount REAL,
  source_file TEXT,
  raw_cols TEXT,         -- JSON list of original column names (order preserved)
  raw_json TEXT,         -- JSON object of the full original row
  inserted_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_open_bank_user_key ON open_bank(user_id, match_key);

-- Reconciliation run log (for undo)
CREATE TABLE IF NOT EXISTS recon_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  portal_files_json TEXT,
  bank_files_json TEXT,
  stats_json TEXT,
  undone_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_recon_runs_user_started ON recon_runs(user_id, started_at);

-- Deleted rows captured during a run so we can restore on undo
CREATE TABLE IF NOT EXISTS recon_deleted_portal (
  run_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  orig_id INTEGER NOT NULL,
  match_key TEXT NOT NULL,
  mid TEXT,
  tid TEXT,
  card_raw TEXT,
  card_first4 TEXT,
  card_last4 TEXT,
  amount REAL,
  source_file TEXT,
  raw_cols TEXT,
  raw_json TEXT,
  inserted_at TEXT,
  PRIMARY KEY(run_id, orig_id)
);

CREATE TABLE IF NOT EXISTS recon_deleted_bank (
  run_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  orig_id INTEGER NOT NULL,
  match_key TEXT NOT NULL,
  merchantid TEXT,
  tid TEXT,
  card_masked TEXT,
  card_first4 TEXT,
  card_last4 TEXT,
  amount REAL,
  source_file TEXT,
  raw_cols TEXT,
  raw_json TEXT,
  inserted_at TEXT,
  PRIMARY KEY(run_id, orig_id)
);

-- Per-user metadata (e.g., last_run)
CREATE TABLE IF NOT EXISTS meta_user (
  user_id INTEGER NOT NULL,
  k TEXT NOT NULL,
  v TEXT,
  PRIMARY KEY(user_id, k)
);

-- Legacy meta kept for backwards compatibility
CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT
);
'''


def connect():
  conn = sqlite3.connect(DB_PATH)
  conn.row_factory = sqlite3.Row
  return conn


def init_db():
  conn = connect()
  conn.executescript(SCHEMA_SQL)

  # Lightweight migrations for older DBs
  def _ensure_col(table: str, col: str, coltype: str):
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if col not in cols:
      conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")

  # Ensure raw_cols/raw_json exist
  _ensure_col("open_portal", "raw_cols", "TEXT")
  _ensure_col("open_portal", "raw_json", "TEXT")
  _ensure_col("open_bank", "raw_cols", "TEXT")
  _ensure_col("open_bank", "raw_json", "TEXT")

  # Ensure run_id exists (needed for undo)
  _ensure_col("open_portal", "run_id", "INTEGER")
  _ensure_col("open_bank", "run_id", "INTEGER")

  # Ensure user_id exists on both open tables, defaulting legacy rows to user_id=1
  _ensure_col("open_portal", "user_id", "INTEGER")
  _ensure_col("open_bank", "user_id", "INTEGER")
  conn.execute("UPDATE open_portal SET user_id = COALESCE(user_id, 1) WHERE user_id IS NULL")
  conn.execute("UPDATE open_bank   SET user_id = COALESCE(user_id, 1) WHERE user_id IS NULL")

  # Create a legacy user (id=1) if DB already had data before auth
  legacy = conn.execute("SELECT id FROM users WHERE id=1").fetchone()
  if not legacy:
    conn.execute(
      "INSERT OR IGNORE INTO users(id, email, password_hash, created_at) VALUES(?,?,?,?)",
      (1, "legacy@local", generate_password_hash("change-me"), datetime.utcnow().isoformat())
    )

  # Migrate legacy meta.last_run -> meta_user(1,'last_run')
  lr = conn.execute("SELECT v FROM meta WHERE k='last_run'").fetchone()
  if lr and lr[0]:
    conn.execute(
      "INSERT OR IGNORE INTO meta_user(user_id,k,v) VALUES(?,?,?)",
      (1, "last_run", lr[0])
    )

  conn.commit()
  conn.close()


# -------------------- AUTH --------------------

def create_user(email: str, password: str) -> int:
  conn = connect()
  cur = conn.execute(
    "INSERT INTO users(email, password_hash, created_at) VALUES(?,?,?)",
    (email.strip().lower(), generate_password_hash(password), datetime.utcnow().isoformat())
  )
  conn.commit()
  user_id = cur.lastrowid
  conn.close()
  return int(user_id)


def authenticate(email: str, password: str):
  conn = connect()
  user = conn.execute(
    "SELECT * FROM users WHERE email = ?",
    (email.strip().lower(),)
  ).fetchone()
  conn.close()
  if user and check_password_hash(user["password_hash"], password):
    return user
  return None


def get_user(user_id: int):
  conn = connect()
  user = conn.execute("SELECT * FROM users WHERE id=?", (int(user_id),)).fetchone()
  conn.close()
  return user


# -------------------- META (per-user) --------------------

def set_meta(key: str, value: str, user_id: Optional[int] = None):
  conn = connect()
  if user_id is None:
    conn.execute(
      "INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
      (key, value)
    )
  else:
    conn.execute(
      "INSERT INTO meta_user(user_id,k,v) VALUES(?,?,?) ON CONFLICT(user_id,k) DO UPDATE SET v=excluded.v",
      (int(user_id), key, value)
    )
  conn.commit()
  conn.close()


def get_meta(key: str, user_id: Optional[int] = None):
  conn = connect()
  if user_id is None:
    row = conn.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
  else:
    row = conn.execute("SELECT v FROM meta_user WHERE user_id=? AND k=?", (int(user_id), key)).fetchone()
  conn.close()
  return row["v"] if row else None


# -------------------- RESET --------------------

def reset_user(user_id: int):
  conn = connect()
  conn.execute("DELETE FROM open_portal WHERE user_id=?", (int(user_id),))
  conn.execute("DELETE FROM open_bank WHERE user_id=?", (int(user_id),))
  conn.execute("DELETE FROM meta_user WHERE user_id=?", (int(user_id),))
  conn.commit()
  conn.close()


def reset_all():
  """Legacy: clears everything (admin/dev use only)."""
  conn = connect()
  conn.execute("DELETE FROM open_portal")
  conn.execute("DELETE FROM open_bank")
  conn.execute("DELETE FROM meta")
  conn.execute("DELETE FROM meta_user")
  conn.commit()
  conn.close()
