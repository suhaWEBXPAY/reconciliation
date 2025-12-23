from flask import Flask, render_template, request, redirect, url_for, send_file, session
from functools import wraps
from werkzeug.utils import secure_filename
from pathlib import Path
import pandas as pd
import json
from datetime import datetime, timedelta

import db
from recon_engine import read_excel_any, prep_portal, prep_bank, reconcile_and_update_db, export_unreconciled
from ai_mapper import infer_column_mapping

APP_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = APP_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

def output_xlsx_path(user_id: int) -> Path:
  # One output file per user (prevents users overwriting each other's downloads)
  return APP_DIR / f"unreconciled_user_{int(user_id)}.xlsx"

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50MB


import os
app.secret_key = os.environ.get("FLASK_SECRET_KEY")

db.init_db()

# -------------------------------------------------
# ✅ CLEANUP: delete uploads older than N days
# -------------------------------------------------
def cleanup_old_uploads(days=3):
  cutoff = datetime.now() - timedelta(days=days)
  for f in UPLOAD_DIR.iterdir():
    if not f.is_file():
      continue
    try:
      mtime = datetime.fromtimestamp(f.stat().st_mtime)
      if mtime < cutoff:
        f.unlink()
    except Exception:
      pass

def get_stats():
  user_id = session.get("user_id")
  if not user_id:
    return {"open_portal": 0, "open_bank": 0, "last_run": None}
  conn = db.connect()
  open_portal = conn.execute("SELECT COUNT(*) AS c FROM open_portal WHERE user_id=?", (int(user_id),)).fetchone()["c"]
  open_bank = conn.execute("SELECT COUNT(*) AS c FROM open_bank WHERE user_id=?", (int(user_id),)).fetchone()["c"]
  conn.close()
  return {
    "open_portal": open_portal,
    "open_bank": open_bank,
    "last_run": db.get_meta("last_run", user_id=int(user_id)),
  }


def login_required(fn):
  @wraps(fn)
  def wrapper(*args, **kwargs):
    if "user_id" not in session:
      return redirect(url_for("login"))
    return fn(*args, **kwargs)
  return wrapper


@app.get("/signup")
def signup():
  msg = request.args.get("msg")
  return render_template("signup.html", message=msg)


@app.post("/signup")
def signup_post():
  email = (request.form.get("email") or "").strip().lower()
  password = request.form.get("password") or ""
  if not email or not password:
    return redirect(url_for("signup", msg="Please enter email and password."))
  try:
    user_id = db.create_user(email, password)
  except Exception:
    return redirect(url_for("signup", msg="Email already exists. Please login."))
  session["user_id"] = int(user_id)
  return redirect(url_for("home"))


@app.get("/login")
def login():
  msg = request.args.get("msg")
  return render_template("login.html", message=msg)


@app.post("/login")
def login_post():
  email = (request.form.get("email") or "").strip().lower()
  password = request.form.get("password") or ""
  user = db.authenticate(email, password)
  if not user:
    return redirect(url_for("login", msg="Invalid email or password."))
  session["user_id"] = int(user["id"])
  return redirect(url_for("home"))


@app.get("/logout")
def logout():
  session.clear()
  return redirect(url_for("login"))

@app.get("/")
@login_required
def home():
  msg = request.args.get("msg")
  return render_template("index.html", stats=get_stats(), message=msg)

@app.post("/reset")
@login_required
def reset():
  user_id = int(session["user_id"])
  db.reset_user(user_id)
  try:
    out_path = output_xlsx_path(user_id)
    if out_path.exists():
      out_path.unlink()
  except Exception:
    pass
  return redirect(url_for("home", msg="Database cleared. Upload files to start again."))

@app.post("/reconcile")
@login_required
def reconcile():
  # ✅ cleanup old uploads before processing
  cleanup_old_uploads(days=3)

  portal_files = request.files.getlist("portal_files")
  bank_files = request.files.getlist("bank_files")
  portal_prompt = (request.form.get("portal_prompt") or "").strip()
  bank_prompt = (request.form.get("bank_prompt") or "").strip()

  # ✅ IMPORTANT FIX:
  # getlist() can return FileStorage objects with empty filename.
  # Treat "has upload" only if any file has a real filename.
  has_portal = any(f and getattr(f, "filename", "") for f in portal_files)
  has_bank = any(f and getattr(f, "filename", "") for f in bank_files)

  # Allow running with only one side uploaded (portal-only OR bank-only)
  if (not has_portal) and (not has_bank):
    return redirect(url_for("home", msg="Please upload at least one Portal file or one Bank file."))

  mapping_cache = {}

  saved_portal_files = []
  portal_frames = []
  for f in portal_files:
    # ✅ IMPORTANT FIX: skip empty FileStorage entries
    if not f or not f.filename:
      continue

    fname = secure_filename(f.filename)
    save_path = UPLOAD_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_PORTAL_{fname}"
    f.save(save_path)
    saved_portal_files.append(save_path.name)

    df = read_excel_any(save_path)
    sig = ("portal", tuple(df.columns.tolist()), portal_prompt)

    if sig not in mapping_cache:
      sample = df.head(5).to_dict(orient="records")
      mapping_cache[sig] = infer_column_mapping(
        side="portal",
        columns=df.columns.tolist(),
        sample_rows=sample,
        user_prompt=portal_prompt,
      )

    portal_frames.append(
      prep_portal(df, source_file=save_path.name, mapping=mapping_cache[sig])
    )

  saved_bank_files = []
  bank_frames = []
  for f in bank_files:
    # ✅ IMPORTANT FIX: skip empty FileStorage entries
    if not f or not f.filename:
      continue

    fname = secure_filename(f.filename)
    save_path = UPLOAD_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_BANK_{fname}"
    f.save(save_path)
    saved_bank_files.append(save_path.name)

    df = read_excel_any(save_path)
    sig = ("bank", tuple(df.columns.tolist()), bank_prompt)

    if sig not in mapping_cache:
      sample = df.head(5).to_dict(orient="records")
      mapping_cache[sig] = infer_column_mapping(
        side="bank",
        columns=df.columns.tolist(),
        sample_rows=sample,
        user_prompt=bank_prompt,
      )

    bank_frames.append(
      prep_bank(df, source_file=save_path.name, mapping=mapping_cache[sig])
    )

  # Build empty dataframes when one side is missing
  portal_cols = ["match_key","mid_norm","tid_norm","card_raw","card_first4","card_last4","amount_norm","source_file","raw_cols","raw_json"]
  bank_cols   = ["match_key","mid_norm","tid_norm","card_masked","card_first4","card_last4","amount_norm","source_file","raw_cols","raw_json"]

  new_portal = pd.concat(portal_frames, ignore_index=True) if portal_frames else pd.DataFrame(columns=portal_cols)
  new_bank   = pd.concat(bank_frames, ignore_index=True) if bank_frames else pd.DataFrame(columns=bank_cols)

  user_id = int(session["user_id"])
  out_path = output_xlsx_path(user_id)

  conn = db.connect()
  try:
    # Create a run record (needed for undo)
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.execute(
      "INSERT INTO recon_runs(user_id, started_at, portal_files_json, bank_files_json) VALUES(?,?,?,?)",
      (int(user_id), started_at, json.dumps(saved_portal_files), json.dumps(saved_bank_files))
    )
    run_id = int(cur.lastrowid)

    stats = reconcile_and_update_db(conn, new_portal, new_bank, user_id=user_id, run_id=run_id)
    conn.execute(
      "UPDATE recon_runs SET finished_at=?, stats_json=? WHERE id=? AND user_id=?",
      (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), json.dumps(stats), int(run_id), int(user_id))
    )
    conn.commit()
    export_unreconciled(conn, str(out_path), user_id=user_id)
  finally:
    conn.close()

  db.set_meta("last_run", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user_id=user_id)

  msg = (
    f"Done. Matched this run: {stats['matched_count']}. "
    f"New portal rows: {stats['new_portal_rows']} (added {stats['inserted_portal']} to unreconciled). "
    f"New bank rows: {stats['new_bank_rows']} (added {stats['inserted_bank']} to unreconciled)."
  )
  return redirect(url_for("home", msg=msg))


def _delete_uploaded_files(file_names):
  for name in file_names or []:
    try:
      p = UPLOAD_DIR / name
      if p.exists() and p.is_file():
        p.unlink()
    except Exception:
      pass


def _undo_run(conn, user_id: int, run_id: int, *, restore_matched_rows: bool = False) -> dict:
  """Undo a single reconciliation run.

  Your requirement for "Undo" is to *firmly remove unreconciled rows that came
  from the last upload / today's uploads*.

  So by default this function:
  - Deletes any open_portal/open_bank rows created by the uploaded files in this run
    (using source_file, and run_id as a secondary filter).
  - Marks the run as undone.
  - Deletes the uploaded files from /uploads.

  If you ever need a "true rewind" (restore older rows that were matched+deleted),
  call with restore_matched_rows=True.
  """
  # Get file list to optionally delete from uploads folder
  rr = conn.execute(
    "SELECT portal_files_json, bank_files_json FROM recon_runs WHERE id=? AND user_id=? AND undone_at IS NULL",
    (int(run_id), int(user_id))
  ).fetchone()
  if not rr:
    return {"ok": False, "msg": "Nothing to undo."}

  portal_files = []
  bank_files = []
  try:
    portal_files = json.loads(rr["portal_files_json"] or "[]")
  except Exception:
    portal_files = []
  try:
    bank_files = json.loads(rr["bank_files_json"] or "[]")
  except Exception:
    bank_files = []

  # 1) Remove unreconciled rows that came from *this run's uploaded files*.
  #    We primarily filter by source_file because that is the most "firm" link to the upload.
  #    We also delete by run_id as a safety net.
  del_p = 0
  del_b = 0

  if portal_files:
    ph = ",".join(["?"] * len(portal_files))
    del_p += conn.execute(
      f"DELETE FROM open_portal WHERE user_id=? AND source_file IN ({ph})",
      [int(user_id), *portal_files]
    ).rowcount

  if bank_files:
    ph = ",".join(["?"] * len(bank_files))
    del_b += conn.execute(
      f"DELETE FROM open_bank WHERE user_id=? AND source_file IN ({ph})",
      [int(user_id), *bank_files]
    ).rowcount

  # Safety-net: anything that still carries this run_id
  del_p += conn.execute(
    "DELETE FROM open_portal WHERE user_id=? AND run_id=?",
    (int(user_id), int(run_id))
  ).rowcount
  del_b += conn.execute(
    "DELETE FROM open_bank WHERE user_id=? AND run_id=?",
    (int(user_id), int(run_id))
  ).rowcount

  # 2) Optional: restore rows that were previously open but got matched+deleted in this run.
  restored_p = 0
  restored_b = 0
  if restore_matched_rows:
    rp = conn.execute(
      "SELECT * FROM recon_deleted_portal WHERE run_id=? AND user_id=? ORDER BY orig_id",
      (int(run_id), int(user_id))
    ).fetchall()
    rb = conn.execute(
      "SELECT * FROM recon_deleted_bank WHERE run_id=? AND user_id=? ORDER BY orig_id",
      (int(run_id), int(user_id))
    ).fetchall()

    if rp:
      conn.executemany(
        """INSERT OR IGNORE INTO open_portal(
             id, user_id, run_id, match_key, mid, tid, card_raw, card_first4, card_last4,
             amount, source_file, raw_cols, raw_json, inserted_at
           ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
          (
            int(r["orig_id"]), int(user_id), None, r["match_key"], r["mid"], r["tid"], r["card_raw"],
            r["card_first4"], r["card_last4"], r["amount"], r["source_file"], r["raw_cols"], r["raw_json"], r["inserted_at"]
          )
          for r in rp
        ]
      )
      restored_p = len(rp)

    if rb:
      conn.executemany(
        """INSERT OR IGNORE INTO open_bank(
             id, user_id, run_id, match_key, merchantid, tid, card_masked, card_first4, card_last4,
             amount, source_file, raw_cols, raw_json, inserted_at
           ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
          (
            int(r["orig_id"]), int(user_id), None, r["match_key"], r["merchantid"], r["tid"], r["card_masked"],
            r["card_first4"], r["card_last4"], r["amount"], r["source_file"], r["raw_cols"], r["raw_json"], r["inserted_at"]
          )
          for r in rb
        ]
      )
      restored_b = len(rb)

  # 3) Mark as undone
  conn.execute(
    "UPDATE recon_runs SET undone_at=? WHERE id=? AND user_id=?",
    (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), int(run_id), int(user_id))
  )

  # Remove uploaded files for this run (optional but matches your requirement)
  _delete_uploaded_files(portal_files)
  _delete_uploaded_files(bank_files)

  return {
    "ok": True,
    "msg": (
      f"Undid run {run_id}. Deleted {del_p} portal + {del_b} bank unreconciled rows from the last uploaded file(s)."
      + (f" Restored {restored_p} portal + {restored_b} bank previously-matched rows." if restore_matched_rows else "")
    ),
  }


@app.post("/undo_last")
@login_required
def undo_last():
  user_id = int(session["user_id"])
  conn = db.connect()
  try:
    r = conn.execute(
      "SELECT id FROM recon_runs WHERE user_id=? AND undone_at IS NULL ORDER BY id DESC LIMIT 1",
      (int(user_id),)
    ).fetchone()
    if not r:
      # Fallback: if no run exists (older DB / edge case), delete the most recent batch of unreconciled rows.
      # We approximate "last upload" as rows with the latest inserted_at timestamp.
      last_ts = conn.execute(
        "SELECT MAX(inserted_at) AS ts FROM (SELECT inserted_at FROM open_portal WHERE user_id=? UNION ALL SELECT inserted_at FROM open_bank WHERE user_id=?)",
        (int(user_id), int(user_id))
      ).fetchone()["ts"]
      if not last_ts:
        return redirect(url_for("home", msg="Nothing to undo."))
      del_p = conn.execute(
        "DELETE FROM open_portal WHERE user_id=? AND inserted_at=?",
        (int(user_id), last_ts)
      ).rowcount
      del_b = conn.execute(
        "DELETE FROM open_bank   WHERE user_id=? AND inserted_at=?",
        (int(user_id), last_ts)
      ).rowcount
      conn.commit()
      res = {"msg": f"Deleted last-upload unreconciled rows: {del_p} portal + {del_b} bank."}
    else:
      res = _undo_run(conn, user_id=user_id, run_id=int(r["id"]))
      conn.commit()
  finally:
    conn.close()

  # Refresh output after undo
  out_path = output_xlsx_path(user_id)
  conn2 = db.connect()
  try:
    export_unreconciled(conn2, str(out_path), user_id=user_id)
  finally:
    conn2.close()
  return redirect(url_for("home", msg=res.get("msg") or "Undone."))


@app.post("/undo_today")
@login_required
def undo_today():
  user_id = int(session["user_id"])
  today = datetime.now().strftime("%Y-%m-%d")

  conn = db.connect()
  try:
    # Use SQLite's localtime so "today" matches the server's local clock.
    runs = conn.execute(
      """SELECT id FROM recon_runs
         WHERE user_id=? AND undone_at IS NULL
           AND date(started_at) = date('now','localtime')
         ORDER BY id DESC""",
      (int(user_id),)
    ).fetchall()
    if not runs:
      # Even if no runs are logged (older DBs / edge cases), still do a firm delete of today's unreconciled.
      del_p = conn.execute(
        "DELETE FROM open_portal WHERE user_id=? AND date(inserted_at)=date('now','localtime')",
        (int(user_id),)
      ).rowcount
      del_b = conn.execute(
        "DELETE FROM open_bank   WHERE user_id=? AND date(inserted_at)=date('now','localtime')",
        (int(user_id),)
      ).rowcount
      conn.commit()
      return redirect(url_for("home", msg=f"Deleted today's unreconciled rows: {del_p} portal + {del_b} bank."))

    count = 0
    for r in runs:
      _undo_run(conn, user_id=user_id, run_id=int(r["id"]))
      count += 1

    # Extra safety-net ("FIRM" behaviour): delete any unreconciled rows inserted today
    # even if a row somehow missed run_id/source_file tagging.
    del_p = conn.execute(
      "DELETE FROM open_portal WHERE user_id=? AND date(inserted_at)=date('now','localtime')",
      (int(user_id),)
    ).rowcount
    del_b = conn.execute(
      "DELETE FROM open_bank   WHERE user_id=? AND date(inserted_at)=date('now','localtime')",
      (int(user_id),)
    ).rowcount
    conn.commit()
  finally:
    conn.close()

  out_path = output_xlsx_path(user_id)
  conn2 = db.connect()
  try:
    export_unreconciled(conn2, str(out_path), user_id=user_id)
  finally:
    conn2.close()

  return redirect(url_for(
    "home",
    msg=f"Undid {count} run(s) and deleted all today's unreconciled rows ({today})."
  ))

@app.get("/download")
@login_required
def download():
  user_id = int(session["user_id"])
  out_path = output_xlsx_path(user_id)
  conn = db.connect()
  try:
    export_unreconciled(conn, str(out_path), user_id=user_id)
  finally:
    conn.close()

  if not out_path.exists():
    return redirect(url_for("home", msg="No output yet. Please run reconciliation first."))
  return send_file(out_path, as_attachment=True, download_name="unreconciled.xlsx")

if __name__ == "__main__":
  app.run(host="0.0.0.0", port=5000, debug=True)
