#!/usr/bin/env python3
"""Small terminal tracker for Slurm jobs with local SQLite history."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import platform
import shlex
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse


DELIM = "\x1f"

RUNNING_STATES = {
    "RUNNING",
    "COMPLETING",
    "CONFIGURING",
    "RESIZING",
    "SIGNALING",
    "STAGE_OUT",
    "SUSPENDED",
    "STOPPED",
}
QUEUED_STATES = {
    "PENDING",
    "REQUEUED",
    "REQUEUE_FED",
    "REQUEUE_HOLD",
    "SPECIAL_EXIT",
}
DONE_STATES = {"COMPLETED"}
ERROR_STATES = {
    "BOOT_FAIL",
    "CANCELLED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "TIMEOUT",
}

LIGHT_BY_CATEGORY = {
    "running": "white",
    "queued": "yellow",
    "done": "green",
    "error": "red",
    "unknown": "gray",
}

ANSI_BY_LIGHT = {
    "white": "\033[97m",
    "yellow": "\033[33m",
    "green": "\033[32m",
    "red": "\033[31m",
    "gray": "\033[90m",
}


@dataclass
class CommandRunner:
    backend: str
    ssh_host: str = ""
    ssh_user: str = ""
    ssh_port: Optional[int] = None
    ssh_key: str = ""
    ssh_options: Tuple[str, ...] = ()
    ssh_batch: bool = True
    timeout: int = 45

    @property
    def label(self) -> str:
        if self.backend == "ssh":
            target = self.ssh_target
            return f"ssh:{target}"
        return "local"

    @property
    def ssh_target(self) -> str:
        if self.ssh_user:
            return f"{self.ssh_user}@{self.ssh_host}"
        return self.ssh_host


@dataclass
class Job:
    job_id: str
    state: str
    job_name: str = ""
    user: str = ""
    partition: str = ""
    elapsed: str = ""
    time_limit: str = ""
    nodes: str = ""
    reason: str = ""
    exit_code: str = ""
    start_time: str = ""
    end_time: str = ""
    first_seen: str = ""
    last_seen: str = ""
    source: str = ""

    @property
    def category(self) -> str:
        return category_for_state(self.state)

    @property
    def light(self) -> str:
        return LIGHT_BY_CATEGORY[self.category]


def clean_state(state: str) -> str:
    state = (state or "").strip().upper()
    if not state:
        return "UNKNOWN"
    return state.split()[0]


def clean_optional(value: str) -> str:
    value = (value or "").strip()
    if value.lower() in {"none", "null", "n/a", "unknown"}:
        return ""
    return value


def env_int(name: str) -> Optional[int]:
    value = clean_optional(os.environ.get(name, ""))
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def split_ssh_host(value: str) -> Tuple[str, str]:
    value = clean_optional(value)
    if "@" not in value:
        return "", value
    user, host = value.rsplit("@", 1)
    if user and host:
        return user, host
    return "", value


def category_for_state(state: str) -> str:
    state = clean_state(state)
    if state in RUNNING_STATES:
        return "running"
    if state in QUEUED_STATES:
        return "queued"
    if state in DONE_STATES:
        return "done"
    if state in ERROR_STATES:
        return "error"
    return "unknown"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def parse_time_value(value: str) -> float:
    value = clean_optional(value)
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def make_remote_command(args: Sequence[str]) -> str:
    return " ".join(shlex.quote(str(arg)) for arg in args)


def build_ssh_args(runner: CommandRunner, remote_args: Sequence[str]) -> List[str]:
    ssh_args = ["ssh"]
    if runner.ssh_batch:
        ssh_args.extend(["-o", "BatchMode=yes"])
    ssh_args.extend(["-o", "ConnectTimeout=10"])
    if runner.ssh_port is not None:
        ssh_args.extend(["-p", str(runner.ssh_port)])
    if runner.ssh_key:
        ssh_args.extend(["-i", runner.ssh_key])
    for option in runner.ssh_options:
        ssh_args.extend(["-o", option])
    ssh_args.append(runner.ssh_target)
    ssh_args.append(make_remote_command(remote_args))
    return ssh_args


def run_command(args: Sequence[str], runner: CommandRunner) -> Tuple[str, Optional[str]]:
    command = list(args)
    if runner.backend == "ssh":
        command = build_ssh_args(runner, command)

    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=runner.timeout,
        )
    except FileNotFoundError:
        if runner.backend == "ssh":
            return "", "command not found: ssh"
        return "", f"command not found: {args[0]}"
    except subprocess.TimeoutExpired:
        return "", f"{runner.label} timed out running {args[0]}"

    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip()
        return "", f"{runner.label} {args[0]} failed: {message}"
    return result.stdout, None


def split_row(line: str, expected_fields: int) -> Optional[List[str]]:
    fields = line.rstrip("\n").split(DELIM)
    if len(fields) != expected_fields:
        return None
    return [field.strip() for field in fields]


def load_squeue_jobs(
    runner: CommandRunner,
    user: str,
    job_ids: Sequence[str],
) -> Tuple[Dict[str, Job], List[str]]:
    fmt = DELIM.join(["%i", "%T", "%j", "%P", "%M", "%l", "%D", "%R"])
    args = ["squeue", "-h", "-o", fmt]
    if user:
        args.extend(["--user", user])
    if job_ids:
        args.extend(["--jobs", ",".join(job_ids)])

    output, error = run_command(args, runner)
    warnings = [error] if error else []
    jobs: Dict[str, Job] = {}

    for line in output.splitlines():
        if not line.strip():
            continue
        fields = split_row(line, 8)
        if fields is None:
            warnings.append(f"could not parse squeue row: {line!r}")
            continue
        job_id, state, job_name, partition, elapsed, time_limit, nodes, reason = fields
        jobs[job_id] = Job(
            job_id=job_id,
            state=clean_state(state),
            job_name=job_name,
            user=user,
            partition=partition,
            elapsed=elapsed,
            time_limit=time_limit,
            nodes=nodes,
            reason=clean_optional(reason),
            source="squeue",
        )

    return jobs, warnings


def load_sacct_jobs(
    runner: CommandRunner,
    user: str,
    job_ids: Sequence[str],
    since: datetime,
) -> Tuple[Dict[str, Job], List[str]]:
    fields = [
        "JobIDRaw",
        "JobName",
        "User",
        "Partition",
        "State",
        "Elapsed",
        "Timelimit",
        "NNodes",
        "ExitCode",
        "Reason",
        "Start",
        "End",
    ]
    args = [
        "sacct",
        "-n",
        "-P",
        f"--delimiter={DELIM}",
        "-X",
        "--format=" + ",".join(fields),
        "--starttime",
        since.strftime("%Y-%m-%dT%H:%M:%S"),
    ]
    if user:
        args.extend(["--user", user])
    if job_ids:
        args.extend(["--jobs", ",".join(job_ids)])

    output, error = run_command(args, runner)
    warnings = [error] if error else []
    jobs: Dict[str, Job] = {}

    for line in output.splitlines():
        if not line.strip():
            continue
        row = split_row(line, len(fields))
        if row is None:
            warnings.append(f"could not parse sacct row: {line!r}")
            continue
        (
            job_id,
            job_name,
            row_user,
            partition,
            state,
            elapsed,
            time_limit,
            nodes,
            exit_code,
            reason,
            start_time,
            end_time,
        ) = row
        jobs[job_id] = Job(
            job_id=job_id,
            state=clean_state(state),
            job_name=job_name,
            user=row_user or user,
            partition=partition,
            elapsed=elapsed,
            time_limit=time_limit,
            nodes=nodes,
            reason=clean_optional(reason),
            exit_code=exit_code,
            start_time=start_time,
            end_time=end_time,
            source="sacct",
        )

    return jobs, warnings


def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            job_name TEXT,
            user TEXT,
            partition TEXT,
            state TEXT NOT NULL,
            category TEXT NOT NULL,
            light TEXT NOT NULL,
            elapsed TEXT,
            time_limit TEXT,
            nodes TEXT,
            reason TEXT,
            exit_code TEXT,
            start_time TEXT,
            end_time TEXT,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            source TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            seen_at TEXT NOT NULL,
            old_state TEXT,
            new_state TEXT NOT NULL,
            old_light TEXT,
            new_light TEXT NOT NULL,
            source TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_job_seen ON events(job_id, seen_at)"
    )
    conn.commit()
    return conn


def record_jobs(conn: sqlite3.Connection, jobs: Iterable[Job]) -> None:
    seen_at = now_iso()
    for job in jobs:
        previous = conn.execute(
            "SELECT state, light, first_seen FROM jobs WHERE job_id = ?",
            (job.job_id,),
        ).fetchone()
        if previous is None:
            first_seen = seen_at
            conn.execute(
                """
                INSERT INTO jobs (
                    job_id, job_name, user, partition, state, category, light,
                    elapsed, time_limit, nodes, reason, exit_code, start_time,
                    end_time, first_seen, last_seen, source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.job_id,
                    job.job_name,
                    job.user,
                    job.partition,
                    job.state,
                    job.category,
                    job.light,
                    job.elapsed,
                    job.time_limit,
                    job.nodes,
                    job.reason,
                    job.exit_code,
                    job.start_time,
                    job.end_time,
                    first_seen,
                    seen_at,
                    job.source,
                ),
            )
            conn.execute(
                """
                INSERT INTO events (
                    job_id, seen_at, old_state, new_state, old_light,
                    new_light, source
                )
                VALUES (?, ?, NULL, ?, NULL, ?, ?)
                """,
                (job.job_id, seen_at, job.state, job.light, job.source),
            )
            continue

        old_state, old_light, first_seen = previous
        if old_state != job.state or old_light != job.light:
            conn.execute(
                """
                INSERT INTO events (
                    job_id, seen_at, old_state, new_state, old_light,
                    new_light, source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.job_id,
                    seen_at,
                    old_state,
                    job.state,
                    old_light,
                    job.light,
                    job.source,
                ),
            )

        conn.execute(
            """
            UPDATE jobs
            SET job_name = ?, user = ?, partition = ?, state = ?, category = ?,
                light = ?, elapsed = ?, time_limit = ?, nodes = ?, reason = ?,
                exit_code = ?, start_time = ?, end_time = ?, first_seen = ?,
                last_seen = ?, source = ?
            WHERE job_id = ?
            """,
            (
                job.job_name,
                job.user,
                job.partition,
                job.state,
                job.category,
                job.light,
                job.elapsed,
                job.time_limit,
                job.nodes,
                job.reason,
                job.exit_code,
                job.start_time,
                job.end_time,
                first_seen,
                seen_at,
                job.source,
                job.job_id,
            ),
        )
    conn.commit()


def load_recorded_jobs(
    conn: sqlite3.Connection,
    limit: int,
    exclude_job_ids: Iterable[str] = (),
) -> List[Job]:
    excluded = list(exclude_job_ids)
    where = ""
    params: List[object] = []
    if excluded:
        placeholders = ",".join("?" for _ in excluded)
        where = f"WHERE job_id NOT IN ({placeholders})"
        params.extend(excluded)
    params.append(limit)

    rows = conn.execute(
        f"""
        SELECT job_id, state, job_name, user, partition, elapsed, time_limit,
               nodes, reason, exit_code, start_time, end_time, first_seen,
               last_seen, source
        FROM jobs
        {where}
        ORDER BY
            COALESCE(NULLIF(NULLIF(end_time, ''), 'Unknown'), last_seen, start_time) DESC,
            CAST(job_id AS INTEGER) DESC,
            last_seen DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return [
        Job(
            job_id=row[0],
            state=row[1],
            job_name=row[2] or "",
            user=row[3] or "",
            partition=row[4] or "",
            elapsed=row[5] or "",
            time_limit=row[6] or "",
            nodes=row[7] or "",
            reason=row[8] or "",
            exit_code=row[9] or "",
            start_time=row[10] or "",
            end_time=row[11] or "",
            first_seen=row[12] or "",
            last_seen=row[13] or "",
            source=row[14] or "db",
        )
        for row in rows
    ]


def choose_display_jobs(
    conn: sqlite3.Connection,
    active: Dict[str, Job],
    limit: int,
) -> List[Job]:
    """Show all live squeue jobs first; fill the rest with local history."""
    history_limit = max(limit - len(active), 0)
    jobs = list(active.values())
    if history_limit:
        jobs.extend(load_recorded_jobs(conn, history_limit, active.keys()))
    jobs.sort(key=sort_key)
    return jobs


def combine_jobs(active: Dict[str, Job], accounted: Dict[str, Job]) -> Dict[str, Job]:
    combined = dict(accounted)
    combined.update(active)
    return combined


def poll_jobs(
    conn: sqlite3.Connection,
    runner: CommandRunner,
    user: str,
    job_ids: Sequence[str],
    since_days: int,
    no_sacct: bool,
    limit: int,
) -> Tuple[List[Job], List[str]]:
    since = datetime.now() - timedelta(days=since_days)
    active, warnings = load_squeue_jobs(runner, user, job_ids)
    accounted: Dict[str, Job] = {}
    if not no_sacct:
        accounted, sacct_warnings = load_sacct_jobs(runner, user, job_ids, since)
        warnings.extend(sacct_warnings)

    current = combine_jobs(active, accounted)
    record_jobs(conn, current.values())
    return choose_display_jobs(conn, active, limit), warnings


def count_jobs(jobs: Iterable[Job]) -> Dict[str, int]:
    counts = {"running": 0, "queued": 0, "done": 0, "error": 0, "unknown": 0}
    for job in jobs:
        counts[job.category] = counts.get(job.category, 0) + 1
    return counts


def job_number(job_id: str) -> int:
    base = job_id.split("_", 1)[0].split(".", 1)[0]
    try:
        return int(base)
    except ValueError:
        return -1


def sort_key(job: Job) -> Tuple[int, float, int, str]:
    category_rank = {
        "queued": 0,
        "running": 1,
        "done": 2,
        "error": 2,
        "unknown": 3,
    }.get(job.category, 9)
    if job.category in {"queued", "running"}:
        return category_rank, 0.0, -job_number(job.job_id), job.job_id
    ended_at = (
        parse_time_value(job.end_time)
        or parse_time_value(job.last_seen)
        or parse_time_value(job.start_time)
    )
    return category_rank, -ended_at, -job_number(job.job_id), job.job_id


def truncate(value: str, width: int) -> str:
    value = value or ""
    if width <= 1:
        return value[:width]
    if len(value) <= width:
        return value
    return value[: width - 1] + "."


def light_symbol(light: str, use_color: bool) -> str:
    plain = {
        "white": "W",
        "yellow": "Y",
        "green": "G",
        "red": "R",
        "gray": "?",
    }.get(light, "?")
    if not use_color:
        return plain
    return f"{ANSI_BY_LIGHT.get(light, '')}●\033[0m"


def info_for_job(job: Job) -> str:
    reason = clean_optional(job.reason)
    info = reason
    if job.exit_code and job.exit_code != "0:0":
        info = f"{reason} exit={job.exit_code}" if reason else f"exit={job.exit_code}"
    if not info and job.category == "unknown":
        info = job.source
    return info


def job_to_dict(job: Job) -> Dict[str, str]:
    return {
        "job_id": job.job_id,
        "state": job.state,
        "category": job.category,
        "light": job.light,
        "job_name": job.job_name,
        "user": job.user,
        "partition": job.partition,
        "elapsed": job.elapsed,
        "time_limit": job.time_limit,
        "nodes": job.nodes,
        "reason": clean_optional(job.reason),
        "exit_code": job.exit_code,
        "start_time": job.start_time,
        "end_time": job.end_time,
        "first_seen": job.first_seen,
        "last_seen": job.last_seen,
        "source": job.source,
        "info": info_for_job(job),
    }


def terminal_width() -> int:
    return shutil.get_terminal_size((120, 30)).columns


def render(
    jobs: Sequence[Job],
    warnings: Sequence[str],
    db_path: str,
    backend_label: str,
    interval: int,
    use_color: bool,
    clear: bool,
) -> None:
    if clear and sys.stdout.isatty():
        print("\033[2J\033[H", end="")

    width = terminal_width()
    counts = count_jobs(jobs)

    print(
        f"Slurm Tracker  {now_iso()}  interval={interval}s  "
        f"backend={backend_label}  db={db_path}"
    )
    print(
        "Lights: "
        f"{light_symbol('white', use_color)} running  "
        f"{light_symbol('green', use_color)} completed  "
        f"{light_symbol('yellow', use_color)} queued  "
        f"{light_symbol('red', use_color)} error"
    )
    print(
        "Counts: "
        f"running={counts['running']}  queued={counts['queued']}  "
        f"completed={counts['done']}  error={counts['error']}  "
        f"unknown={counts['unknown']}"
    )
    if warnings:
        print("Warnings:")
        for warning in warnings[-3:]:
            print(f"  {truncate(warning, max(40, width - 4))}")
    print()

    if not jobs:
        print("No jobs found yet. Waiting for squeue/sacct data...")
        return

    fixed_width = 2 + 1 + 18 + 1 + 13 + 1 + 22 + 1 + 12 + 1 + 10 + 1 + 10 + 1 + 5 + 1
    info_width = max(18, width - fixed_width)
    print(
        f"{'L':<2} {'JOBID':<18} {'STATE':<13} {'NAME':<22} {'PARTITION':<12} "
        f"{'ELAPSED':<10} {'LIMIT':<10} {'NODES':<5} INFO"
    )
    print("-" * min(width, 120))
    for job in jobs:
        info = info_for_job(job)
        print(
            f"{light_symbol(job.light, use_color):<2} "
            f"{truncate(job.job_id, 18):<18} "
            f"{truncate(job.state, 13):<13} "
            f"{truncate(job.job_name, 22):<22} "
            f"{truncate(job.partition, 12):<12} "
            f"{truncate(job.elapsed, 10):<10} "
            f"{truncate(job.time_limit, 10):<10} "
            f"{truncate(job.nodes, 5):<5} "
            f"{truncate(info, info_width)}"
        )


WEB_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Slurm Tracker</title>
<style>
:root {
  color-scheme: light;
  --bg: #f6f7f8;
  --panel: #ffffff;
  --ink: #1f2933;
  --muted: #6b7280;
  --line: #d8dde3;
  --white-light: #ffffff;
  --yellow-light: #f2c94c;
  --green-light: #2fbf71;
  --red-light: #d94b4b;
  --gray-light: #9aa4b2;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  min-height: 100vh;
  background: var(--bg);
  color: var(--ink);
  font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  min-height: 64px;
  padding: 0 24px;
  border-bottom: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.88);
}
h1 {
  margin: 0;
  font-size: 18px;
  font-weight: 650;
  letter-spacing: 0;
}
.summary {
  display: flex;
  align-items: center;
  gap: 12px;
  color: var(--muted);
  font-size: 13px;
  white-space: nowrap;
}
.count {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}
.tiny-light {
  width: 9px;
  height: 9px;
  border-radius: 50%;
  display: inline-block;
  box-shadow: 0 0 0 1px rgba(31, 41, 51, 0.16);
}
.wrap {
  width: min(980px, calc(100vw - 32px));
  margin: 0 auto;
  padding: 28px 0 44px;
}
.light-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, 34px);
  grid-auto-rows: 34px;
  gap: 14px;
  align-items: center;
}
.job-light {
  width: 34px;
  height: 34px;
  border-radius: 50%;
  border: 1px solid rgba(31, 41, 51, 0.12);
  padding: 0;
  cursor: pointer;
  background: var(--gray-light);
  box-shadow:
    inset 0 2px 5px rgba(255, 255, 255, 0.55),
    inset 0 -4px 8px rgba(31, 41, 51, 0.18),
    0 3px 10px rgba(31, 41, 51, 0.16);
}
.job-light:hover,
.job-light:focus-visible {
  outline: 3px solid rgba(31, 41, 51, 0.18);
  outline-offset: 3px;
}
.job-light[data-light="white"],
.tiny-light[data-light="white"] {
  background: var(--white-light);
  border-color: #9aa4b2;
}
.job-light[data-light="yellow"],
.tiny-light[data-light="yellow"] { background: var(--yellow-light); }
.job-light[data-light="green"],
.tiny-light[data-light="green"] { background: var(--green-light); }
.job-light[data-light="red"],
.tiny-light[data-light="red"] { background: var(--red-light); }
.job-light[data-light="gray"],
.tiny-light[data-light="gray"] { background: var(--gray-light); }
.status-line {
  min-height: 20px;
  margin-bottom: 22px;
  color: var(--muted);
  font-size: 13px;
}
.backdrop {
  position: fixed;
  inset: 0;
  background: rgba(31, 41, 51, 0.28);
}
.detail {
  position: fixed;
  top: 0;
  right: 0;
  width: min(430px, calc(100vw - 28px));
  height: 100vh;
  overflow: auto;
  border-left: 1px solid var(--line);
  background: var(--panel);
  box-shadow: -12px 0 30px rgba(31, 41, 51, 0.16);
}
.detail-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  padding: 22px 22px 16px;
  border-bottom: 1px solid var(--line);
}
.detail-title {
  margin: 0;
  font-size: 17px;
  font-weight: 650;
  letter-spacing: 0;
}
.detail-subtitle {
  margin-top: 4px;
  color: var(--muted);
  font-size: 13px;
  overflow-wrap: anywhere;
}
.close {
  width: 32px;
  height: 32px;
  border-radius: 50%;
  border: 1px solid var(--line);
  background: #ffffff;
  color: var(--ink);
  font-size: 20px;
  line-height: 1;
  cursor: pointer;
}
.details {
  display: grid;
  grid-template-columns: 120px 1fr;
  gap: 0;
  margin: 0;
  padding: 10px 22px 24px;
}
.details dt,
.details dd {
  margin: 0;
  padding: 11px 0;
  border-bottom: 1px solid #eef1f4;
  font-size: 13px;
}
.details dt {
  color: var(--muted);
}
.details dd {
  overflow-wrap: anywhere;
}
[hidden] { display: none !important; }
@media (max-width: 620px) {
  .topbar {
    align-items: flex-start;
    flex-direction: column;
    padding: 14px 16px;
  }
  .summary {
    flex-wrap: wrap;
    white-space: normal;
  }
  .wrap {
    width: calc(100vw - 28px);
    padding-top: 22px;
  }
  .light-grid {
    grid-template-columns: repeat(auto-fill, 32px);
    grid-auto-rows: 32px;
    gap: 12px;
  }
  .job-light {
    width: 32px;
    height: 32px;
  }
}
</style>
</head>
<body>
<header class="topbar">
  <h1>Slurm Tracker</h1>
  <div class="summary" id="summary"></div>
</header>
<main class="wrap">
  <div class="status-line" id="status"></div>
  <section class="light-grid" id="lights" aria-label="Slurm jobs"></section>
</main>
<div class="backdrop" id="backdrop" hidden></div>
<aside class="detail" id="detail" hidden>
  <div class="detail-head">
    <div>
      <h2 class="detail-title" id="detail-title"></h2>
      <div class="detail-subtitle" id="detail-subtitle"></div>
    </div>
    <button class="close" id="close" type="button" aria-label="Close">&times;</button>
  </div>
  <dl class="details" id="details"></dl>
</aside>
<script>
const lights = document.getElementById("lights");
const summary = document.getElementById("summary");
const statusLine = document.getElementById("status");
const detail = document.getElementById("detail");
const backdrop = document.getElementById("backdrop");
const detailTitle = document.getElementById("detail-title");
const detailSubtitle = document.getElementById("detail-subtitle");
const details = document.getElementById("details");
const closeButton = document.getElementById("close");
let jobsById = new Map();
let selectedJobId = null;
let refreshTimer = null;

function text(value) {
  return value === null || value === undefined || value === "" ? "-" : String(value);
}

function escapeHtml(value) {
  return text(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function renderSummary(data) {
  const counts = data.counts || {};
  const items = [
    ["yellow", counts.queued || 0],
    ["white", counts.running || 0],
    ["green", counts.done || 0],
    ["red", counts.error || 0],
    ["gray", counts.unknown || 0],
  ];
  summary.innerHTML = items.map(([light, count]) =>
    `<span class="count"><span class="tiny-light" data-light="${light}"></span>${count}</span>`
  ).join("") + `<span>${escapeHtml(data.backend)}</span><span>${escapeHtml(data.updated_at)}</span>`;
}

function openDetail(job) {
  selectedJobId = job.job_id;
  detailTitle.textContent = `${job.job_id}  ${job.state}`;
  detailSubtitle.textContent = job.job_name || job.partition || "";
  const fields = [
    ["Light", job.light],
    ["State", job.state],
    ["Name", job.job_name],
    ["Partition", job.partition],
    ["Elapsed", job.elapsed],
    ["Limit", job.time_limit],
    ["Nodes", job.nodes],
    ["Info", job.info],
    ["Exit", job.exit_code],
    ["Start", job.start_time],
    ["End", job.end_time],
    ["First Seen", job.first_seen],
    ["Last Seen", job.last_seen],
    ["Source", job.source],
  ];
  details.innerHTML = fields.map(([key, value]) =>
    `<dt>${escapeHtml(key)}</dt><dd>${escapeHtml(value)}</dd>`
  ).join("");
  detail.hidden = false;
  backdrop.hidden = false;
}

function closeDetail() {
  selectedJobId = null;
  detail.hidden = true;
  backdrop.hidden = true;
}

function renderLights(data) {
  jobsById = new Map(data.jobs.map(job => [job.job_id, job]));
  lights.replaceChildren();
  data.jobs.forEach(job => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "job-light";
    button.dataset.light = job.light;
    button.title = `${job.job_id} ${job.state} ${job.job_name || ""}`.trim();
    button.setAttribute("aria-label", button.title);
    button.addEventListener("click", () => openDetail(job));
    lights.appendChild(button);
  });
  if (selectedJobId && jobsById.has(selectedJobId)) {
    openDetail(jobsById.get(selectedJobId));
  }
}

async function refresh() {
  try {
    const response = await fetch("/api/jobs", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    renderSummary(data);
    renderLights(data);
    statusLine.textContent = data.warnings && data.warnings.length
      ? data.warnings[data.warnings.length - 1]
      : "";
    if (!refreshTimer) {
      refreshTimer = window.setInterval(refresh, data.refresh_ms || 10000);
    }
  } catch (error) {
    statusLine.textContent = String(error);
    if (!refreshTimer) refreshTimer = window.setInterval(refresh, 10000);
  }
}

closeButton.addEventListener("click", closeDetail);
backdrop.addEventListener("click", closeDetail);
document.addEventListener("keydown", event => {
  if (event.key === "Escape") closeDetail();
});
refresh();
</script>
</body>
</html>
"""


@dataclass
class TrackerConfig:
    runner: CommandRunner
    user: str
    job_ids: List[str]
    db_path: str
    since_days: int
    no_sacct: bool
    limit: int
    interval: int


class TrackerHTTPServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: Tuple[str, int],
        handler_class: type,
        config: TrackerConfig,
    ) -> None:
        super().__init__(server_address, handler_class)
        self.config = config
        self.db_lock = threading.Lock()


class TrackerRequestHandler(BaseHTTPRequestHandler):
    server: TrackerHTTPServer

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"/", "/index.html"}:
            self.send_text(WEB_HTML, "text/html; charset=utf-8")
            return
        if parsed.path == "/api/jobs":
            self.send_jobs(parsed.query)
            return
        self.send_error(404, "Not found")

    def send_text(self, body: str, content_type: str) -> None:
        encoded = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)

    def send_json(self, payload: Dict[str, object]) -> None:
        encoded = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)

    def send_jobs(self, query: str) -> None:
        config = self.server.config
        params = parse_qs(query)
        limit = config.limit
        if "limit" in params:
            try:
                limit = max(1, int(params["limit"][0]))
            except (TypeError, ValueError):
                limit = config.limit

        with self.server.db_lock:
            conn = init_db(config.db_path)
            try:
                jobs, warnings = poll_jobs(
                    conn,
                    config.runner,
                    config.user,
                    config.job_ids,
                    config.since_days,
                    config.no_sacct,
                    limit,
                )
            finally:
                conn.close()

        self.send_json(
            {
                "updated_at": now_iso(),
                "refresh_ms": config.interval * 1000,
                "limit": limit,
                "backend": config.runner.label,
                "counts": count_jobs(jobs),
                "warnings": warnings,
                "jobs": [job_to_dict(job) for job in jobs],
            }
        )

    def log_message(self, fmt: str, *args: object) -> None:
        return


def run_web(args: argparse.Namespace, job_ids: List[str], db_path: str) -> int:
    config = TrackerConfig(
        runner=args.runner,
        user=args.user,
        job_ids=job_ids,
        db_path=db_path,
        since_days=args.since_days,
        no_sacct=args.no_sacct,
        limit=args.limit,
        interval=args.interval,
    )
    try:
        server = TrackerHTTPServer((args.host, args.port), TrackerRequestHandler, config)
    except OSError as exc:
        if args.port == 0:
            print(
                f"Could not bind {args.host}:0: {exc}",
                file=sys.stderr,
            )
            return 2
        print(
            f"Could not bind {args.host}:{args.port}: {exc}",
            file=sys.stderr,
        )
        print("Trying an automatically selected free port...", file=sys.stderr)
        try:
            server = TrackerHTTPServer((args.host, 0), TrackerRequestHandler, config)
        except OSError as fallback_exc:
            print(
                f"Could not bind {args.host}:0: {fallback_exc}",
                file=sys.stderr,
            )
            return 2
    host, port = server.server_address[:2]
    print(f"Slurm Tracker web UI: http://{host}:{port}/", flush=True)
    print(f"Backend: {config.runner.label}", flush=True)
    print(f"Database: {db_path}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def parse_job_ids(values: Sequence[str]) -> List[str]:
    job_ids: List[str] = []
    for value in values:
        for item in value.split(","):
            item = item.strip()
            if item:
                job_ids.append(item)
    return job_ids


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Realtime Slurm job tracker with local SQLite history."
    )
    parser.add_argument(
        "--user",
        default=os.environ.get("SLURM_TRACKER_USER"),
        help="Slurm user to monitor. Defaults to SSH user or current local user.",
    )
    parser.add_argument(
        "--backend",
        choices=["auto", "local", "ssh"],
        default=os.environ.get("SLURM_TRACKER_BACKEND", "auto"),
        help="How to run Slurm commands. auto uses local Slurm when available, otherwise SSH if configured.",
    )
    parser.add_argument(
        "--ssh-host",
        default=os.environ.get("SLURM_TRACKER_SSH_HOST", ""),
        help="Login node hostname for SSH mode. Can also use SLURM_TRACKER_SSH_HOST.",
    )
    parser.add_argument(
        "--ssh-user",
        default=os.environ.get("SLURM_TRACKER_SSH_USER", ""),
        help="SSH username for the login node. Defaults to the Slurm user.",
    )
    parser.add_argument(
        "--ssh-port",
        type=int,
        default=env_int("SLURM_TRACKER_SSH_PORT"),
        help="SSH port for the login node.",
    )
    parser.add_argument(
        "--ssh-key",
        default=os.environ.get("SLURM_TRACKER_SSH_KEY", ""),
        help="Private key path for SSH mode.",
    )
    parser.add_argument(
        "--ssh-option",
        action="append",
        default=[],
        help="Extra SSH -o option, for example ProxyJump=host. Can be repeated.",
    )
    parser.add_argument(
        "--ssh-no-batch",
        action="store_true",
        help="Allow interactive SSH password prompts. Not recommended for --web.",
    )
    parser.add_argument(
        "--command-timeout",
        type=int,
        default=45,
        help="Timeout in seconds for each local or SSH Slurm command.",
    )
    parser.add_argument(
        "--job-id",
        action="append",
        default=[],
        help="Limit monitoring to one job id. Can be repeated or comma-separated.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Polling interval in seconds.",
    )
    parser.add_argument(
        "--db",
        default="slurm_jobs.sqlite3",
        help="Local SQLite database path.",
    )
    parser.add_argument(
        "--since-days",
        type=int,
        default=7,
        help="How many days of sacct history to read.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum recent jobs to display after active queued/running jobs.",
    )
    parser.add_argument(
        "--web",
        action="store_true",
        help="Run the local browser UI instead of the terminal table.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host for --web.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Port for --web. Use 0 to pick a free port.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Poll once, record locally, print the table, then exit.",
    )
    parser.add_argument(
        "--no-sacct",
        action="store_true",
        help="Only use squeue. Completed/error jobs may not be detected after they leave squeue.",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Use W/Y/G/R instead of colored terminal lights.",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Do not clear the terminal between refreshes.",
    )
    return parser


def local_slurm_available(no_sacct: bool) -> bool:
    if shutil.which("squeue") is None:
        return False
    if not no_sacct and shutil.which("sacct") is None:
        return False
    return True


def resolve_runner(args: argparse.Namespace) -> Tuple[Optional[CommandRunner], Optional[str]]:
    host_user, ssh_host = split_ssh_host(args.ssh_host)
    user = (
        clean_optional(args.user)
        or clean_optional(args.ssh_user)
        or host_user
        or getpass.getuser()
    )
    ssh_user = clean_optional(args.ssh_user) or host_user or user
    is_windows = platform.system().lower() == "windows"

    if args.backend == "local":
        return (
            CommandRunner(backend="local", timeout=args.command_timeout),
            None,
        )

    if args.backend == "ssh" or (args.backend == "auto" and ssh_host):
        if not ssh_host:
            return None, "--ssh-host is required for SSH backend"
        return (
            CommandRunner(
                backend="ssh",
                ssh_host=ssh_host,
                ssh_user=ssh_user,
                ssh_port=args.ssh_port,
                ssh_key=clean_optional(args.ssh_key),
                ssh_options=tuple(args.ssh_option or ()),
                ssh_batch=not args.ssh_no_batch,
                timeout=args.command_timeout,
            ),
            None,
        )

    if args.backend == "auto" and local_slurm_available(args.no_sacct):
        return (
            CommandRunner(backend="local", timeout=args.command_timeout),
            None,
        )

    if args.backend == "auto" and is_windows:
        return (
            None,
            "Windows detected, but no SSH login node is configured. "
            "Set --ssh-host or SLURM_TRACKER_SSH_HOST.",
        )

    return (
        None,
        "No local Slurm commands found. Use --ssh-host LOGIN_NODE or --backend local.",
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.interval < 1:
        print("--interval must be at least 1 second", file=sys.stderr)
        return 2
    if args.since_days < 0:
        print("--since-days must be >= 0", file=sys.stderr)
        return 2
    if args.limit < 1:
        print("--limit must be at least 1", file=sys.stderr)
        return 2
    if args.command_timeout < 1:
        print("--command-timeout must be at least 1", file=sys.stderr)
        return 2

    job_ids = parse_job_ids(args.job_id)
    host_user, _ = split_ssh_host(args.ssh_host)
    args.user = (
        clean_optional(args.user)
        or clean_optional(args.ssh_user)
        or host_user
        or getpass.getuser()
    )
    runner, runner_error = resolve_runner(args)
    if runner_error:
        print(runner_error, file=sys.stderr)
        return 2
    args.runner = runner
    db_path = os.path.abspath(args.db)
    if args.web:
        return run_web(args, job_ids, db_path)

    conn = init_db(db_path)
    use_color = not args.no_color and sys.stdout.isatty()
    stop = False

    def handle_stop(signum, frame):  # type: ignore[no-untyped-def]
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    while not stop:
        displayed, warnings = poll_jobs(
            conn,
            runner,
            args.user,
            job_ids,
            args.since_days,
            args.no_sacct,
            args.limit,
        )
        render(
            displayed,
            warnings,
            db_path,
            runner.label,
            args.interval,
            use_color,
            clear=not args.no_clear,
        )

        if args.once:
            break
        for _ in range(args.interval):
            if stop:
                break
            time.sleep(1)

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
