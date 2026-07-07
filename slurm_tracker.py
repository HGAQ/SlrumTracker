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


@dataclass
class PartitionSummary:
    partition: str
    availability: str = ""
    max_time: str = ""
    nodes_total: int = 0
    nodes_idle: int = 0
    nodes_allocated: int = 0
    nodes_mixed: int = 0
    nodes_down: int = 0
    nodes_draining: int = 0
    nodes_other: int = 0
    jobs_queued: int = 0
    jobs_running: int = 0
    jobs_other: int = 0
    user_queued: int = 0
    user_running: int = 0

    @property
    def load_text(self) -> str:
        active = self.nodes_allocated + self.nodes_mixed
        return f"{active}/{self.nodes_total}" if self.nodes_total else "-"


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


def normalize_partition(value: str) -> str:
    value = clean_optional(value).rstrip("*")
    return value or "unknown"


def parse_int(value: str) -> int:
    try:
        return int(clean_optional(value))
    except ValueError:
        return 0


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
            if DELIM in line:
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
            if DELIM in line:
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


def add_node_state(summary: PartitionSummary, state: str, count: int) -> None:
    state = clean_optional(state).lower()
    summary.nodes_total += count
    if "idle" in state:
        summary.nodes_idle += count
    elif "alloc" in state:
        summary.nodes_allocated += count
    elif "mix" in state:
        summary.nodes_mixed += count
    elif "drain" in state or "drng" in state:
        summary.nodes_draining += count
    elif "down" in state or "fail" in state:
        summary.nodes_down += count
    else:
        summary.nodes_other += count


def load_sinfo_partitions(
    runner: CommandRunner,
) -> Tuple[Dict[str, PartitionSummary], List[str]]:
    fmt = DELIM.join(["%P", "%a", "%l", "%D", "%T"])
    output, error = run_command(["sinfo", "-h", "-o", fmt], runner)
    warnings = [error] if error else []
    partitions: Dict[str, PartitionSummary] = {}

    for line in output.splitlines():
        if not line.strip():
            continue
        fields = split_row(line, 5)
        if fields is None:
            if DELIM in line:
                warnings.append(f"could not parse sinfo row: {line!r}")
            continue
        raw_partition, availability, max_time, nodes, node_state = fields
        partition = normalize_partition(raw_partition)
        summary = partitions.setdefault(partition, PartitionSummary(partition=partition))
        if availability.lower() == "up" or not summary.availability:
            summary.availability = availability
        if max_time and not summary.max_time:
            summary.max_time = max_time
        add_node_state(summary, node_state, parse_int(nodes))

    return partitions, warnings


def load_partition_queue_counts(
    runner: CommandRunner,
) -> Tuple[Dict[str, Dict[str, int]], List[str]]:
    fmt = DELIM.join(["%P", "%T"])
    output, error = run_command(["squeue", "-h", "-o", fmt], runner)
    warnings = [error] if error else []
    counts: Dict[str, Dict[str, int]] = {}

    for line in output.splitlines():
        if not line.strip():
            continue
        fields = split_row(line, 2)
        if fields is None:
            if DELIM in line:
                warnings.append(f"could not parse partition squeue row: {line!r}")
            continue
        partition, state = fields
        category = category_for_state(state)
        bucket = counts.setdefault(
            normalize_partition(partition),
            {"queued": 0, "running": 0, "other": 0},
        )
        if category == "queued":
            bucket["queued"] += 1
        elif category == "running":
            bucket["running"] += 1
        else:
            bucket["other"] += 1

    return counts, warnings


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


def partition_sort_key(summary: PartitionSummary) -> Tuple[int, int, int, str]:
    availability_rank = 0 if summary.availability.lower() == "up" else 1
    return (
        availability_rank,
        -(summary.jobs_queued + summary.jobs_running),
        -summary.nodes_total,
        summary.partition,
    )


def load_partition_summaries(
    runner: CommandRunner,
    active_user_jobs: Dict[str, Job],
) -> Tuple[List[PartitionSummary], List[str]]:
    partitions, warnings = load_sinfo_partitions(runner)
    queue_counts, queue_warnings = load_partition_queue_counts(runner)
    warnings.extend(queue_warnings)

    for partition, counts in queue_counts.items():
        summary = partitions.setdefault(
            partition,
            PartitionSummary(partition=partition),
        )
        summary.jobs_queued = counts.get("queued", 0)
        summary.jobs_running = counts.get("running", 0)
        summary.jobs_other = counts.get("other", 0)

    for job in active_user_jobs.values():
        summary = partitions.setdefault(
            normalize_partition(job.partition),
            PartitionSummary(partition=normalize_partition(job.partition)),
        )
        if job.category == "queued":
            summary.user_queued += 1
        elif job.category == "running":
            summary.user_running += 1

    result = list(partitions.values())
    result.sort(key=partition_sort_key)
    return result, warnings


def poll_jobs(
    conn: sqlite3.Connection,
    runner: CommandRunner,
    user: str,
    job_ids: Sequence[str],
    since_days: int,
    no_sacct: bool,
    limit: int,
) -> Tuple[List[Job], List[PartitionSummary], List[str]]:
    since = datetime.now() - timedelta(days=since_days)
    active, warnings = load_squeue_jobs(runner, user, job_ids)
    accounted: Dict[str, Job] = {}
    if not no_sacct:
        accounted, sacct_warnings = load_sacct_jobs(runner, user, job_ids, since)
        warnings.extend(sacct_warnings)

    current = combine_jobs(active, accounted)
    record_jobs(conn, current.values())
    partitions, partition_warnings = load_partition_summaries(runner, active)
    warnings.extend(partition_warnings)
    return choose_display_jobs(conn, active, limit), partitions, warnings


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


def partition_to_dict(summary: PartitionSummary) -> Dict[str, object]:
    return {
        "partition": summary.partition,
        "availability": summary.availability,
        "max_time": summary.max_time,
        "nodes_total": summary.nodes_total,
        "nodes_idle": summary.nodes_idle,
        "nodes_allocated": summary.nodes_allocated,
        "nodes_mixed": summary.nodes_mixed,
        "nodes_down": summary.nodes_down,
        "nodes_draining": summary.nodes_draining,
        "nodes_other": summary.nodes_other,
        "load_text": summary.load_text,
        "jobs_queued": summary.jobs_queued,
        "jobs_running": summary.jobs_running,
        "jobs_other": summary.jobs_other,
        "user_queued": summary.user_queued,
        "user_running": summary.user_running,
    }


def terminal_width() -> int:
    return shutil.get_terminal_size((120, 30)).columns


def render(
    jobs: Sequence[Job],
    partitions: Sequence[PartitionSummary],
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

    if partitions:
        print("Partitions:")
        print(
            f"{'PARTITION':<14} {'AVAIL':<7} {'QUEUED':>6} {'RUN':>5} "
            f"{'MINE_Q':>6} {'MINE_R':>6} {'NODES':>9} "
            f"{'IDLE':>5} {'DOWN':>5} {'DRAIN':>5} {'LIMIT':>10}"
        )
        print("-" * min(width, 100))
        for part in partitions[:20]:
            print(
                f"{truncate(part.partition, 14):<14} "
                f"{truncate(part.availability, 7):<7} "
                f"{part.jobs_queued:>6} "
                f"{part.jobs_running:>5} "
                f"{part.user_queued:>6} "
                f"{part.user_running:>6} "
                f"{part.load_text:>9} "
                f"{part.nodes_idle:>5} "
                f"{part.nodes_down:>5} "
                f"{part.nodes_draining:>5} "
                f"{truncate(part.max_time, 10):>10}"
            )
        print()

    if not jobs:
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
  --node-red: #d94b4b;
  --node-yellow: #f2c94c;
  --node-blue: #3b82d6;
  --node-green: #2fbf71;
  --node-gray: #9aa4b2;
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
.partition-strip {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
  gap: 10px;
  margin-bottom: 26px;
}
.partition-item {
  min-width: 0;
  border: 1px solid var(--line);
  border-radius: 8px;
  background: #ffffff;
  padding: 10px 12px;
  color: inherit;
  cursor: pointer;
  font: inherit;
  text-align: left;
  width: 100%;
}
.partition-item:hover,
.partition-item:focus-visible {
  border-color: #9aa4b2;
  outline: 3px solid rgba(31, 41, 51, 0.12);
  outline-offset: 2px;
}
.partition-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 8px;
}
.partition-name {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 13px;
  font-weight: 650;
}
.partition-avail {
  border-radius: 999px;
  border: 1px solid var(--line);
  padding: 2px 7px;
  color: var(--muted);
  font-size: 11px;
  line-height: 1.4;
  white-space: nowrap;
}
.partition-avail[data-up="true"] {
  color: #17633a;
  border-color: rgba(47, 191, 113, 0.4);
}
.partition-metrics {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
}
.partition-body {
  display: grid;
  grid-template-columns: 62px minmax(0, 1fr);
  gap: 12px;
  align-items: center;
}
.partition-pie {
  position: relative;
  width: 62px;
  height: 62px;
  border-radius: 50%;
  background: var(--node-gray);
  box-shadow:
    inset 0 0 0 1px rgba(31, 41, 51, 0.14),
    0 4px 10px rgba(31, 41, 51, 0.1);
}
.partition-pie::after {
  content: "";
  position: absolute;
  inset: 17px;
  border-radius: 50%;
  background: #ffffff;
  box-shadow: inset 0 0 0 1px rgba(31, 41, 51, 0.08);
}
.partition-pie-total {
  position: absolute;
  inset: 0;
  z-index: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 12px;
  font-weight: 650;
  font-variant-numeric: tabular-nums;
}
.metric-label {
  display: block;
  color: var(--muted);
  font-size: 10px;
}
.metric-value {
  display: block;
  margin-top: 2px;
  font-size: 13px;
  font-variant-numeric: tabular-nums;
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
.node-chart-section {
  display: grid;
  grid-template-columns: 132px minmax(0, 1fr);
  gap: 16px;
  align-items: center;
  padding: 18px 22px;
  border-bottom: 1px solid var(--line);
}
.node-pie {
  position: relative;
  width: 132px;
  height: 132px;
  border-radius: 50%;
  background: var(--node-gray);
  box-shadow:
    inset 0 0 0 1px rgba(31, 41, 51, 0.14),
    0 8px 20px rgba(31, 41, 51, 0.12);
}
.node-pie::after {
  content: "";
  position: absolute;
  inset: 34px;
  border-radius: 50%;
  background: var(--panel);
  box-shadow: inset 0 0 0 1px rgba(31, 41, 51, 0.08);
}
.node-pie-total {
  position: absolute;
  inset: 0;
  z-index: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 16px;
  font-weight: 650;
  font-variant-numeric: tabular-nums;
}
.node-legend {
  display: grid;
  gap: 8px;
  min-width: 0;
}
.node-legend-row {
  display: grid;
  grid-template-columns: 12px minmax(0, 1fr) auto;
  gap: 8px;
  align-items: center;
  font-size: 12px;
}
.node-swatch {
  width: 12px;
  height: 12px;
  border-radius: 3px;
  box-shadow: inset 0 0 0 1px rgba(31, 41, 51, 0.16);
}
.node-label {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.node-value {
  color: var(--muted);
  font-variant-numeric: tabular-nums;
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
  .node-chart-section {
    grid-template-columns: 112px minmax(0, 1fr);
  }
  .node-pie {
    width: 112px;
    height: 112px;
  }
  .node-pie::after {
    inset: 29px;
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
  <div class="status-line" id="status">Loading Slurm data...</div>
  <section class="partition-strip" id="partitions" aria-label="Partitions"></section>
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
  <section class="node-chart-section" id="node-chart" hidden>
    <div class="node-pie" id="node-pie" aria-label="Node state pie chart">
      <span class="node-pie-total" id="node-pie-total"></span>
    </div>
    <div class="node-legend" id="node-legend"></div>
  </section>
  <dl class="details" id="details"></dl>
</aside>
<script>
const lights = document.getElementById("lights");
const partitions = document.getElementById("partitions");
const summary = document.getElementById("summary");
const statusLine = document.getElementById("status");
const detail = document.getElementById("detail");
const backdrop = document.getElementById("backdrop");
const detailTitle = document.getElementById("detail-title");
const detailSubtitle = document.getElementById("detail-subtitle");
const details = document.getElementById("details");
const nodeChart = document.getElementById("node-chart");
const nodePie = document.getElementById("node-pie");
const nodePieTotal = document.getElementById("node-pie-total");
const nodeLegend = document.getElementById("node-legend");
const closeButton = document.getElementById("close");
let jobsById = new Map();
let selectedJobId = null;
let selectedPartitionName = null;
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

function percent(value, total) {
  if (!total) return "0.0%";
  return `${((Number(value || 0) / total) * 100).toFixed(1)}%`;
}

function nodeSlices(partition) {
  return [
    ["Down", Number(partition.nodes_down || 0), "var(--node-red)"],
    ["Draining", Number(partition.nodes_draining || 0), "var(--node-red)"],
    ["Allocated", Number(partition.nodes_allocated || 0), "var(--node-yellow)"],
    ["Mixed", Number(partition.nodes_mixed || 0), "var(--node-blue)"],
    ["Idle", Number(partition.nodes_idle || 0), "var(--node-green)"],
    ["Other", Number(partition.nodes_other || 0), "var(--node-gray)"],
  ];
}

function nodePieGradient(slices, total) {
  if (!total) return "var(--node-gray)";
  let cursor = 0;
  const segments = [];
  slices.forEach(([, value, color]) => {
    if (!value) return;
    const start = cursor;
    cursor += (value / total) * 360;
    segments.push(`${color} ${start.toFixed(3)}deg ${cursor.toFixed(3)}deg`);
  });
  return segments.length
    ? `conic-gradient(${segments.join(", ")})`
    : "var(--node-gray)";
}

function renderNodeChart(partition) {
  const slices = nodeSlices(partition);
  const total = slices.reduce((sum, [, value]) => sum + value, 0);
  nodePie.style.background = nodePieGradient(slices, total);
  nodePieTotal.textContent = String(total);
  nodeLegend.innerHTML = slices.map(([label, value, color]) => `
    <div class="node-legend-row">
      <span class="node-swatch" style="background: ${color}"></span>
      <span class="node-label">${escapeHtml(label)}</span>
      <span class="node-value">${escapeHtml(value)} (${escapeHtml(percent(value, total))})</span>
    </div>
  `).join("");
  nodeChart.hidden = false;
}

function renderSummary(data) {
  if (data.error) {
    summary.innerHTML = `<span>${escapeHtml(data.backend || "backend unknown")}</span>`;
    return;
  }
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
  selectedPartitionName = null;
  nodeChart.hidden = true;
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

function openPartitionDetail(partition) {
  selectedJobId = null;
  selectedPartitionName = partition.partition;
  const downTotal = Number(partition.nodes_down || 0) + Number(partition.nodes_draining || 0);
  detailTitle.textContent = partition.partition;
  detailSubtitle.textContent = `Availability ${text(partition.availability)}`;
  renderNodeChart(partition);
  const fields = [
    ["Availability", partition.availability],
    ["Max Time", partition.max_time],
    ["Queue Jobs", partition.jobs_queued],
    ["Running Jobs", partition.jobs_running],
    ["Other Jobs", partition.jobs_other],
    ["My Queued", partition.user_queued],
    ["My Running", partition.user_running],
    ["Nodes Used", partition.load_text],
    ["Nodes Total", partition.nodes_total],
    ["Nodes Idle", partition.nodes_idle],
    ["Nodes Allocated", partition.nodes_allocated],
    ["Nodes Mixed", partition.nodes_mixed],
    ["Nodes Down", partition.nodes_down],
    ["Nodes Draining", partition.nodes_draining],
    ["Nodes Down+Drain", downTotal],
    ["Nodes Other", partition.nodes_other],
  ];
  details.innerHTML = fields.map(([key, value]) =>
    `<dt>${escapeHtml(key)}</dt><dd>${escapeHtml(value)}</dd>`
  ).join("");
  detail.hidden = false;
  backdrop.hidden = false;
}

function closeDetail() {
  selectedJobId = null;
  selectedPartitionName = null;
  nodeChart.hidden = true;
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

function renderPartitions(data) {
  partitions.replaceChildren();
  (data.partitions || []).forEach(partition => {
    const item = document.createElement("button");
    item.type = "button";
    item.className = "partition-item";
    const isUp = String(partition.availability || "").toLowerCase() === "up";
    item.title = `${partition.partition} availability ${partition.availability}`;
    item.setAttribute("aria-label", item.title);
    const slices = nodeSlices(partition);
    const nodeTotal = slices.reduce((sum, [, value]) => sum + value, 0);
    item.innerHTML = `
      <div class="partition-head">
        <div class="partition-name" title="${escapeHtml(partition.partition)}">${escapeHtml(partition.partition)}</div>
        <div class="partition-avail" data-up="${isUp}" title="Partition availability from sinfo">Avail ${escapeHtml(partition.availability)}</div>
      </div>
      <div class="partition-body">
        <div class="partition-pie" style="background: ${nodePieGradient(slices, nodeTotal)}" aria-label="Node state pie chart">
          <span class="partition-pie-total">${escapeHtml(nodeTotal)}</span>
        </div>
        <div class="partition-metrics">
          <div><span class="metric-label">Queue</span><span class="metric-value">${escapeHtml(partition.jobs_queued)}</span></div>
          <div><span class="metric-label">Run</span><span class="metric-value">${escapeHtml(partition.jobs_running)}</span></div>
          <div><span class="metric-label">Mine</span><span class="metric-value">${escapeHtml(partition.user_queued)} / ${escapeHtml(partition.user_running)}</span></div>
          <div><span class="metric-label">Nodes</span><span class="metric-value">${escapeHtml(partition.load_text)}</span></div>
          <div><span class="metric-label">Idle</span><span class="metric-value">${escapeHtml(partition.nodes_idle)}</span></div>
          <div><span class="metric-label">Down</span><span class="metric-value">${escapeHtml(partition.nodes_down + partition.nodes_draining)}</span></div>
        </div>
      </div>`;
    item.addEventListener("click", () => openPartitionDetail(partition));
    partitions.appendChild(item);
  });
  if (selectedPartitionName) {
    const selected = (data.partitions || []).find(
      partition => partition.partition === selectedPartitionName
    );
    if (selected) openPartitionDetail(selected);
  }
}

async function refresh() {
  try {
    const response = await fetch("/api/jobs", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    renderSummary(data);
    if (data.error) {
      partitions.replaceChildren();
      lights.replaceChildren();
      statusLine.textContent = data.error;
      if (!refreshTimer) {
        refreshTimer = window.setInterval(refresh, data.refresh_ms || 10000);
      }
      return;
    }
    renderPartitions(data);
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
        try:
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(encoded)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            return

    def send_json(self, payload: Dict[str, object]) -> None:
        encoded = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            return

    def send_jobs(self, query: str) -> None:
        config = self.server.config
        params = parse_qs(query)
        limit = config.limit
        if "limit" in params:
            try:
                limit = max(1, int(params["limit"][0]))
            except (TypeError, ValueError):
                limit = config.limit

        try:
            with self.server.db_lock:
                conn = init_db(config.db_path)
                try:
                    jobs, partitions, warnings = poll_jobs(
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
        except Exception as exc:
            self.send_json(
                {
                    "updated_at": now_iso(),
                    "refresh_ms": config.interval * 1000,
                    "limit": limit,
                    "backend": config.runner.label,
                    "counts": count_jobs([]),
                    "warnings": [],
                    "partitions": [],
                    "jobs": [],
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            return

        self.send_json(
            {
                "updated_at": now_iso(),
                "refresh_ms": config.interval * 1000,
                "limit": limit,
                "backend": config.runner.label,
                "counts": count_jobs(jobs),
                "warnings": warnings,
                "partitions": [partition_to_dict(part) for part in partitions],
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
    explicit_ssh_user = clean_optional(args.ssh_user)
    if host_user and explicit_ssh_user and host_user != explicit_ssh_user:
        return (
            None,
            "--ssh-host looks like user@host, but --ssh-user is different. "
            "Use either --ssh-host sycamore.unc.edu --ssh-user lsr "
            "or --ssh-host lsr@sycamore.unc.edu.",
        )
    user = (
        clean_optional(args.user)
        or explicit_ssh_user
        or host_user
        or getpass.getuser()
    )
    ssh_user = explicit_ssh_user or host_user or user
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
        displayed, partitions, warnings = poll_jobs(
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
            partitions,
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
