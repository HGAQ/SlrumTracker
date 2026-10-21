#!/usr/bin/env python3
"""Small terminal tracker for Slurm jobs with local SQLite history."""

from __future__ import annotations

import argparse
import configparser
import getpass
import json
import os
import platform
import re
import shlex
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse


DELIM = "\x1f"
SECTION_MARKER = "__SLURM_TRACKER_SECTION__"

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
    cpus_total: int = 0
    cpus_allocated: int = 0
    cpus_idle: int = 0
    cpus_other: int = 0
    cpus_available: bool = False
    cpus_reserved_idle: int = 0
    node_cpu_counts: Dict[str, Optional[Tuple[int, int, int, int, int]]] = field(default_factory=dict)
    job_cpus_allocated: Optional[int] = None
    user_cpus_allocated: Optional[int] = None
    partition_qos: str = ""
    qos_user_cpu_limit: Optional[int] = None
    qos_user_cpus_allocated: Optional[int] = None
    jobs_queued: int = 0
    jobs_running: int = 0
    jobs_other: int = 0
    user_queued: int = 0
    user_running: int = 0
    user_pending_reasons: Dict[str, int] = field(default_factory=dict)

    @property
    def load_text(self) -> str:
        active = self.nodes_allocated + self.nodes_mixed
        return f"{active}/{self.nodes_total}" if self.nodes_total else "-"

    @property
    def cpu_load_text(self) -> str:
        if not self.cpus_available:
            return "-"
        return f"{self.cpus_allocated}/{self.cpus_total}"

    @property
    def user_cpu_load_text(self) -> str:
        if self.qos_user_cpu_limit is not None:
            if self.qos_user_cpus_allocated is None:
                return "-"
            return f"{self.qos_user_cpus_allocated}/{self.qos_user_cpu_limit}"
        return str(self.user_cpus_allocated) if self.user_cpus_allocated is not None else "-"


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
    ssh_args.extend(
        [
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=2",
            "-o",
            "ConnectionAttempts=2",
        ]
    )
    if runner.ssh_port is not None:
        ssh_args.extend(["-p", str(runner.ssh_port)])
    if runner.ssh_key:
        ssh_args.extend(["-i", runner.ssh_key])
    for option in runner.ssh_options:
        ssh_args.extend(["-o", option])
    ssh_args.append(runner.ssh_target)
    ssh_args.append(make_remote_command(remote_args))
    return ssh_args


def build_ssh_shell_args(runner: CommandRunner, script: str) -> List[str]:
    ssh_args = build_ssh_args(runner, ["true"])
    ssh_args[-1] = "sh -lc " + shlex.quote(script)
    return ssh_args


def transient_ssh_error(message: str) -> bool:
    message = message.lower()
    return any(
        pattern in message
        for pattern in (
            "connection closed",
            "connection reset",
            "connection aborted",
            "timed out",
            "broken pipe",
        )
    )


def run_command(args: Sequence[str], runner: CommandRunner) -> Tuple[str, Optional[str]]:
    command = list(args)
    if runner.backend == "ssh":
        command = build_ssh_args(runner, command)

    attempts = 2 if runner.backend == "ssh" else 1
    last_error = ""
    for attempt in range(attempts):
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
            last_error = f"{runner.label} timed out running {args[0]}"
            if attempt + 1 < attempts and transient_ssh_error(last_error):
                time.sleep(0.5)
                continue
            return "", last_error

        if result.returncode == 0:
            return result.stdout, None

        message = result.stderr.strip() or result.stdout.strip()
        last_error = f"{runner.label} {args[0]} failed: {message}"
        if attempt + 1 < attempts and transient_ssh_error(last_error):
            time.sleep(0.5)
            continue
        return "", last_error
    return "", last_error


def run_ssh_batch(
    runner: CommandRunner,
    commands: Sequence[Tuple[str, Sequence[str]]],
) -> Tuple[Dict[str, str], List[str]]:
    script_lines: List[str] = []
    for name, args in commands:
        script_lines.append(f"printf '%s\\n' {shlex.quote(SECTION_MARKER + ' BEGIN ' + name)}")
        script_lines.append(f"{{ {make_remote_command(args)}; }} 2>&1")
        script_lines.append("rc=$?")
        script_lines.append(
            f"printf '%s %s\\n' {shlex.quote(SECTION_MARKER + ' END ' + name)} \"$rc\""
        )
    script = "\n".join(script_lines)
    command = build_ssh_shell_args(runner, script)
    attempts = 2
    last_error = ""

    for attempt in range(attempts):
        try:
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
                timeout=max(runner.timeout, runner.timeout * len(commands)),
            )
        except FileNotFoundError:
            return {}, ["command not found: ssh"]
        except subprocess.TimeoutExpired:
            last_error = f"{runner.label} timed out running Slurm batch"
            if attempt + 1 < attempts:
                time.sleep(0.5)
                continue
            return {}, [last_error]

        stderr = result.stderr.strip()
        stdout = result.stdout
        combined_error = stderr or stdout.strip()
        if result.returncode != 0 and transient_ssh_error(combined_error):
            last_error = f"{runner.label} Slurm batch failed: {combined_error}"
            if attempt + 1 < attempts:
                time.sleep(0.5)
                continue

        sections: Dict[str, List[str]] = {}
        return_codes: Dict[str, int] = {}
        current: Optional[str] = None
        for line in stdout.splitlines():
            if line.startswith(SECTION_MARKER + " BEGIN "):
                current = line.split(" BEGIN ", 1)[1].strip()
                sections[current] = []
                continue
            if line.startswith(SECTION_MARKER + " END "):
                rest = line.split(" END ", 1)[1].strip()
                parts = rest.rsplit(" ", 1)
                if len(parts) == 2:
                    name, rc = parts
                    return_codes[name] = parse_int(rc)
                current = None
                continue
            if current:
                sections.setdefault(current, []).append(line)

        outputs = {name: "\n".join(lines) for name, lines in sections.items()}
        warnings: List[str] = []
        if result.returncode != 0:
            warnings.append(f"{runner.label} Slurm batch failed: {combined_error}")
        for name, args in commands:
            rc = return_codes.get(name, 0)
            if rc != 0:
                output = outputs.get(name, "").strip()
                warning = f"{runner.label} {args[0]} failed"
                if output:
                    warning += f": {output}"
                warnings.append(warning)
        return outputs, warnings

    return {}, [last_error]


def split_row(line: str, expected_fields: int) -> Optional[List[str]]:
    fields = line.rstrip("\n").split(DELIM)
    if len(fields) != expected_fields:
        return None
    return [field.strip() for field in fields]


def build_squeue_jobs_args(user: str, job_ids: Sequence[str]) -> List[str]:
    fmt = DELIM.join(["%i", "%T", "%j", "%P", "%M", "%l", "%D", "%R"])
    args = ["squeue", "-h", "--array", "-o", fmt]
    if user:
        args.extend(["--user", user])
    if job_ids:
        args.extend(["--jobs", ",".join(job_ids)])
    return args


def parse_squeue_jobs_output(output: str, user: str) -> Tuple[Dict[str, Job], List[str]]:
    warnings: List[str] = []
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


def load_squeue_jobs(
    runner: CommandRunner,
    user: str,
    job_ids: Sequence[str],
) -> Tuple[Dict[str, Job], List[str]]:
    output, error = run_command(build_squeue_jobs_args(user, job_ids), runner)
    jobs, warnings = parse_squeue_jobs_output(output, user)
    if error:
        warnings.insert(0, error)
    return jobs, warnings


def build_sacct_jobs_args(
    user: str,
    job_ids: Sequence[str],
    since: datetime,
) -> List[str]:
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
    return args


def parse_sacct_jobs_output(output: str, user: str) -> Tuple[Dict[str, Job], List[str]]:
    warnings: List[str] = []
    fields_count = 12
    jobs: Dict[str, Job] = {}

    for line in output.splitlines():
        if not line.strip():
            continue
        row = split_row(line, fields_count)
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


def load_sacct_jobs(
    runner: CommandRunner,
    user: str,
    job_ids: Sequence[str],
    since: datetime,
) -> Tuple[Dict[str, Job], List[str]]:
    output, error = run_command(build_sacct_jobs_args(user, job_ids, since), runner)
    jobs, warnings = parse_sacct_jobs_output(output, user)
    if error:
        warnings.insert(0, error)
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


def build_sinfo_partitions_args() -> List[str]:
    # One row per node/partition lets shared pools deduplicate physical CPUs.
    fmt = DELIM.join(["%P", "%a", "%l", "%D", "%T", "%C", "%N"])
    return ["sinfo", "-N", "-h", "-o", fmt]


def parse_sinfo_partitions_output(
    output: str,
) -> Tuple[Dict[str, PartitionSummary], List[str]]:
    warnings: List[str] = []
    partitions: Dict[str, PartitionSummary] = {}
    invalid_cpu_partitions = set()

    for line in output.splitlines():
        if not line.strip():
            continue
        fields = split_row(line, 7) or split_row(line, 6)
        if fields is None:
            if DELIM in line:
                invalid_cpu_partitions.add(normalize_partition(line.split(DELIM, 1)[0]))
                warnings.append(f"could not parse sinfo row: {line!r}")
            continue
        raw_partition, availability, max_time, nodes, node_state, cpu_states = fields[:6]
        node_name = fields[6] if len(fields) == 7 else ""
        partition = normalize_partition(raw_partition)
        summary = partitions.setdefault(partition, PartitionSummary(partition=partition))
        if availability.lower() == "up" or not summary.availability:
            summary.availability = availability
        if max_time and not summary.max_time:
            summary.max_time = max_time
        add_node_state(summary, node_state, parse_int(nodes))
        if node_name:
            summary.node_cpu_counts[node_name] = None
        try:
            cpu_counts = [int(cpu_count) for cpu_count in cpu_states.split("/")]
            if len(cpu_counts) != 4 or any(cpu_count < 0 for cpu_count in cpu_counts):
                raise ValueError("expected four nonnegative CPU counts")
            cpus_allocated, cpus_idle, cpus_other, cpus_total = cpu_counts
            if cpus_allocated + cpus_idle + cpus_other != cpus_total:
                raise ValueError("CPU states do not add up to total")
        except ValueError:
            invalid_cpu_partitions.add(partition)
            warnings.append(f"could not parse sinfo CPU counts for {partition}: {cpu_states!r}")
            continue
        summary.cpus_allocated += cpus_allocated
        summary.cpus_idle += cpus_idle
        summary.cpus_other += cpus_other
        summary.cpus_total += cpus_total
        reserved_idle = cpus_idle if "resv" in node_state.lower() or "reserved" in node_state.lower() else 0
        summary.cpus_reserved_idle += reserved_idle
        if node_name:
            summary.node_cpu_counts[node_name] = (
                cpus_allocated, cpus_idle, cpus_other, cpus_total, reserved_idle,
            )
        summary.cpus_available = True

    for partition in invalid_cpu_partitions:
        # Do not present a partial sum as the full partition's CPU capacity.
        if partition in partitions:
            partitions[partition].cpus_available = False

    return partitions, warnings


def load_sinfo_partitions(
    runner: CommandRunner,
) -> Tuple[Dict[str, PartitionSummary], List[str]]:
    output, error = run_command(build_sinfo_partitions_args(), runner)
    partitions, warnings = parse_sinfo_partitions_output(output)
    if error:
        warnings.insert(0, error)
    return partitions, warnings


def build_partition_queue_counts_args() -> List[str]:
    fmt = DELIM.join(["%P", "%T", "%C", "%u"])
    # Expand pending arrays so each element counts as a job.
    return ["squeue", "-h", "--array", "-o", fmt]


def parse_partition_queue_counts_output(
    output: str, user: str = "",
) -> Tuple[Dict[str, Dict[str, int]], List[str]]:
    warnings: List[str] = []
    counts: Dict[str, Dict[str, int]] = {}

    for line in output.splitlines():
        if not line.strip():
            continue
        fields = split_row(line, 4)
        if fields is None:
            if DELIM in line:
                warnings.append(f"could not parse partition squeue row: {line!r}")
            continue
        partition_list, state, cpu_text, job_user = fields
        try:
            job_cpu_count = int(cpu_text)
            if job_cpu_count < 0:
                raise ValueError("negative job CPU count")
        except ValueError:
            warnings.append(f"could not parse squeue CPU count: {cpu_text!r}")
            continue
        category = category_for_state(state)
        for partition in set(partition_list.split(",")):
            partition_counts = counts.setdefault(
                normalize_partition(partition),
                {"queued": 0, "running": 0, "other": 0,
                 "allocated_cpus": 0, "user_allocated_cpus": 0},
            )
            if category == "queued":
                partition_counts["queued"] += 1
            elif category == "running":
                partition_counts["running"] += 1
                # Suspended jobs retain memory but can release their CPUs.
                if clean_state(state) != "SUSPENDED":
                    partition_counts["allocated_cpus"] += job_cpu_count
                    if job_user == user:
                        partition_counts["user_allocated_cpus"] += job_cpu_count
            else:
                partition_counts["other"] += 1

    return counts, warnings


def load_partition_queue_counts(
    runner: CommandRunner, user: str = "",
) -> Tuple[Optional[Dict[str, Dict[str, int]]], List[str]]:
    output, error = run_command(build_partition_queue_counts_args(), runner)
    counts, warnings = parse_partition_queue_counts_output(output, user)
    if error:
        warnings.insert(0, error)
    return (None if warnings else counts), warnings


def build_partition_policies_args() -> List[str]:
    return ["scontrol", "show", "partition", "-o"]


def build_qos_cpu_limits_args() -> List[str]:
    return ["sacctmgr", "-n", "-P", "show", "qos", "format=Name,MaxTRESPU"]


def parse_partition_cpu_policies(
    partition_output: str, qos_output: str,
) -> Dict[str, Tuple[str, Optional[int]]]:
    """Read partition QoS, not job %q (which may simply be 'normal')."""
    qos_cpu_limits: Dict[str, int] = {}
    for qos_line in qos_output.splitlines():
        qos_fields = qos_line.split("|")
        if len(qos_fields) < 2:
            continue
        for qos_tres in qos_fields[1].split(","):
            qos_cpu_match = re.fullmatch(r"cpu=(\d+)", qos_tres.strip())
            if qos_cpu_match:
                qos_cpu_limits[qos_fields[0].strip()] = int(qos_cpu_match.group(1))
    partition_policies: Dict[str, Tuple[str, Optional[int]]] = {}
    for partition_line in partition_output.splitlines():
        partition_fields = dict(re.findall(r"(\w+)=(\S+)", partition_line))
        partition_name = partition_fields.get("PartitionName")
        if partition_name:
            partition_qos = clean_optional(partition_fields.get("QoS", ""))
            if partition_qos == "N/A" or partition_qos == "(null)":
                partition_qos = ""
            partition_policies[partition_name] = (
                partition_qos, qos_cpu_limits.get(partition_qos),
            )
    return partition_policies


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
    """Show live jobs first, then recent history, within the display limit."""
    history_limit = max(limit - len(active), 0)
    jobs = list(active.values())
    if history_limit:
        jobs.extend(load_recorded_jobs(conn, history_limit, active.keys()))
    jobs.sort(key=sort_key)
    return jobs[:limit]


def combine_jobs(active: Dict[str, Job], accounted: Dict[str, Job]) -> Dict[str, Job]:
    combined = dict(accounted)
    combined.update(active)
    return combined


def partition_sort_key(summary: PartitionSummary) -> Tuple[int, int, int, str]:
    availability_rank = 0 if summary.availability.lower() == "up" else 1
    return (
        availability_rank,
        -(summary.jobs_queued + summary.jobs_running),
        -summary.cpus_total if summary.cpus_available else 0,
        summary.partition,
    )


def load_partition_summaries(
    runner: CommandRunner,
    active_user_jobs: Dict[str, Job],
    user: str = "",
) -> Tuple[List[PartitionSummary], List[str]]:
    partitions, warnings = load_sinfo_partitions(runner)
    queue_counts, queue_warnings = load_partition_queue_counts(runner, user)
    warnings.extend(queue_warnings)
    partition_output, partition_error = run_command(build_partition_policies_args(), runner)
    qos_output, qos_error = run_command(build_qos_cpu_limits_args(), runner)
    warnings.extend(policy_error for policy_error in (partition_error, qos_error) if policy_error)
    partition_policies = parse_partition_cpu_policies(partition_output, qos_output)
    return combine_partition_summaries(
        partitions, queue_counts, active_user_jobs, partition_policies,
    ), warnings


def combine_partition_summaries(
    partitions: Dict[str, PartitionSummary],
    queue_counts: Optional[Dict[str, Dict[str, int]]],
    active_user_jobs: Dict[str, Job],
    partition_policies: Optional[Dict[str, Tuple[str, Optional[int]]]] = None,
) -> List[PartitionSummary]:
    for partition, counts in (queue_counts or {}).items():
        summary = partitions.setdefault(
            partition,
            PartitionSummary(partition=partition),
        )
        summary.jobs_queued = counts.get("queued", 0)
        summary.jobs_running = counts.get("running", 0)
        summary.jobs_other = counts.get("other", 0)

    for job in active_user_jobs.values():
        for partition in set(job.partition.split(",")):
            partition = normalize_partition(partition)
            summary = partitions.setdefault(partition, PartitionSummary(partition=partition))
            if job.category == "queued":
                summary.user_queued += 1
                pending_reason = job.reason.strip().strip("()") or "Unknown"
                summary.user_pending_reasons[pending_reason] = (
                    summary.user_pending_reasons.get(pending_reason, 0) + 1
                )
            elif job.category == "running":
                summary.user_running += 1

    partition_policies = partition_policies or {}
    qos_user_cpu_counts: Dict[str, int] = {}
    for partition, summary in partitions.items():
        summary.partition_qos, summary.qos_user_cpu_limit = partition_policies.get(
            partition, ("", None),
        )
        if queue_counts is not None:
            partition_counts = queue_counts.get(partition, {})
            summary.job_cpus_allocated = partition_counts.get("allocated_cpus", 0)
            summary.user_cpus_allocated = partition_counts.get("user_allocated_cpus", 0)
            qos_user_cpu_counts[summary.partition_qos] = (
                qos_user_cpu_counts.get(summary.partition_qos, 0) + summary.user_cpus_allocated
            )
    for summary in partitions.values():
        if summary.partition_qos and queue_counts is not None:
            summary.qos_user_cpus_allocated = qos_user_cpu_counts.get(summary.partition_qos, 0)

    partition_summaries = list(partitions.values())
    partition_summaries.sort(key=partition_sort_key)
    return partition_summaries


def poll_jobs_ssh_batch(
    conn: sqlite3.Connection,
    runner: CommandRunner,
    user: str,
    job_ids: Sequence[str],
    since_days: int,
    no_sacct: bool,
    limit: int,
) -> Tuple[List[Job], List[PartitionSummary], List[str]]:
    since = datetime.now() - timedelta(days=since_days)
    commands: List[Tuple[str, Sequence[str]]] = [
        ("active_jobs", build_squeue_jobs_args(user, job_ids)),
        ("sinfo", build_sinfo_partitions_args()),
        ("partition_queue", build_partition_queue_counts_args()),
        ("partition_policies", build_partition_policies_args()),
        ("qos_limits", build_qos_cpu_limits_args()),
    ]
    if not no_sacct:
        commands.insert(1, ("accounted_jobs", build_sacct_jobs_args(user, job_ids, since)))

    outputs, warnings = run_ssh_batch(runner, commands)
    active, active_warnings = parse_squeue_jobs_output(outputs.get("active_jobs", ""), user)
    warnings.extend(active_warnings)

    accounted: Dict[str, Job] = {}
    if not no_sacct:
        accounted, sacct_warnings = parse_sacct_jobs_output(
            outputs.get("accounted_jobs", ""),
            user,
        )
        warnings.extend(sacct_warnings)

    partitions, sinfo_warnings = parse_sinfo_partitions_output(outputs.get("sinfo", ""))
    warnings.extend(sinfo_warnings)
    queue_counts, queue_warnings = parse_partition_queue_counts_output(
        outputs.get("partition_queue", ""), user,
    )
    warnings.extend(queue_warnings)

    current = combine_jobs(active, accounted)
    record_jobs(conn, current.values())
    if (queue_warnings or "partition_queue" not in outputs
            or any("squeue failed" in warning or "batch failed" in warning for warning in warnings)):
        queue_counts = None
    partition_policies = parse_partition_cpu_policies(
        outputs.get("partition_policies", ""), outputs.get("qos_limits", ""),
    )
    partition_summaries = combine_partition_summaries(
        partitions, queue_counts, active, partition_policies,
    )
    return choose_display_jobs(conn, active, limit), partition_summaries, warnings


def poll_jobs(
    conn: sqlite3.Connection,
    runner: CommandRunner,
    user: str,
    job_ids: Sequence[str],
    since_days: int,
    no_sacct: bool,
    limit: int,
) -> Tuple[List[Job], List[PartitionSummary], List[str]]:
    if runner.backend == "ssh":
        return poll_jobs_ssh_batch(
            conn,
            runner,
            user,
            job_ids,
            since_days,
            no_sacct,
            limit,
        )

    since = datetime.now() - timedelta(days=since_days)
    active, warnings = load_squeue_jobs(runner, user, job_ids)
    accounted: Dict[str, Job] = {}
    if not no_sacct:
        accounted, sacct_warnings = load_sacct_jobs(runner, user, job_ids, since)
        warnings.extend(sacct_warnings)

    current = combine_jobs(active, accounted)
    record_jobs(conn, current.values())
    partitions, partition_warnings = load_partition_summaries(runner, active, user)
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
        "cpus_total": summary.cpus_total if summary.cpus_available else None,
        "cpus_allocated": summary.cpus_allocated if summary.cpus_available else None,
        "cpus_idle": summary.cpus_idle if summary.cpus_available else None,
        "cpus_other": summary.cpus_other if summary.cpus_available else None,
        "cpus_reserved_idle": summary.cpus_reserved_idle if summary.cpus_available else None,
        "cpus_available": summary.cpus_available,
        "cpu_load_text": summary.cpu_load_text,
        "job_cpus_allocated": summary.job_cpus_allocated,
        "user_cpus_allocated": summary.user_cpus_allocated,
        "partition_qos": summary.partition_qos,
        "qos_user_cpu_limit": summary.qos_user_cpu_limit,
        "qos_user_cpus_allocated": summary.qos_user_cpus_allocated,
        "user_cpu_load_text": summary.user_cpu_load_text,
        "jobs_queued": summary.jobs_queued,
        "jobs_running": summary.jobs_running,
        "jobs_other": summary.jobs_other,
        "user_queued": summary.user_queued,
        "user_running": summary.user_running,
        "user_pending_reasons": summary.user_pending_reasons,
    }


def build_partition_pool_summaries(
    partitions: Sequence[PartitionSummary],
) -> List[Dict[str, object]]:
    """Group connected node sets and count each physical node exactly once."""
    pool_members: List[List[PartitionSummary]] = []
    for partition in partitions:
        merged_members = [partition]
        merged_nodes = set(partition.node_cpu_counts)
        separate_pools: List[List[PartitionSummary]] = []
        for existing_members in pool_members:
            existing_nodes = {
                node_name for existing_partition in existing_members
                for node_name in existing_partition.node_cpu_counts
            }
            if merged_nodes & existing_nodes:
                merged_members.extend(existing_members)
                merged_nodes.update(existing_nodes)
            else:
                separate_pools.append(existing_members)
        separate_pools.append(merged_members)
        pool_members = separate_pools

    partition_pools: List[Dict[str, object]] = []
    partition_order = {partition.partition: index for index, partition in enumerate(partitions)}
    for pool_partitions in pool_members:
        pool_partitions.sort(key=lambda partition: partition.partition)
        pool_node_counts: Dict[str, Optional[Tuple[int, int, int, int, int]]] = {}
        pool_available = all(partition.cpus_available for partition in pool_partitions)
        for partition in pool_partitions:
            for node_name, node_counts in partition.node_cpu_counts.items():
                if node_name in pool_node_counts and pool_node_counts[node_name] != node_counts:
                    pool_available = False
                pool_node_counts[node_name] = node_counts
        pool_cpu_counts = [0, 0, 0, 0, 0]
        if pool_node_counts:
            for node_counts in pool_node_counts.values():
                if node_counts is None:
                    pool_available = False
                else:
                    pool_cpu_counts = [
                        pool_count + node_count
                        for pool_count, node_count in zip(pool_cpu_counts, node_counts)
                    ]
        else:
            # Unknown membership remains standalone, even when totals match.
            partition = pool_partitions[0]
            pool_cpu_counts = [partition.cpus_allocated, partition.cpus_idle,
                               partition.cpus_other, partition.cpus_total, partition.cpus_reserved_idle]
        partition_pools.append({
            "partitions": [partition.partition for partition in pool_partitions],
            "nodes_total": len(pool_node_counts) if pool_node_counts else pool_partitions[0].nodes_total,
            "membership_known": bool(pool_node_counts),
            "cpus_available": pool_available,
            **{
                cpu_name: cpu_count if pool_available else None
                for cpu_name, cpu_count in zip(
                    ("cpus_allocated", "cpus_idle", "cpus_other", "cpus_total", "cpus_reserved_idle"),
                    pool_cpu_counts,
                )
            },
        })
    partition_pools.sort(key=lambda pool: min(partition_order[name] for name in pool["partitions"]))
    return partition_pools


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
            f"{'MINE_Q':>6} {'MINE_R':>6} {'JOB_CPUS':>9} "
            f"{'MY_CPU/CAP':>12} {'POOL A/T':>13} {'LIMIT':>10}"
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
                f"{str(part.job_cpus_allocated) if part.job_cpus_allocated is not None else '-':>9} "
                f"{part.user_cpu_load_text:>12} "
                f"{part.cpu_load_text:>13} "
                f"{truncate(part.max_time, 10):>10}"
            )
            if part.user_pending_reasons:
                pending_reason_text = "; ".join(
                    f"{pending_reason}: {pending_count}"
                    for pending_reason, pending_count in sorted(part.user_pending_reasons.items())
                )
                print(f"  My pending reasons: {pending_reason_text}")
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
  --cpu-red: #d94b4b;
  --cpu-yellow: #f2c94c;
  --cpu-unallocated: #3b82d6;
  --cpu-reserved: #8b5cf6;
  --cpu-gray: #9aa4b2;
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
  grid-template-columns: repeat(20, 34px);
  grid-auto-rows: 34px;
  gap: 14px;
  align-items: center;
  overflow-x: auto;
  padding: 6px;
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
  gap: 16px;
  margin-bottom: 26px;
}
.pool-section {
  min-width: 0;
  padding: 20px;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: var(--panel);
}
.pool-head {
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
  align-items: baseline;
  gap: 8px 20px;
  margin-bottom: 16px;
}
.pool-title {
  margin: 0;
  font-size: 15px;
  font-weight: 650;
  overflow-wrap: anywhere;
}
.pool-capacity {
  color: var(--muted);
  font-size: 12px;
  font-variant-numeric: tabular-nums;
}
.pool-bar {
  display: flex;
  height: 20px;
  overflow: hidden;
  border-radius: 5px;
  background: var(--cpu-gray);
}
.pool-bar-segment { height: 100%; }
.pool-legend {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 20px;
  margin: 12px 0 18px;
  color: var(--muted);
  font-size: 12px;
  font-variant-numeric: tabular-nums;
}
.pool-legend-item {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}
.pool-swatch {
  width: 9px;
  height: 9px;
  border-radius: 2px;
}
.pool-partitions {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(min(100%, 210px), 1fr));
  gap: 10px;
}
.partition-item {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 6px 12px;
  min-width: 0;
  padding: 10px 12px;
  border: 1px solid var(--line);
  border-radius: 6px;
  color: inherit;
  background: var(--panel);
  cursor: pointer;
  font: inherit;
  text-align: left;
}
.partition-item strong { font-size: 13px; overflow-wrap: anywhere; }
.partition-item span { color: var(--muted); font-size: 12px; }
.partition-item .partition-personal { flex-basis: 100%; }
.partition-item:hover,
.partition-item:focus-visible {
  border-color: #9aa4b2;
  outline: 3px solid rgba(31, 41, 51, 0.12);
  outline-offset: 2px;
}
.partition-note {
  color: var(--muted);
  font-size: 12px;
  line-height: 1.6;
  margin: 0 0 14px;
}
.metric-label {
  display: block;
  color: var(--muted);
  font-size: 12px;
}
.metric-value {
  display: block;
  margin-top: 6px;
  font-size: 16px;
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
.cpu-chart-section {
  display: grid;
  grid-template-columns: 132px minmax(0, 1fr);
  gap: 16px;
  align-items: center;
  padding: 18px 22px;
  border-bottom: 1px solid var(--line);
}
.cpu-pie {
  position: relative;
  width: 132px;
  height: 132px;
  border-radius: 50%;
  background: var(--cpu-gray);
  box-shadow:
    inset 0 0 0 1px rgba(31, 41, 51, 0.14),
    0 8px 20px rgba(31, 41, 51, 0.12);
}
.cpu-pie::after {
  content: "";
  position: absolute;
  inset: 34px;
  border-radius: 50%;
  background: var(--panel);
  box-shadow: inset 0 0 0 1px rgba(31, 41, 51, 0.08);
}
.cpu-pie-total {
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
.cpu-legend {
  display: grid;
  gap: 8px;
  min-width: 0;
}
.cpu-legend-row {
  display: grid;
  grid-template-columns: 12px minmax(0, 1fr) auto;
  gap: 8px;
  align-items: center;
  font-size: 12px;
}
.cpu-swatch {
  width: 12px;
  height: 12px;
  border-radius: 3px;
  box-shadow: inset 0 0 0 1px rgba(31, 41, 51, 0.16);
}
.cpu-label {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.cpu-value {
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
    grid-template-columns: repeat(20, 32px);
    grid-auto-rows: 32px;
    gap: 12px;
  }
  .job-light {
    width: 32px;
    height: 32px;
  }
  .cpu-chart-section {
    grid-template-columns: 112px minmax(0, 1fr);
  }
  .cpu-pie {
    width: 112px;
    height: 112px;
  }
  .cpu-pie::after {
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
  <p class="partition-note">One bar per shared node pool; independent partitions have their own bar. Unallocated CPUs may still be constrained. Select a partition for your QoS cap and pending reasons.</p>
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
  <section class="cpu-chart-section" id="cpu-chart" hidden>
    <div class="cpu-pie" id="cpu-pie" aria-label="Shared node pool CPU allocation">
      <span class="cpu-pie-total" id="cpu-pie-total"></span>
    </div>
    <div class="cpu-legend" id="cpu-legend"></div>
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
const cpuChart = document.getElementById("cpu-chart");
const cpuPie = document.getElementById("cpu-pie");
const cpuPieTotal = document.getElementById("cpu-pie-total");
const cpuLegend = document.getElementById("cpu-legend");
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

function cpuSlices(partition) {
  if (!partition.cpus_available) return [];
  return [
    ["Allocated CPUs", Number(partition.cpus_allocated), "var(--cpu-yellow)"],
    ["Other unallocated CPUs", Number(partition.cpus_idle) - Number(partition.cpus_reserved_idle || 0), "var(--cpu-unallocated)"],
    ["Idle on reserved nodes", Number(partition.cpus_reserved_idle || 0), "var(--cpu-reserved)"],
    ["Other CPUs", Number(partition.cpus_other), "var(--cpu-red)"],
  ];
}

function cpuPieGradient(cpuSlices, cpuTotal) {
  if (!cpuTotal) return "var(--cpu-gray)";
  let cursor = 0;
  const cpuSegments = [];
  cpuSlices.forEach(([, value, color]) => {
    if (!value) return;
    const start = cursor;
    cursor += (value / cpuTotal) * 360;
    cpuSegments.push(`${color} ${start.toFixed(3)}deg ${cursor.toFixed(3)}deg`);
  });
  return cpuSegments.length
    ? `conic-gradient(${cpuSegments.join(", ")})`
    : "var(--cpu-gray)";
}

function renderCpuChart(partition) {
  const partitionCpuSlices = cpuSlices(partition);
  const cpuTotal = partitionCpuSlices.reduce((sum, [, value]) => sum + value, 0);
  cpuPie.style.background = cpuPieGradient(partitionCpuSlices, cpuTotal);
  cpuPieTotal.textContent = partition.cpus_available ? String(cpuTotal) : "-";
  cpuLegend.innerHTML = partitionCpuSlices.map(([label, value, color]) => `
    <div class="cpu-legend-row">
      <span class="cpu-swatch" style="background: ${color}"></span>
      <span class="cpu-label">${escapeHtml(label)}</span>
      <span class="cpu-value">${escapeHtml(value)} (${escapeHtml(percent(value, cpuTotal))})</span>
    </div>
  `).join("") || "CPU counts unavailable";
  cpuChart.hidden = false;
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
  cpuChart.hidden = true;
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
  detailSubtitle.textContent = `Partition QoS: ${text(partition.partition_qos)} · Chart: shared node pool, including jobs from other partitions`;
  renderCpuChart(partition);
  const pendingReasonEntries = Object.entries(partition.user_pending_reasons || {})
    .sort((firstReason, secondReason) => secondReason[1] - firstReason[1]);
  const pendingReasonText = pendingReasonEntries
    .map(([pendingReason, pendingCount]) => `${pendingReason}: ${pendingCount}`)
    .join("; ") || "No pending jobs in this query";
  const partitionFields = [
    ["Availability", partition.availability],
    ["Max Time", partition.max_time],
    ["Partition Job CPUs", partition.job_cpus_allocated],
    ["My CPUs in Partition", partition.user_cpus_allocated],
    ["Partition QoS", partition.partition_qos],
    ["My CPUs under This QoS", partition.qos_user_cpus_allocated],
    ["QoS CPU Cap per User", partition.qos_user_cpu_limit],
    ["My Pending Reasons", pendingReasonText],
    ["QoS Scope", "The CPU cap is per user across partitions with the same partition QoS. Job QoS or account limits may impose additional constraints."],
    ["Shared Pool Used / Total", partition.cpu_load_text],
    ["Shared Pool Unallocated", partition.cpus_idle],
    ["Idle on Reserved Nodes", partition.cpus_reserved_idle],
    ["Shared Pool Other", partition.cpus_other],
    ["Scheduling", "The shared pool is not exclusive to this partition. Unallocated CPUs may be reserved or fragmented; memory, priority, QoS and dependencies can prevent a job from starting."],
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
  details.innerHTML = partitionFields.map(([key, value]) =>
    `<dt>${escapeHtml(key)}</dt><dd>${escapeHtml(value)}</dd>`
  ).join("");
  detail.hidden = false;
  backdrop.hidden = false;
}

function closeDetail() {
  selectedJobId = null;
  selectedPartitionName = null;
  cpuChart.hidden = true;
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

function poolCpuSlices(pool, poolPartitions) {
  if (!pool.cpus_available) return [];
  const partitionColors = ["#d59a27", "#268c85", "#547bc5", "#c56b48", "#9868b3", "#6f8640"];
  const partitionCpuSum = poolPartitions.reduce((cpuSum, partition) => cpuSum + Number(partition.job_cpus_allocated || 0), 0);
  const partitionCpuSlices = [];
  if (poolPartitions.every(partition => partition.job_cpus_allocated != null)
      && partitionCpuSum <= Number(pool.cpus_allocated)) {
    poolPartitions.forEach((partition, partitionIndex) => {
      partitionCpuSlices.push([partition.partition, Number(partition.job_cpus_allocated), partitionColors[partitionIndex % partitionColors.length]]);
    });
    partitionCpuSlices.push(["Other allocated", Number(pool.cpus_allocated) - partitionCpuSum, "var(--cpu-yellow)"]);
  } else {
    // squeue and sinfo are separate snapshots: never stretch a bar past capacity.
    partitionCpuSlices.push(["Allocated (pool snapshot)", Number(pool.cpus_allocated), "var(--cpu-yellow)"]);
  }
  partitionCpuSlices.push(
    ["Unallocated", Number(pool.cpus_idle) - Number(pool.cpus_reserved_idle || 0), "var(--cpu-unallocated)"],
    ["Idle on reserved nodes", Number(pool.cpus_reserved_idle || 0), "var(--cpu-reserved)"],
    ["Unavailable / other", Number(pool.cpus_other), "var(--cpu-red)"],
  );
  return partitionCpuSlices.filter(([, cpuCount]) => cpuCount > 0);
}

function renderPartitions(partitionPayload) {
  partitions.replaceChildren();
  const partitionLookup = new Map((partitionPayload.partitions || []).map(partition => [partition.partition, partition]));
  const partitionPools = partitionPayload.partition_pools || (partitionPayload.partitions || []).map(partition => ({...partition, partitions: [partition.partition]}));
  partitionPools.forEach(pool => {
    const poolPartitions = pool.partitions.map(partitionName => partitionLookup.get(partitionName)).filter(Boolean);
    const poolSlices = poolCpuSlices(pool, poolPartitions);
    const poolTotal = Number(pool.cpus_total || 0);
    const poolSection = document.createElement("section");
    poolSection.className = "pool-section";
    const poolTitle = pool.partitions.join(" / ");
    const poolBarLabel = pool.cpus_available
      ? poolSlices.map(([cpuLabel, cpuCount]) => `${cpuLabel}: ${cpuCount}`).join("; ")
      : "CPU counts unavailable";
    poolSection.innerHTML = `
      <div class="pool-head">
        <h2 class="pool-title">${escapeHtml(poolTitle)}</h2>
        <span class="pool-capacity">${pool.partitions.length > 1 ? "Shared pool" : (pool.membership_known ? "Independent partition" : "Partition pool")} · ${escapeHtml(pool.nodes_total)} nodes · ${pool.cpus_available ? escapeHtml(poolTotal) : "-"} CPUs</span>
      </div>
      <div class="pool-bar" role="img" aria-label="${escapeHtml(poolBarLabel)}">
        ${poolSlices.map(([cpuLabel, cpuCount, cpuColor]) => `<span class="pool-bar-segment" style="width: ${poolTotal ? cpuCount / poolTotal * 100 : 0}%; background: ${cpuColor}" title="${escapeHtml(cpuLabel)}: ${escapeHtml(cpuCount)} CPUs (${percent(cpuCount, poolTotal)})"></span>`).join("")}
      </div>
      <div class="pool-legend">
        ${poolSlices.map(([cpuLabel, cpuCount, cpuColor]) => `<span class="pool-legend-item"><span class="pool-swatch" style="background: ${cpuColor}"></span>${escapeHtml(cpuLabel)} <strong>${escapeHtml(cpuCount)}</strong></span>`).join("") || (pool.cpus_available ? "No CPU capacity" : "CPU counts unavailable")}
      </div>`;
    const poolButtons = document.createElement("div");
    poolButtons.className = "pool-partitions";
    poolPartitions.forEach(partition => {
      const partitionButton = document.createElement("button");
      partitionButton.type = "button";
      partitionButton.className = "partition-item";
      partitionButton.title = `${partition.partition} · state ${text(partition.availability)} · View QoS and pending reasons`;
      partitionButton.innerHTML = `
        <strong>${escapeHtml(partition.partition)}</strong>
        <span>${escapeHtml(partition.job_cpus_allocated)} job CPUs</span>
        <span class="partition-personal">${partition.qos_user_cpu_limit == null ? "My CPUs" : "My CPUs / cap"} ${escapeHtml(partition.user_cpu_load_text)} · My queued ${escapeHtml(partition.user_queued)}</span>`;
      partitionButton.addEventListener("click", () => openPartitionDetail(partition));
      poolButtons.appendChild(partitionButton);
    });
    poolSection.appendChild(poolButtons);
    partitions.appendChild(poolSection);
  });
  if (selectedPartitionName && partitionLookup.has(selectedPartitionName)) {
    openPartitionDetail(partitionLookup.get(selectedPartitionName));
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
                "partition_pools": build_partition_pool_summaries(partitions),
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
    if platform.system().lower() == "windows":
        browser_host = "127.0.0.1" if host == "0.0.0.0" else host
        try:
            webbrowser.open(f"http://{browser_host}:{port}/")
        except webbrowser.Error as exc:
            print(f"Could not open browser: {exc}", file=sys.stderr)
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


def load_tracker_settings(tracker_config_path: Path) -> Dict[str, object]:
    """Read key=value startup settings; key.txt is not an SSH private key."""
    if not tracker_config_path.exists():
        return {}
    tracker_config = configparser.ConfigParser(interpolation=None)
    tracker_config.read_string(
        "[tracker]\n" + tracker_config_path.read_text(encoding="utf-8-sig")
    )
    tracker_setting_types = {
        "web": bool, "port": int, "host": str, "backend": str,
        "ssh_host": str, "ssh_user": str, "ssh_port": int, "ssh_key": str,
        "user": str, "limit": int, "interval": int, "since_days": int,
        "db": str,
    }
    tracker_settings: Dict[str, object] = {}
    for tracker_setting_name, tracker_setting_text in tracker_config["tracker"].items():
        if tracker_setting_name not in tracker_setting_types:
            raise ValueError(f"Unknown setting in key.txt: {tracker_setting_name}")
        tracker_setting_type = tracker_setting_types[tracker_setting_name]
        if tracker_setting_type is bool:
            tracker_settings[tracker_setting_name] = tracker_config.getboolean(
                "tracker", tracker_setting_name
            )
        elif tracker_setting_name == "ssh_port" and not tracker_setting_text:
            tracker_settings[tracker_setting_name] = None
        else:
            tracker_settings[tracker_setting_name] = tracker_setting_type(tracker_setting_text)
    if tracker_settings.get("backend", "auto") not in {"auto", "local", "ssh"}:
        raise ValueError("key.txt backend must be auto, local or ssh")
    for tracker_path_setting in ("ssh_key", "db"):
        if tracker_settings.get(tracker_path_setting):
            tracker_setting_path = Path(str(tracker_settings[tracker_path_setting])).expanduser()
            if not tracker_setting_path.is_absolute():
                tracker_setting_path = tracker_config_path.parent / tracker_setting_path
            tracker_settings[tracker_path_setting] = str(tracker_setting_path)
    return tracker_settings


def build_parser() -> argparse.ArgumentParser:
    is_windows = platform.system().lower() == "windows"
    tracker_directory = Path(__file__).resolve().parent
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
        default=os.environ.get(
            "SLURM_TRACKER_SSH_HOST", "lsr@sycamore.unc.edu" if is_windows else ""
        ),
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
        default=str(tracker_directory / "slurm_jobs.sqlite3") if is_windows else "slurm_jobs.sqlite3",
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
        default=200,
        help="Maximum jobs to display, with queued/running jobs first (default: 200).",
    )
    parser.add_argument(
        "--web",
        action="store_true",
        default=is_windows,
        help="Run the local browser UI instead of the terminal table.",
    )
    parser.add_argument(
        "--terminal",
        action="store_false",
        dest="web",
        help="Use the terminal table (Windows defaults to the browser UI).",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host for --web.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0 if is_windows else 8765,
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
    parser.set_defaults(**load_tracker_settings(tracker_directory / "key.txt"))
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
        ssh_key_path = clean_optional(args.ssh_key)
        if ssh_key_path:
            try:
                ssh_key_file = Path(ssh_key_path).expanduser().resolve()
                if not ssh_key_file.read_bytes().strip():
                    return None, f"SSH key file is empty: {ssh_key_file}. Put your SSH private key in this file."
                ssh_key_path = str(ssh_key_file)
            except OSError as exc:
                return None, f"Cannot read SSH key file: {exc}"
        return (
            CommandRunner(
                backend="ssh",
                ssh_host=ssh_host,
                ssh_user=ssh_user,
                ssh_port=args.ssh_port,
                ssh_key=ssh_key_path,
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
    try:
        args = build_parser().parse_args(argv)
    except (OSError, ValueError, configparser.Error) as exc:
        print(f"Cannot load key.txt configuration: {exc}", file=sys.stderr)
        return 2
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
    if args.web and not args.once:
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
    tracker_exit_code = main()
    if tracker_exit_code and platform.system().lower() == "windows" and len(sys.argv) == 1:
        try:
            input("Press Enter to close...")
        except EOFError:
            pass
    raise SystemExit(tracker_exit_code)
