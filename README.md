# Slurm Tracker

`slurm_tracker.py` is a zero-dependency Python monitor for Slurm jobs. It shows live job status, records status changes in a local SQLite database, and provides both a terminal view and a local Web UI.

Status lights:

- White: running, for example `RUNNING`
- Green: completed, for example `COMPLETED`
- Yellow: queued, for example `PENDING`
- Red: ended with an error, for example `FAILED`, `TIMEOUT`, `CANCELLED`, or `OUT_OF_MEMORY`

## Run

```bash
python3 slurm_tracker.py
```

By default, the tracker refreshes every 10 seconds, monitors the current user, displays up to 200 jobs, and writes a local database in the current directory:

```text
slurm_jobs.sqlite3
```

Display order:

- Queued jobs first
- Running jobs next
- Finished jobs sorted by end time, newest first

## Web UI

Start the local browser UI:

```bash
./slurm_tracker.py --web
```

Default URL:

```text
http://127.0.0.1:8765/
```

The main job view is a grid of status lights. Click a light to open job details.

The top of the page shows one horizontal allocation bar per shared node pool. Partitions with overlapping nodes are grouped together, including partial overlaps; independent partitions each get their own bar. Membership comes from `sinfo -N`, and each physical node is counted once. Equal capacities or equal QoS names do not imply a shared pool.

Colored segments show each partition's job allocation, other allocations, unallocated CPUs, idle CPUs on reserved nodes, and unavailable capacity. Since `squeue` and `sinfo` are separate snapshots, a temporary excess of job CPU counts falls back to a single pool-allocation segment rather than stretching the bar beyond capacity. Clickable partition buttons below each bar retain:

- `Partition job CPUs`: CPUs assigned to active jobs submitted to this partition, from `squeue`; pending requests and suspended jobs are excluded
- `My CPUs / QoS cap`: your allocation across partitions sharing the partition QoS / its per-user CPU cap. Without a reported cap, `My CPUs` shows your allocation in this partition
- `My queued`: your pending jobs in the current query
- Partition state is available in the button tooltip and details; `up` does not guarantee that a job can start

Cards use per-job allocations from `squeue --array`, expanding job arrays for counting. Partition QoS is read from `scontrol show partition -o`, and its per-user CPU cap comes from `sacctmgr show qos` (`MaxTRESPU`), without hardcoded cluster limits. A job's own QoS may be `normal` while a partition QoS also applies. Job QoS and account limits can impose additional constraints. Failed CPU queries display as `-`.

The [UNC Sycamore partition guide](https://help.rc.unc.edu/sycamore-partitions) defines `small maxcpu=192` as the sum across one user's jobs, not the total CPU capacity for all users in the partition. For example, using 72 CPUs and requesting another 128 exceeds that cap (200 > 192), so a job may wait with `QOSMaxCpuPerUserLimit` despite idle CPUs in the shared node pool.

The details' shared node pool comes from `sinfo %C`. It may include allocations by jobs submitted to other partitions. Partitions sharing nodes do not have independent capacities, so their pool totals must not be added together. A CPU is Slurm's configured core/thread scheduling unit, not live CPU utilization. Terminal columns distinguish `JOB_CPUS` (partition jobs), `MY_CPU/CAP` (your usage / partition QoS cap), and `POOL A/T` (shared pool allocated / total).

Each group shows shared capacity only once. Click a partition button for its pool pie chart, full CPU/node counts, and global/personal job counts. `My Pending Reasons` groups the current query's pending jobs by `squeue %R`, such as `Priority` or `Dependency`. Unallocated CPUs can be constrained by fragmentation, memory, reservations, time limits, QoS, priority, or dependencies. Spare totals alone cannot establish whether a job can start.

CPU-allocation colors:

- Yellow: allocated CPUs
- Blue: other unallocated CPUs (not guaranteed schedulable)
- Purple: idle CPUs on reserved nodes, a subset of total idle CPUs
- Red: other CPUs
- Gray background: no CPU capacity or unavailable counts

## Configuration and direct startup

The script automatically reads `key.txt` beside `slurm_tracker.py` as `key = value` settings:

```ini
web = true
port = 0
ssh_host = sycamore.unc.edu
ssh_user = lsr
user = lsr
limit = 200
interval = 10
ssh_key =
```

Run `python slurm_tracker.py` to use these settings. On Windows, double-clicking the script also starts the web UI and opens the browser; Python file association and OpenSSH are required.

`key.txt` contains configuration, not private key contents. Leave `ssh_key` empty to use default SSH keys or the agent, or set it to a private key file path. Relative paths in the configuration are resolved beside the script. Verify the SSH host fingerprint in a terminal before the first connection; load encrypted keys into the agent first.

Precedence: command-line options, then `key.txt`, then environment/built-in defaults. Use `--terminal` for the terminal UI or `--once` for a single query. On Windows the database defaults to the script directory.

The display defaults to at most 200 jobs in 20 columns (10 rows when full), with horizontal scrolling on narrow screens. Queued/running jobs come first, followed by recent history from the database and the last 7 days of `sacct`. Use `--since-days 30` to query further back.

## Windows With SSH Login Node

On Windows, the script can run locally and query Slurm through SSH on a login node. It executes `squeue`, `sacct`, and `sinfo` remotely, then stores the results in the Windows-local `slurm_jobs.sqlite3`.

First verify that SSH can run Slurm commands from PowerShell or Command Prompt:

```powershell
ssh your_user@login-node.example.edu squeue -u your_user
```

Start the Web UI:

```powershell
python slurm_tracker.py --web --ssh-host login-node.example.edu --ssh-user your_user --user your_user
```

If Windows blocks or reserves the default port, let the OS choose a free port:

```powershell
python slurm_tracker.py --web --port 0 --ssh-host your_user@login-node.example.edu --user your_user
```

The script prints the actual URL, for example:

```text
http://127.0.0.1:51234/
```

UNC Sycamore examples:

```powershell
python slurm_tracker.py --web --port 0 --ssh-host sycamore.unc.edu --ssh-user lsr --user lsr
```

or:

```powershell
python slurm_tracker.py --web --port 0 --ssh-host lsr@sycamore.unc.edu --user lsr
```

Do not use `sycamore@unc.edu`; that is parsed as a `user@host` value and points to the wrong host.

You can also configure SSH with environment variables:

```powershell
$env:SLURM_TRACKER_SSH_HOST = "login-node.example.edu"
$env:SLURM_TRACKER_SSH_USER = "your_user"
$env:SLURM_TRACKER_USER = "your_user"
python slurm_tracker.py --web
```

If you need a specific private key:

```powershell
python slurm_tracker.py --web --ssh-host login-node.example.edu --ssh-user your_user --ssh-key C:\Users\you\.ssh\id_ed25519
```

SSH uses `BatchMode=yes` by default, which is best for key-based login. If key-based login is not set up yet, configure it first in your terminal. Avoid relying on interactive password prompts while the Web UI is polling.

In SSH mode, each refresh uses one SSH connection for job, node, and QoS queries (`squeue`, `sacct`, `sinfo`, `scontrol`, and `sacctmgr`). If the login node still occasionally reports `Connection closed`, increase the refresh interval:

```powershell
python slurm_tracker.py --web --port 0 --interval 30 --ssh-host sycamore.unc.edu --ssh-user lsr --user lsr
```

## Useful Commands

Poll once, write the local database, print output, then exit:

```bash
python3 slurm_tracker.py --once
```

Change refresh interval:

```bash
python3 slurm_tracker.py --interval 5
```

Monitor one job:

```bash
python3 slurm_tracker.py --job-id 123456
```

Show more jobs:

```bash
python3 slurm_tracker.py --limit 50
```

Specify user or database path:

```bash
python3 slurm_tracker.py --user "$USER" --db ./slurm_jobs.sqlite3
```

If `sacct` is unavailable on the cluster, use only `squeue`:

```bash
python3 slurm_tracker.py --no-sacct
```

Note: with `--no-sacct`, completed or failed jobs may disappear once they leave `squeue`. Reliable completed/error history depends on `sacct`.

## Local History

Inspect the current job table:

```bash
sqlite3 slurm_jobs.sqlite3 'select job_id,state,light,last_seen from jobs order by last_seen desc limit 20;'
```

Inspect status-change events:

```bash
sqlite3 slurm_jobs.sqlite3 'select job_id,old_state,new_state,new_light,seen_at from events order by seen_at desc limit 20;'
```

## Troubleshooting

If the Web page opens but shows no data, open the JSON endpoint printed by the server:

```text
http://127.0.0.1:PORT/api/jobs
```

Check the `warnings` or `error` fields.

If the backend line shows the wrong host, stop the server and verify your SSH options. For Sycamore it should look like:

```text
Backend: ssh:lsr@sycamore.unc.edu
```
