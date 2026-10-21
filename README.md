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

The top of the page also shows a partition summary:

- `Allocated CPUs`: allocated CPUs / total CPUs
- `Unallocated`: idle CPUs reported by Slurm, including spare CPUs on mixed nodes; not necessarily available to your jobs
- `My queued`: your pending jobs in the current query
- `State up`: the partition state from `sinfo`, not a guarantee that a job can start

CPU counts come directly from `sinfo %C` (allocated/idle/other/total), summed per partition, rather than multiplying occupied nodes by cores per node. A CPU is the unit configured in Slurm: a physical core or a hardware thread, depending on the cluster. This measures allocation, not live CPU utilization. Partitions can share nodes; adding their capacities does not give the cluster's total capacity. The terminal partition table also uses CPU counts (`CPUS A/T` means allocated / total; `UNALLOC` means unallocated). Missing or incomplete CPU counts display as `-`.

Cards use a thin CPU-allocation bar and three key metrics with more spacing. Click a card to open the pie chart, legend, full CPU and node counts, and global/personal job counts. `My Pending Reasons` groups the current query's pending jobs by the reason reported by `squeue %R`, such as `Priority` or `Dependency`. Unallocated CPUs may be fragmented across nodes or constrained by memory, reservations, time limits, QoS, priority, or dependencies. Spare CPU totals alone cannot establish whether a specific job can start.

CPU-allocation colors:

- Yellow: allocated CPUs
- Blue: unallocated CPUs
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

In SSH mode, each refresh uses one SSH connection and runs `squeue`, `sacct`, and `sinfo` together on the login node. If the login node still occasionally reports `Connection closed`, increase the refresh interval:

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
