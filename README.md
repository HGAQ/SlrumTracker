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

By default, the tracker refreshes every 10 seconds, monitors the current user, displays 20 recent jobs, and writes a local database in the current directory:

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

- `Queue`: all queued jobs in that partition
- `Run`: all running jobs in that partition
- `Mine`: your queued / running jobs
- `Nodes`: allocated or mixed nodes / total nodes
- `Idle`: idle nodes
- `Down`: down or draining nodes
- `Avail up`: partition availability from `sinfo`; `up` means the partition is currently available

Each partition card includes a small node-state pie chart. Click a partition card to open a larger pie chart, legend, and full details.

Node-state colors:

- Red: `down` and `draining`
- Yellow: `allocated`
- Blue: `mixed`
- Green: `idle`
- Gray: `other`

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
