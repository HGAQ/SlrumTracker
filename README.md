# Slurm Tracker

本目录里的 `slurm_tracker.py` 是一个零依赖 Python 监控脚本，用来实时查看 Slurm 任务状态，并把状态变化记录到本地 SQLite 数据库。

状态灯规则：

- 白灯：运行中，例如 `RUNNING`
- 绿灯：运行完成，例如 `COMPLETED`
- 黄灯：排队中，例如 `PENDING`
- 红灯：已结束但报错，例如 `FAILED`、`TIMEOUT`、`CANCELLED`、`OUT_OF_MEMORY`

## 运行

```bash
python3 slurm_tracker.py
```

默认每 10 秒刷新一次，监控当前用户，显示 20 个任务，并在当前目录写入：

```text
slurm_jobs.sqlite3
```

显示顺序：

- 当前排队任务优先
- 当前运行任务其次
- 已结束任务按结束时间从新到旧排序

## Web UI

启动本地浏览器界面：

```bash
./slurm_tracker.py --web
```

默认地址：

```text
http://127.0.0.1:8765/
```

主界面只显示任务状态灯，点击某个灯会显示该任务详情。

## Windows 远程查询登录节点

在 Windows 上运行时，脚本会检测系统；如果配置了 SSH 登录节点，会通过 SSH 到登录节点执行 `squeue` 和 `sacct`，然后把结果记录在 Windows 本地的 `slurm_jobs.sqlite3`。

先确认 Windows 终端里能登录节点并执行 Slurm 命令：

```powershell
ssh your_user@login-node.example.edu squeue -u your_user
```

启动 Web UI：

```powershell
python slurm_tracker.py --web --ssh-host login-node.example.edu --ssh-user your_user --user your_user
```

如果 Windows 报端口权限或占用问题，直接让系统选择空闲端口：

```powershell
python slurm_tracker.py --web --port 0 --ssh-host your_user@login-node.example.edu --user your_user
```

脚本会在终端打印实际访问地址，例如 `http://127.0.0.1:51234/`。

也可以用环境变量，之后直接运行脚本：

```powershell
$env:SLURM_TRACKER_SSH_HOST = "login-node.example.edu"
$env:SLURM_TRACKER_SSH_USER = "your_user"
$env:SLURM_TRACKER_USER = "your_user"
python slurm_tracker.py --web
```

如果需要指定私钥：

```powershell
python slurm_tracker.py --web --ssh-host login-node.example.edu --ssh-user your_user --ssh-key C:\Users\you\.ssh\id_ed25519
```

默认 SSH 使用 `BatchMode=yes`，适合已经配置好免密登录的情况。如果还没配置免密，可以先在终端里完成 SSH key 配置；不建议让 Web UI 轮询时等待密码输入。

只试跑一次并记录本地数据库：

```bash
python3 slurm_tracker.py --once
```

调整刷新间隔：

```bash
python3 slurm_tracker.py --interval 5
```

只看某个任务：

```bash
python3 slurm_tracker.py --job-id 123456
```

显示更多任务：

```bash
python3 slurm_tracker.py --limit 50
```

指定用户或数据库路径：

```bash
python3 slurm_tracker.py --user "$USER" --db ./slurm_jobs.sqlite3
```

如果集群暂时不能用 `sacct`，可以只用 `squeue`：

```bash
python3 slurm_tracker.py --no-sacct
```

注意：只用 `squeue` 时，已经完成或失败并从队列里消失的任务可能无法被识别；完整的完成/失败记录依赖 `sacct`。

## 查看本地记录

查看当前任务表：

```bash
sqlite3 slurm_jobs.sqlite3 'select job_id,state,light,last_seen from jobs order by last_seen desc limit 20;'
```

查看状态变化历史：

```bash
sqlite3 slurm_jobs.sqlite3 'select job_id,old_state,new_state,new_light,seen_at from events order by seen_at desc limit 20;'
```
