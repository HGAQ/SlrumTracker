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

默认每 10 秒刷新一次，监控当前用户，最多显示 200 个任务，并在当前目录写入：

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

页面上方还会显示各个 partition 的当前状态：

- `Partition job CPUs`：通过 `squeue` 汇总实际提交到该 partition 的活动作业占用核数，不包括排队请求和已暂停作业
- `My CPUs / QoS cap`：你在同一 partition QoS 下的占用核数 / 每用户核数上限；多个 partition 共用 QoS 时合并你的占用。查不到上限时显示 `My CPUs`，仅统计该分区
- `My queued`：本次查询中你的排队任务数
- `State up`：来自 `sinfo` 的 partition 状态，不保证任务能立即启动

卡片以 `squeue --array` 的逐作业 CPU 分配为准，展开任务数组计数。QoS 名称从 `scontrol show partition -o` 获取，每用户 CPU 上限从 `sacctmgr show qos` 的 `MaxTRESPU` 获取，不硬编码集群上限。作业自身的 QoS 可能是 `normal`，与 partition QoS 不同；其他作业 QoS 或账户限制仍可能生效。查询失败时对应 CPU 数据显示 `-`。

[UNC Sycamore 分区说明](https://help.rc.unc.edu/sycamore-partitions)中的 `small maxcpu=192` 是**每用户所有作业合计**的上限，不是该分区所有用户总共只有 192 核。例如你已占用 72 核，再启动 128 核作业将达到 200 核，可能因 `QOSMaxCpuPerUserLimit` 等待，即使物理池还有 idle CPU。

详情中的共享节点池统计来自 `sinfo %C`；多个 partition 可以共用节点，池中已分配 CPU 也可能属于其他分区的作业。不能将这些 partition 的容量相加当作集群总容量。CPU 是 Slurm 的调度单位（物理核或硬件线程），不表示实时 CPU 使用率。终端的 `JOB_CPUS` 表示该分区作业占用，`MY_CPU/CAP` 表示你的占用/partition QoS 上限，`POOL A/T` 才是物理节点池已分配/总数。

点击任意 partition 摘要会展开 CPU 明细、全局队列数、你的任务数和 `My Pending Reasons`（当前查询返回的等待原因及数量），同时保留 allocated/mixed/down/draining 节点数。等待原因直接来自 `squeue %R`：例如 `Priority` 表示优先级等待，`Dependency` 表示依赖尚未满足。未分配 CPU 可能分散在不同节点或受预留、内存、时限、QoS 等约束；不能仅根据 CPU 总余量判断某个作业能否启动。

partition 详情里会显示共享节点池的 CPU 分配饼图：

- 黄色：allocated CPU
- 蓝色：其他未分配 CPU（仍不保证可调度）
- 紫色：reserved 状态节点上的未分配 CPU，是总 idle 的子集
- 红色：other CPU
- 灰色底图：无 CPU 容量或统计不可用

主页面卡片只保留三个关键数字，增加留白；点击卡片查看饼图、图例和完整明细。

## 配置文件与直接启动

脚本自动读取同目录的 `key.txt`，内容为 `key = value` 格式的启动配置：

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

直接运行 `python slurm_tracker.py` 即相当于原来的 `python slurm_tracker.py --web --port 0 --ssh-host sycamore.unc.edu --ssh-user lsr --user lsr`。Windows 安装 Python 并关联 `.py` 后，也可双击启动并自动打开浏览器；需要 Windows OpenSSH。

`key.txt` 存放配置，不存放私钥内容。`ssh_key` 留空时沿用 SSH 默认密钥或 agent；如需指定私钥，在这一项填写私钥文件路径。配置中的相对路径以脚本目录为基准。首次连接前通过终端 SSH 确认主机指纹，加密私钥需预先加载到 SSH agent。

配置文件覆盖环境变量和内置默认值，命令行参数优先级最高。`--terminal` 切回终端模式，`--once` 仅查询一次。Windows 数据库默认保存在脚本同目录。

默认最多 200 个任务，每行 20 个，满 200 个时为 10 行；窄窗口横向滚动。排队和运行任务优先，其余显示最近结束的任务。历史来自本地数据库和默认最近 7 天的 `sacct`，可通过 `--since-days 30` 扩大范围。

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

UNC Sycamore 示例：

```powershell
python slurm_tracker.py --web --port 0 --ssh-host sycamore.unc.edu --ssh-user lsr --user lsr
```

或者：

```powershell
python slurm_tracker.py --web --port 0 --ssh-host lsr@sycamore.unc.edu --user lsr
```

不要把主机写成 `sycamore@unc.edu`；那会被解析成错误的 `user@host` 形式。

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

SSH 模式下，每次刷新只会建立一个 SSH 连接，并在登录节点上执行作业、节点和 QoS 查询（`squeue`、`sacct`、`sinfo`、`scontrol`、`sacctmgr`）。如果登录节点仍然偶尔报 `Connection closed`，可以把刷新间隔调大：

```powershell
python slurm_tracker.py --web --port 0 --interval 30 --ssh-host sycamore.unc.edu --ssh-user lsr --user lsr
```

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
