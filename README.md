# TJU_TCP 课程实践项目

## 1. 项目简介

本项目是《计算机网络》课程实践的 TJU_TCP 实现，基于课程提供的 UDP 仿真内核，在 Vagrant 双虚拟机环境中实现一个教学简化版的 TCP。当前版本包含：

- 连接建立与关闭（三次握手、四次挥手、TIME-WAIT）；
- 可靠数据传输：序号/确认号、累计确认、超时重传、乱序缓存；
- 流量控制：接收窗口 `rwnd`、发送窗口更新、SWS 避免；
- 基础 Reno 拥塞控制：慢启动、拥塞避免、三次重复 ACK 基本快速重传、RTO 超时降窗与指数退避；
- RTT/RTO 估计：按 RFC 6298 维护，重传报文按 Karn 算法排除采样；
- 第三阶段性能实验：丢包率与时延两组受控对照实验，每组重复 `n=3`，共 24 轮有效数据；
- NewReno 选做任务：依据 RFC 6582 实现快速恢复和 partial ACK 处理，通过 `NR-01` 与基础 Reno 对比。

当前未实现：SACK/RACK、CUBIC、RACK-TLP。NewReno 仅在选做任务和独立分支/提交中说明，不改变基础 Reno 必做部分的边界。

## 2. 目录结构

```text
tju_tcp/
├── README.md                    # 项目说明
├── inc/                         # 协议头文件
│   ├── tju_tcp.h
│   ├── tju_core.h
│   ├── congestion_control.h
│   ├── flow_control.h
│   ├── rtt.h
│   ├── timer.h
│   ├── kernel.h
│   ├── queue.h
│   ├── tju_packet.h
│   ├── trace.h
│   └── global.h
├── src/                         # 协议实现
│   ├── tju_tcp.c
│   ├── tju_core.c               # 连接管理、可靠传输、发送/接收主流程
│   ├── congestion_control.c     # 基础 Reno
│   ├── flow_control.c           # 发送窗口与 SWS 避免
│   ├── rtt.c                    # RTT/RTO 估计与 Karn 算法
│   ├── timer.c                  # 重传定时器
│   ├── kernel.c                 # UDP 仿真内核
│   ├── queue.c                  # 发送/接收缓冲队列
│   ├── tju_packet.c             # TJU_TCP 报文封装与解析
│   ├── trace.c                  # 事件 Trace 写入
│   └── client.c / server.c      # 框架入口
├── Makefile                     # 顶层构建
└── test/
    ├── Makefile                 # 测试程序构建
    ├── test_rdt_client.c        # 客户端发送测试
    ├── test_rdt_server.c        # 服务端接收测试
    ├── test_client.c / test_server.c / test_close_client.c
    ├── test_congestion.py       # 单轮运行：tcset + 起停进程 + 抓包 + 绘图
    ├── gen_graph_win.py         # Trace → 窗口/RTT/吞吐率图
    ├── gen_graph_seq.py         # 序号推进图
    ├── run_perf_sweep.py        # 24 轮对照实验：重复、归档、统计、汇总
    ├── rdt_send_file.txt        # 测试输入数据
    ├── pack.sh                 # 课程评分 handin.zip 打包脚本
    ├── pack_test.sh            # test/evidence 包打包脚本
    └── evidence/               # 归档的原始证据与图表
        ├── T-01-delay20ms-loss3pct/  # 最终报告 T-01 的证据
        ├── perf-20260917-103605/     # 24 轮性能实验
        └── newreno-comparison/       # NR-01 基础 Reno/NewReno 对比
```

## 3. 构建与运行

```bash
cd /vagrant/tju_tcp
make
cd test
make
```

单轮 T-01（基础 Reno 测试）：

```bash
sudo tcset enp0s8 --delay 20ms --loss 3% --overwrite
./rdt_server      # Server VM
./rdt_client      # Client VM
```

> **T-01 测试方法说明。** T-01 的原始证据由上面的手工单轮流程生成（`test/evidence/T-01-delay20ms-loss3pct/test-config.txt` 记录了实际命令）。`test_congestion.py` 是同一类单轮测试的自动化运行方式，可用 `python3 test_congestion.py 100 20 0 3` 运行；它会在两端执行 tcset（`--rate 100Mbps --delay-distro 0`），采用 6 s 建连 + 60 s 等待的时序，因此与 T-01 原始手工证据的部署和时序不同，吞吐率数值可能不同（例如报告的 T-01 为 0.879 Mbps，而统一部署下的 A2 组为 0.583 Mbps）。`test_congestion.py` 主要作为 24 轮对照实验的单轮运行器，不应视为 T-01 的精确复现。

24 轮对照实验（在 Client VM 上运行）：

```bash
cd /vagrant/tju_tcp/test
python3 run_perf_sweep.py --dry-run
python3 run_perf_sweep.py --repeats 3
```

> `run_perf_sweep.py` 只负责编排、归档、统计与汇总；单轮网络设置、运行和出图仍由 `test_congestion.py`、`gen_graph_win.py`、`gen_graph_seq.py` 完成。

## 4. 性能实验

### 4.1 实验设计

- 共同固定：带宽 100 Mbps、抖动 0 ms、`SMSS=1380 B`、收发缓冲 `5000×SMSS`、单次应用写入 `10000 B`、源数据 50 MB、同一代码提交。
- 实验 A（丢包率）：固定单向时延 20 ms，丢包率 0/1/3/5%。
- 实验 B（时延）：固定丢包率 1%，单向时延 20/50/100/200 ms。
- 每组重复 `n=3`，轮次交错执行，共 24 轮；本次 24/24 轮有效。

### 4.2 吞吐率口径

- t0 = `client.event.trace` 中第一条数据 `SEND`（`flag=0, length>0`）的时间戳；
- 观测窗口 = `[t0, t0+55s)`；
- `G = ΣDELV.size × 8 / 55`（bps）；`DELV` 是服务端按序交付给应用层的字节数，即应用层有效吞吐率；
- 若数据在窗口内提前发完，则改用完成时间口径 `G = 总交付字节 × 8 / (末个 DELV - t0)`，并在 `summary.csv` 标记 `metric=active` / `early_finish=True`；
- 本次 24 轮全部为 `metric=window`、`window_covered=True`、`early_finish=False`。

### 4.3 主要结果

- 丢包率 0/1/3/5%：均值 `1.283/1.030/0.583/0.360 Mbps`；下降最陡区间为 1%→3%（均值 −43.5%）。
- 单向时延 20/50/100/200 ms：均值 `1.049/0.750/0.521/0.347 Mbps`；200 ms 档位最稳定（标准差 0.028），转为 RTT/窗口限制区间。
- 主要瓶颈：基础 Reno 不支持 NewReno/SACK，多缺口时依赖串行 RTO 恢复；不是带宽或 `rwnd` 限制。

### 4.4 NewReno 选做对比

NewReno 已在选做分支中实现，并使用 `NR-01` 与基础 Reno 进行同参数对比。结果、原始 Trace、pcap 和对比图归档在 `test/evidence/newreno-comparison/NR-01-20260921-024944/`。该对比目前为单轮结果，用于验证 partial ACK 恢复机制和说明趋势，不作统计显著性结论。

## 5. test/evidence 目录说明

### 5.1 T-01-delay20ms-loss3pct/

最终报告第七部分的基础 Reno 测试用例为 **T-01**；工作区目录为 `T-01-delay20ms-loss3pct`。本目录包含 T-01 单轮机制验证（20 ms / 3%）的证据。注意：第六部分的 R-01/R-02 是可靠数据传输测试，与本目录无关。

| 文件 | 说明 |
| --- | --- |
| `client.event.trace` | 客户端事件 Trace：SEND、RECV、CWND、RWND、SWND、RTTS |
| `server.event.trace` | 服务端事件 Trace：RECV、DELV、RWND、SEND ACK |
| `client_CongestionWindowSize_VS_Time.png` | 报告图 7-3：cwnd 与慢启动/拥塞避免/快速重传/RTO 事件 |
| `client_RTT_VS_Time.png` | 报告图 7-4：SampleRTT、EstimatedRTT、DeviationRTT、TimeoutInterval |
| `client_AllWindowSize_VS_Time.png` | 报告图 7-2：Client 端所有窗口大小随时间变化 |
| `client_SendWindowSize_VS_Time.png` | 报告图 7-5：Client 端发送窗口随时间变化 |
| `client_ReceiveWindowSize_VS_Time.png` | 接收窗口辅助图 |
| `server_*.png` | 服务端窗口与吞吐率辅助图 |
| `SeqNum_VS_Time.png` | 报告图 7-6：Client 端 Seq_num 随时间变化 |
| `test-config.txt` | T-01 参数与测试配置记录 |

### 5.2 perf-20260917-103605/

24 轮丢包率/时延对照实验（2026-09-17 10:36:05 批次）：

批次级文件：

| 文件 | 说明 |
| --- | --- |
| `batch-config.txt` | 参数、执行顺序、commit、两端时钟 |
| `summary.csv` | 每轮一行：G、metric、快速重传次数、RTO 超时次数、有效性 |
| `summary_by_condition.csv` | 每组：n、mean/std/median/min/max |
| `report_throughput_vs_loss.png` | 报告图 8-1：丢包率-吞吐率汇总图 |
| `report_throughput_vs_delay.png` | 报告图 8-2：时延-吞吐率汇总图 |
| `throughput_vs_loss.png` / `throughput_vs_delay.png` | 备选纯趋势图 |

每个条件目录（如 `A-delay20ms-loss0pct`、`B-delay50ms-loss1pct`）下每次重复一个 `rN/`：

| 文件 | 说明 |
| --- | --- |
| `client.event.trace` | 本轮客户端原始 Trace |
| `server.event.trace` | 本轮服务端原始 Trace |
| `metrics.json` | 本轮吞吐率、口径、快速重传/RTO 次数、有效性 |
| `run-info.txt` | 本轮参数与结果的可读摘要 |

### 5.3 newreno-comparison/NR-01-20260921-024944/

NR-01 对比实验保存基础 Reno 和 NewReno 两个版本的同参数证据：

| 路径 | 说明 |
| --- | --- |
| `baseline/r01/` | 基础 Reno 原始 Trace、pcap、图表、metrics 和源码哈希 |
| `newreno/r01/` | NewReno 原始 Trace、pcap、图表、metrics 和源码哈希 |
| `comparison.csv` | 各对比指标的结构化结果 |
| `comparison.md` | 可读的对比说明 |
| `comparison/newreno_vs_reno.png` | 报告用对比图 |
| `comparison/newreno_vs_reno.svg` | 对比图矢量版本 |

### 5.4 Trace 格式

每行格式：

```text
[timestamp_us] [EVENT] [key:value ...]
```

事件类型：

| 事件 | 含义 |
| --- | --- |
| `SEND` | 发送报文：seq、ack、flag、length |
| `RECV` | 接收报文：seq、ack、flag、length |
| `CWND` | 拥塞窗口变化：type、size（字节） |
| `RWND` | 接收窗口通告：size |
| `SWND` | 实际发送窗口：size |
| `RTTS` | RTT/RTO 估计：SampleRTT、EstimatedRTT、DeviationRTT、TimeoutInterval（毫秒） |
| `DELV` | 服务端按序交付给应用层的字节数：seq、size |

`CWND type` 取值：`0` 慢启动，`1` 拥塞避免，`2` 快速重传，`3` RTO 超时。

### 5.5 复现命令

在 `tju_tcp/` 根目录下执行：

```bash
# 从 T-01 的 client trace 重新生成窗口/RTT/吞吐率图
python3 test/gen_graph_win.py test/evidence/T-01-delay20ms-loss3pct/client.event.trace

# 重新执行 24 轮对照实验（会重新运行测试并写入新的批次目录）
python3 test/run_perf_sweep.py --repeats 3 --outdir test/evidence/reproduced

# NR-01 基础组、NewReno 组和对比图
cd test
python3 run_newreno_compare.py run --variant baseline --scenario NR-01 --batch-dir /vagrant/tju_tcp/test/evidence/newreno-comparison/NR-01-20260921-024944
python3 run_newreno_compare.py run --variant newreno --scenario NR-01 --batch-dir /vagrant/tju_tcp/test/evidence/newreno-comparison/NR-01-20260921-024944
python3 run_newreno_compare.py compare --batch-dir /vagrant/tju_tcp/test/evidence/newreno-comparison/NR-01-20260921-024944
```

仅查看已有批次的统计量时，直接阅读 `test/evidence/perf-20260917-103605/summary_by_condition.csv` 与每轮的 `metrics.json`、`run-info.txt` 即可；当前脚本没有“只分析已有 Trace、不重新运行实验”的独立模式。
### 5.6 图表与证据对应

- 报告图 7-2 → `test/evidence/T-01-delay20ms-loss3pct/client_AllWindowSize_VS_Time.png`；
- 报告图 7-3 → `test/evidence/T-01-delay20ms-loss3pct/client_CongestionWindowSize_VS_Time.png`；
- 报告图 7-4 → `test/evidence/T-01-delay20ms-loss3pct/client_RTT_VS_Time.png`；
- 报告图 7-5 → `test/evidence/T-01-delay20ms-loss3pct/client_SendWindowSize_VS_Time.png`；
- 报告图 7-6 → `test/evidence/T-01-delay20ms-loss3pct/SeqNum_VS_Time.png`；
- 报告图 8-1/8-2 → `test/evidence/perf-20260917-103605/report_throughput_vs_*.png`；
- 表 8-3/8-4 → `test/evidence/perf-20260917-103605/summary.csv`、`summary_by_condition.csv`（分别为丢包率、时延结果）；
- 表 8-5 → 原始数据映射表（`batch-config.txt`、`summary*.csv`、各轮 trace）；
- 每轮吞吐率与快速重传/RTO 次数 → `test/evidence/perf-20260917-103605/A-*/rN/metrics.json`、`run-info.txt`；
- 原始 Trace → `test/evidence/perf-20260917-103605/A-*/rN/client.event.trace`、`server.event.trace`。
- NR-01 → `test/evidence/newreno-comparison/NR-01-20260921-024944/comparison/`、`baseline/r01/`、`newreno/r01/`。

## 6. 打包脚本

### 6.1 test/pack_test.sh（最终提交包）

```bash
cd /vagrant/tju_tcp/test
bash pack_test.sh
```

脚本默认生成 `tju_tcp_package.zip`，固定纳入源码、测试脚本以及 T-01、性能实验和 NR-01 三组 evidence。打包时递归排除 `*.log`，并生成包含 Git 分支、提交号、标签和证据范围的 `PACKAGE-INFO.txt`。

```text
tju_tcp/
├── README.md
├── PACKAGE-INFO.txt
├── Makefile
├── inc/
├── src/
└── test/
    ├── Makefile
    ├── *.c
    ├── *.py
    ├── rdt_send_file.txt
    ├── pack.sh
    ├── pack_test.sh
    └── evidence/
        ├── T-01-delay20ms-loss3pct/
        ├── perf-20260917-103605/
        └── newreno-comparison/
```

可选参数：

```bash
bash pack_test.sh --outdir /vagrant   # 指定输出目录
```

## 7. 版本与环境记录

- 当前选做分支：`stage-4`
- v1.3 基础提交：`23e7b6326a8b84c16f2256188560b45d1f456796`
- v1.4：RFC 6582 NewReno 选做实现、NR-01 对比证据与最终提交包
- 环境：Vagrant `ubuntu/netproj`；Client `172.17.0.2`，Server `172.17.0.3`
- 工具：gcc、make、Python 3、fabric、matplotlib、scapy、tcpdump、tc/tcconfig
- 默认网络：100 Mbps、20 ms、无丢包；实验网络参数见 `test/evidence/perf-20260917-103605/batch-config.txt`
