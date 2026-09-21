#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_perf_sweep.py

在 Client VM 上运行，自动重复调用现有 test_congestion.py，完成“丢包率”
和“时延”两类变量的吞吐率对照实验，并把每轮的原始 trace、图片、日志和
计算结果归档到同一个批次目录。

设计原则
--------
1. 不修改 test_congestion.py / gen_graph_win.py / gen_graph_seq.py 的任何代码；
2. 本脚本只负责编排、归档、计算吞吐率和汇总；
3. 单轮的网络设置、运行、抓包和出图仍然由 test_congestion.py 完成；
4. 每轮结束后立即把 test/ 根目录下的产物复制到 <batch>/<condition>/rN/，
   避免下一轮覆盖上一轮的 trace 和图片。

用法示例
--------
    # 只打印将要执行的条件，不真正运行
    python3 run_perf_sweep.py --dry-run

    # 完整实验：A、B 各 4 个水平，每组重复 3 次
    python3 run_perf_sweep.py --repeats 3

    # 先做一个快速试跑
    python3 run_perf_sweep.py --only A --loss-levels 0,3 --repeats 1

    # 中断后继续（已存在且有效的轮次会跳过）
    python3 run_perf_sweep.py --repeats 3 --resume

默认输出目录：
    /vagrant/tju_tcp/test/evidence/perf-YYYYmmdd-HHMMSS/

默认每轮只归档 server_Throuput.png 与 trace，避免报告素材过多；批次结束时
额外生成两张可直接放进报告的汇总图：
    report_throughput_vs_loss.png
    report_throughput_vs_delay.png
"""

import argparse
import csv
import json
import math
import os
import queue
import re
import shlex
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

try:
    from fabric import Connection
except ImportError:
    Connection = None

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:
    plt = None

TEST_DIR = Path(__file__).resolve().parent
RUNNER = TEST_DIR / "test_congestion.py"

SERVER_HOST = "172.17.0.3"
TEST_USER = "vagrant"
TEST_PASS = "vagrant"
IFACE = "enp0s8"
SERVER_PCAP = "/home/vagrant/server.pcap"

# test_rdt_client.c: t_times = 5000, EACHSIZE = 10 * 1000
TOTAL_SEND_BYTES = 5000 * 10000
SMSS = 1380

TRACE_RE = re.compile(
    r"^\[(\d+)\]\s+\[(SEND|RECV|CWND|RWND|SWND|RTTS|DELV)\]\s+\[(.+)\]$"
)

PNG_SUFFIXES = (
    "ReceiveWindowSize_VS_Time.png",
    "SendWindowSize_VS_Time.png",
    "CongestionWindowSize_VS_Time.png",
    "AllWindowSize_VS_Time.png",
    "RTT_VS_Time.png",
    "Throuput.png",
)

WORK_FILES = (
    "client.event.trace",
    "server.event.trace",
    "client.log",
    "server.log",
    "SeqNum_VS_Time.png",
)

CURRENT_PROC = None
STOP_REQUESTED = False


def log(message):
    print(message, flush=True)


def parse_int_list(text):
    values = []
    for item in str(text).split(","):
        item = item.strip()
        if not item:
            continue
        values.append(int(item))
    if not values:
        raise argparse.ArgumentTypeError("列表不能为空: %r" % text)
    return values


def reset_net_cmd():
    return "sudo tcset %s --rate 100Mbps --delay 20ms --overwrite" % IFACE


def local_run(command, timeout=60, check=False):
    """在 Client VM 本地执行 shell 命令。"""
    return subprocess.run(
        command,
        shell=True,
        timeout=timeout,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=check,
    )


def handle_signal(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    log("\n收到停止信号，正在结束当前轮次……")
    proc = CURRENT_PROC
    if proc is not None and proc.poll() is None:
        try:
            proc.terminate()
        except Exception:
            pass

def build_conditions(args):
    conditions = []
    if args.only in ("all", "A"):
        for loss in args.loss_levels:
            condition_id = "A-delay%gms-loss%gpct" % (args.fixed_delay, loss)
            conditions.append(dict(
                id=condition_id,
                experiment="A",
                rate=args.rate,
                delay=args.fixed_delay,
                jitter=args.jitter,
                loss=loss,
            ))
    if args.only in ("all", "B"):
        for delay in args.delay_levels:
            condition_id = "B-delay%gms-loss%gpct" % (delay, args.fixed_loss)
            conditions.append(dict(
                id=condition_id,
                experiment="B",
                rate=args.rate,
                delay=delay,
                jitter=args.jitter,
                loss=args.fixed_loss,
            ))
    return conditions


def print_plan(args, conditions, batch_dir):
    schedule = [
        (repeat, condition)
        for repeat in range(1, args.repeats + 1)
        for condition in conditions
    ]

    log("=" * 72)
    log("自动性能对照实验计划")
    log("=" * 72)
    log("批次目录 : %s" % batch_dir)
    log("运行脚本 : %s" % RUNNER)
    log("带宽     : %d Mbps" % args.rate)
    log("抖动     : %d ms（所有条件固定）" % args.jitter)
    log("观测窗口 : %g s" % args.window)
    log("重复次数 : %d" % args.repeats)
    log("最大重试 : %d 次/轮" % args.max_retries)
    log("")
    if args.only in ("all", "A"):
        log("实验 A（固定时延 %g ms）: 丢包率 %s" % (
            args.fixed_delay, args.loss_levels))
    if args.only in ("all", "B"):
        log("实验 B（固定丢包 %g%%）: 时延 %s" % (
            args.fixed_loss, args.delay_levels))
    log("")
    log("执行顺序（按轮次交错）：")
    for repeat, condition in schedule:
        log("  r%-2d %-28s rate=%d delay=%d jitter=%d loss=%d" % (
            repeat,
            condition["id"],
            condition["rate"],
            condition["delay"],
            condition["jitter"],
            condition["loss"],
        ))
    log("")
    log("总轮数：%d" % len(schedule))
    log("=" * 72)
    return schedule


def cleanup(conn, quiet=False):
    """清理残留进程并恢复默认网络。注意：变量名 reset 不要改成 netem。"""
    stop_tcpdump = 'sudo pkill -f "tcpdump -i %s -w %s udp"' % (
        IFACE, SERVER_PCAP)
    stop_server = 'sudo pkill -f "/vagrant/tju_tcp/test/rdt_server"'
    stop_client = 'sudo pkill -f "/vagrant/tju_tcp/test/rdt_client"'
    reset = reset_net_cmd()
    actions = (
        ("server: 停止 tcpdump", lambda: conn.run(
            stop_tcpdump, warn=True, hide=True, timeout=15)),
        ("server: 停止 rdt_server", lambda: conn.run(
            stop_server, warn=True, hide=True, timeout=15)),
        ("server: 恢复默认网络", lambda: conn.run(
            reset, warn=True, hide=True, timeout=30)),
        ("client: 停止 rdt_client", lambda: local_run(stop_client)),
        ("client: 恢复默认网络", lambda: local_run(reset)),
    )
    for name, action in actions:
        try:
            action()
        except Exception as exc:
            if not quiet:
                log("  [警告] %s 失败: %s" % (name, exc))


def prepare_workdir():
    """删除 test/ 根目录下上一轮生成的产物，避免误把旧文件当成新结果。"""
    removed = []
    for name in WORK_FILES:
        path = TEST_DIR / name
        if path.exists():
            try:
                path.unlink()
                removed.append(name)
            except OSError as exc:
                log("  [警告] 无法删除 %s: %s" % (path, exc))

    for prefix in ("client", "server"):
        for suffix in PNG_SUFFIXES:
            path = TEST_DIR / ("%s_%s" % (prefix, suffix))
            if path.exists():
                try:
                    path.unlink()
                    removed.append(path.name)
                except OSError as exc:
                    log("  [警告] 无法删除 %s: %s" % (path, exc))
    return removed


def run_runner(args, condition, log_path):
    """调用现有 test_congestion.py，并把它的输出同时打印和写入日志。"""
    global CURRENT_PROC

    command = [
        args.python,
        str(RUNNER),
        str(condition["rate"]),
        str(condition["delay"]),
        str(condition["jitter"]),
        str(condition["loss"]),
    ]
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    log("  运行命令: %s" % " ".join(shlex.quote(item) for item in command))
    start_time = time.time()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with open(log_path, "w", encoding="utf-8", errors="replace") as log_file:
        proc = subprocess.Popen(
            command,
            cwd=str(TEST_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
        CURRENT_PROC = proc

        lines = queue.Queue()

        def reader(pipe, output_queue):
            try:
                for line in iter(pipe.readline, ""):
                    output_queue.put(line)
            finally:
                output_queue.put(None)

        thread = threading.Thread(
            target=reader, args=(proc.stdout, lines), daemon=True)
        thread.start()

        deadline = time.time() + args.run_timeout
        timed_out = False

        while True:
            try:
                line = lines.get(timeout=0.5)
            except queue.Empty:
                if proc.poll() is not None:
                    break
                if STOP_REQUESTED:
                    proc.terminate()
                    time.sleep(1)
                    if proc.poll() is None:
                        proc.kill()
                    break
                if time.time() > deadline:
                    timed_out = True
                    log("  [警告] 单轮运行超时（%ds），终止本轮" % args.run_timeout)
                    proc.kill()
                    break
                continue

            if line is None:
                break
            sys.stdout.write(line)
            sys.stdout.flush()
            log_file.write(line)
            log_file.flush()

        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

        CURRENT_PROC = None
        return_code = proc.returncode

    elapsed = time.time() - start_time
    return return_code, elapsed, timed_out


def parse_trace(path):
    events = []
    with open(path, "r", encoding="utf-8-sig") as trace_file:
        for line in trace_file:
            match = TRACE_RE.match(line.strip())
            if match is None:
                continue
            info = {}
            for item in match.group(3).split():
                if ":" not in item:
                    continue
                key, value = item.split(":", 1)
                info[key] = value
            events.append((int(match.group(1)), match.group(2), info))
    return events


def compute_metrics(client_trace, server_trace, window_seconds):
    """按统一口径计算单轮吞吐率。

    正常情况：G = 窗口内 DELV 字节数 * 8 / 窗口秒数。
    若传输在窗口结束前已经完成：改用完成时间口径
              G = 全部 DELV 字节数 * 8 / 实际传输时间。
    """
    metrics = {
        "valid": False,
        "metric": "invalid",
        "note": "",
    }

    if not client_trace.exists() or not server_trace.exists():
        metrics["note"] = "缺少 client.event.trace 或 server.event.trace"
        return metrics

    try:
        client_events = parse_trace(client_trace)
        server_events = parse_trace(server_trace)
    except Exception as exc:
        metrics["note"] = "解析 trace 失败: %s" % exc
        return metrics

    data_sends = []
    for timestamp, event, info in client_events:
        if event != "SEND" or info.get("flag") != "0":
            continue
        try:
            length = int(info.get("length", "0"))
            sequence = int(info.get("seq", "0"))
        except ValueError:
            continue
        if length > 0:
            data_sends.append((timestamp, sequence, length))

    deliveries = []
    for timestamp, event, info in server_events:
        if event != "DELV":
            continue
        try:
            deliveries.append((timestamp, int(info.get("size", "0"))))
        except ValueError:
            continue

    if not data_sends:
        metrics["note"] = "client trace 中没有数据 SEND 事件"
        return metrics
    if not deliveries:
        metrics["note"] = "server trace 中没有 DELV 事件"
        return metrics

    t0 = data_sends[0][0]
    window_end = t0 + int(round(window_seconds * 1000000))
    delivered_window = sum(
        size for timestamp, size in deliveries
        if t0 <= timestamp < window_end
    )
    delivered_total = sum(size for _, size in deliveries)
    last_delivery = deliveries[-1][0]
    active_seconds = max(0.0, (last_delivery - t0) / 1000000.0)

    throughput_window = (
        delivered_window * 8.0 / window_seconds / 1000000.0
        if window_seconds > 0 else 0.0
    )
    throughput_active = (
        delivered_total * 8.0 / active_seconds / 1000000.0
        if active_seconds > 0 else 0.0
    )

    max_sent = max(sequence + length for _, sequence, length in data_sends)
    finished = max_sent >= TOTAL_SEND_BYTES - SMSS
    last_event = max(client_events[-1][0], server_events[-1][0])
    window_covered = last_event >= window_end
    early_finish = bool(finished and last_delivery < window_end)

    type2 = sum(
        1 for timestamp, event, info in client_events
        if event == "CWND" and t0 <= timestamp < window_end
        and info.get("type") == "2"
    )
    type3 = sum(
        1 for timestamp, event, info in client_events
        if event == "CWND" and t0 <= timestamp < window_end
        and info.get("type") == "3"
    )

    if early_finish:
        metric = "active"
        throughput = throughput_active
        valid = delivered_total > 0
        note = "传输在窗口结束前已完成，改用完成时间口径"
    elif window_covered and delivered_window > 0:
        metric = "window"
        throughput = throughput_window
        valid = True
        note = ""
    else:
        metric = "invalid"
        throughput = 0.0
        valid = False
        note = "trace 未完整覆盖 %.0fs 窗口，或窗口内没有 DELV" % window_seconds

    metrics.update(dict(
        valid=valid,
        metric=metric,
        note=note,
        t0_us=t0,
        window_end_us=window_end,
        last_event_us=last_event,
        last_delivery_us=last_delivery,
        window_covered=window_covered,
        early_finish=early_finish,
        delivered_window_bytes=delivered_window,
        delivered_total_bytes=delivered_total,
        active_seconds=round(active_seconds, 6),
        throughput_window_mbps=round(throughput_window, 6),
        throughput_active_mbps=round(throughput_active, 6),
        throughput_mbps=round(throughput, 6),
        max_sent_bytes=max_sent,
        type2=type2,
        type3=type3,
    ))
    return metrics

def archive_outputs(conn, run_dir, keep_images="throughput",
                    archive_pcap=False):
    """把 test/ 根目录下本轮产物复制到 run_dir。

    keep_images:
      all        - 复制本轮生成的全部 PNG（与旧行为一致）
      throughput - 只复制 server_Throuput.png（默认，避免报告素材过多）
      none       - 不复制任何 PNG
    """
    run_dir.mkdir(parents=True, exist_ok=True)
    copied = []

    for name in (
        "client.event.trace",
        "server.event.trace",
        "client.log",
        "server.log",
    ):
        source = TEST_DIR / name
        if source.exists():
            shutil.copy2(source, run_dir / name)
            copied.append(name)

    image_names = []
    if keep_images == "all":
        image_names.append("SeqNum_VS_Time.png")
        for prefix in ("client", "server"):
            for suffix in PNG_SUFFIXES:
                image_names.append("%s_%s" % (prefix, suffix))
    elif keep_images == "throughput":
        image_names.append("server_Throuput.png")

    for name in image_names:
        source = TEST_DIR / name
        if source.exists():
            shutil.copy2(source, run_dir / name)
            copied.append(name)

    if not archive_pcap:
        return copied

    # server.pcap 在 Server VM 的 home 目录：优先用 SFTP 取回，
    # 失败再尝试在服务端复制到 /vagrant 共享目录。
    pcap_destination = run_dir / "server.pcap"
    try:
        conn.get(SERVER_PCAP, str(pcap_destination))
        if pcap_destination.exists():
            copied.append("server.pcap")
    except Exception as get_error:
        try:
            command = "sudo cp -f %s %s && sudo chown vagrant:vagrant %s" % (
                shlex.quote(SERVER_PCAP),
                shlex.quote(str(pcap_destination)),
                shlex.quote(str(pcap_destination)),
            )
            conn.run(command, warn=True, hide=True, timeout=60)
            if pcap_destination.exists():
                copied.append("server.pcap")
        except Exception as copy_error:
            log("  [警告] 复制 server.pcap 失败: %s / %s" % (
                get_error, copy_error))

    return copied

def write_run_info(run_dir, row):
    lines = [
        "条件: %s" % row["condition_id"],
        "实验: %s" % row["experiment"],
        "带宽: %d Mbps" % row["rate_mbps"],
        "单向时延: %d ms" % row["delay_ms"],
        "抖动: %d ms" % row["jitter_ms"],
        "丢包率: %d%%" % row["loss_pct"],
        "重复: r%d" % row["repeat"],
        "尝试次数: %d" % row["attempt"],
        "返回码: %s" % row["return_code"],
        "运行耗时: %.3f s" % row["elapsed_s"],
        "是否超时: %s" % row["timed_out"],
        "有效: %s" % row["valid"],
        "吞吐率口径: %s" % row["metric"],
        "吞吐率: %.6f Mbps" % row["throughput_mbps"],
        "窗口口径: %.6f Mbps" % row["throughput_window_mbps"],
        "完成时间口径: %.6f Mbps" % row["throughput_active_mbps"],
        "窗口内交付字节: %d" % row["delivered_window_bytes"],
        "总交付字节: %d" % row["delivered_total_bytes"],
        "窗口是否完整覆盖: %s" % row["window_covered"],
        "是否提前完成: %s" % row["early_finish"],
        "快速重传(type:2): %s" % row.get("type2", ""),
        "RTO(type:3): %s" % row.get("type3", ""),
        "说明: %s" % row["note"],
        "归档目录: %s" % row["run_dir"],
        "归档文件: %s" % row["artifacts"],
    ]
    (run_dir / "run-info.txt").write_text(
        "\n".join(lines) + "\n", encoding="utf-8")


def run_one(conn, args, condition, repeat, run_dir):
    run_dir.mkdir(parents=True, exist_ok=True)
    max_attempts = max(1, args.max_retries + 1)
    last_row = None

    for attempt in range(1, max_attempts + 1):
        log("  尝试 %d/%d" % (attempt, max_attempts))
        cleanup(conn)
        prepare_workdir()

        log_path = run_dir / ("attempt%d.log" % attempt)
        return_code, elapsed, timed_out = run_runner(
            args, condition, log_path)

        copied = archive_outputs(
            conn, run_dir, args.archive_images, args.archive_pcap)
        metrics = compute_metrics(
            run_dir / "client.event.trace",
            run_dir / "server.event.trace",
            args.window,
        )

        row = dict(
            batch=run_dir.parent.parent.name,
            condition_id=condition["id"],
            experiment=condition["experiment"],
            rate_mbps=condition["rate"],
            delay_ms=condition["delay"],
            jitter_ms=condition["jitter"],
            loss_pct=condition["loss"],
            repeat=repeat,
            attempt=attempt,
            return_code=return_code,
            elapsed_s=round(elapsed, 3),
            timed_out=timed_out,
            artifacts=",".join(copied),
            run_dir=str(run_dir),
        )
        row.update(metrics)

        write_run_info(run_dir, row)
        (run_dir / "metrics.json").write_text(
            json.dumps(row, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        last_row = row

        if row["valid"]:
            log("  有效：%.4f Mbps（%s 口径）" % (
                row["throughput_mbps"], row["metric"]))
            break

        log("  本轮无效：%s" % (row.get("note") or "未知原因"))
        if STOP_REQUESTED:
            break
        if attempt < max_attempts:
            log("  重试同一轮次……")

    return last_row


SUMMARY_COLUMNS = (
    "batch",
    "condition_id",
    "experiment",
    "rate_mbps",
    "delay_ms",
    "jitter_ms",
    "loss_pct",
    "repeat",
    "attempt",
    "valid",
    "metric",
    "throughput_mbps",
    "throughput_window_mbps",
    "throughput_active_mbps",
    "delivered_window_bytes",
    "delivered_total_bytes",
    "active_seconds",
    "window_covered",
    "early_finish",
    "type2",
    "type3",
    "max_sent_bytes",
    "t0_us",
    "window_end_us",
    "last_event_us",
    "last_delivery_us",
    "return_code",
    "elapsed_s",
    "timed_out",
    "artifacts",
    "note",
    "run_dir",
)


def write_summary(batch_dir, rows):
    path = batch_dir / "summary.csv"
    with open(path, "w", newline="", encoding="utf-8-sig") as summary_file:
        writer = csv.DictWriter(
            summary_file, fieldnames=SUMMARY_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def write_group_summary(batch_dir, rows):
    groups = defaultdict(list)
    for row in rows:
        if row.get("valid"):
            groups[row["condition_id"]].append(row)

    summary = []
    for condition_id, items in groups.items():
        values = [float(item["throughput_mbps"]) for item in items]
        first = items[0]
        std = statistics.stdev(values) if len(values) > 1 else 0.0
        summary.append(dict(
            condition_id=condition_id,
            experiment=first["experiment"],
            delay_ms=first["delay_ms"],
            loss_pct=first["loss_pct"],
            n=len(values),
            mean_mbps=round(statistics.mean(values), 6),
            std_mbps=round(std, 6),
            median_mbps=round(statistics.median(values), 6),
            min_mbps=round(min(values), 6),
            max_mbps=round(max(values), 6),
            metrics_used=",".join(sorted(set(item["metric"] for item in items))),
        ))

    summary.sort(key=lambda item: (
        item["experiment"], item["delay_ms"], item["loss_pct"]))

    columns = (
        "condition_id", "experiment", "delay_ms", "loss_pct", "n",
        "mean_mbps", "std_mbps", "median_mbps", "min_mbps", "max_mbps",
        "metrics_used",
    )
    path = batch_dir / "summary_by_condition.csv"
    with open(path, "w", newline="", encoding="utf-8-sig") as summary_file:
        writer = csv.DictWriter(
            summary_file, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for item in summary:
            writer.writerow(item)
    return summary


def plot_sweeps(batch_dir, rows, group_summary):
    if plt is None:
        log("  [警告] 未安装 matplotlib，跳过汇总图")
        return []

    outputs = []
    specs = (
        ("A", "loss_pct", "Loss Rate (%)", "throughput_vs_loss.png"),
        ("B", "delay_ms", "One-way Delay (ms)", "throughput_vs_delay.png"),
    )

    for experiment, x_key, x_label, filename in specs:
        items = [
            item for item in group_summary
            if item["experiment"] == experiment
        ]
        if not items:
            continue
        items.sort(key=lambda item: item[x_key])

        xs = [item[x_key] for item in items]
        means = [item["mean_mbps"] for item in items]
        stds = [item["std_mbps"] for item in items]

        figure, axis = plt.subplots(figsize=(8, 5))
        axis.errorbar(
            xs, means, yerr=stds, marker="o", capsize=4,
            linewidth=2, label="mean +/- std")

        for item in items:
            raw_values = [
                float(row["throughput_mbps"]) for row in rows
                if row.get("valid") and row["condition_id"] == item["condition_id"]
            ]
            axis.scatter(
                [item[x_key]] * len(raw_values), raw_values,
                color="tab:gray", alpha=0.7, zorder=3)

        if experiment == "A":
            title = "Throughput vs Loss Rate (delay = %g ms)" % items[0]["delay_ms"]
        else:
            title = "Throughput vs One-way Delay (loss = %g%%)" % items[0]["loss_pct"]

        axis.set_title(title)
        axis.set_xlabel(x_label)
        axis.set_ylabel("Throughput (Mbps)")
        axis.grid(True, alpha=0.3)
        axis.legend()
        figure.tight_layout()
        output = batch_dir / filename
        figure.savefig(output, dpi=300)
        plt.close(figure)
        outputs.append(output)

    return outputs


def per_second_throughput(client_trace, server_trace, window_seconds):
    """返回 [t0, t0+window) 内每 1 s 的吞吐率序列（Mbps）。"""
    if not client_trace.exists() or not server_trace.exists():
        return None

    try:
        client_events = parse_trace(client_trace)
        server_events = parse_trace(server_trace)
    except Exception:
        return None

    t0 = None
    for timestamp, event, info in client_events:
        if event != "SEND" or info.get("flag") != "0":
            continue
        try:
            length = int(info.get("length", "0"))
        except ValueError:
            continue
        if length > 0:
            t0 = timestamp
            break
    if t0 is None:
        return None

    bins = max(1, int(math.floor(window_seconds)))
    series = [0.0] * bins
    for timestamp, event, info in server_events:
        if event != "DELV" or timestamp < t0:
            continue
        try:
            size = int(info.get("size", "0"))
        except ValueError:
            continue
        index = int((timestamp - t0) / 1000000.0)
        if 0 <= index < bins:
            series[index] += size * 8.0 / 1000000.0
    return series


def plot_report_dashboards(batch_dir, rows, group_summary, window_seconds):
    """生成两张可直接放进报告的汇总图。

    每张图：上排为吞吐率趋势（mean +/- std，散点为各次重复），
    下排为每个水平在观测窗口内的逐秒吞吐率（细线为重复，粗线为均值）。
    """
    if plt is None:
        log("  [警告] 未安装 matplotlib，跳过报告汇总图")
        return []

    outputs = []
    specs = (
        ("A", "loss_pct", "Loss rate (%)", "loss",
         "report_throughput_vs_loss.png"),
        ("B", "delay_ms", "One-way delay (ms)", "delay",
         "report_throughput_vs_delay.png"),
    )

    valid_rows = [row for row in rows if row.get("valid")]

    for experiment, x_key, x_label, short_label, filename in specs:
        experiment_rows = [
            row for row in valid_rows if row["experiment"] == experiment
        ]
        items = [
            item for item in group_summary
            if item["experiment"] == experiment
        ]
        if not experiment_rows or not items:
            continue

        items.sort(key=lambda item: item[x_key])
        levels = [item[x_key] for item in items]
        ncols = min(4, len(levels))
        nrows = max(1, int(math.ceil(len(levels) / float(ncols))))

        figure = plt.figure(figsize=(4.6 * ncols, 4.0 + 3.2 * nrows))
        grid = figure.add_gridspec(
            1 + nrows, ncols,
            height_ratios=[1.25] + [1.0] * nrows,
            hspace=0.50, wspace=0.30,
        )

        top = figure.add_subplot(grid[0, :])
        xs = [item[x_key] for item in items]
        means = [item["mean_mbps"] for item in items]
        stds = [item["std_mbps"] for item in items]
        top.errorbar(xs, means, yerr=stds, marker="o", capsize=4,
                     linewidth=2, label="mean +/- std")
        for item in items:
            raw_values = [
                float(row["throughput_mbps"]) for row in experiment_rows
                if row["condition_id"] == item["condition_id"]
            ]
            top.scatter([item[x_key]] * len(raw_values), raw_values,
                        color="tab:gray", alpha=0.75, zorder=3)
        repeats_per_level = [
            len([row for row in experiment_rows
                 if row["condition_id"] == item["condition_id"]])
            for item in items
        ]
        top.set_xlabel(x_label)
        top.set_ylabel("Throughput (Mbps)")
        top.set_title("Throughput vs %s (n = %d)" % (
            x_label, max(repeats_per_level)))
        top.grid(True, alpha=0.3)
        top.legend()

        for index, item in enumerate(items):
            row_index, col_index = divmod(index, ncols)
            axis = figure.add_subplot(grid[1 + row_index, col_index])
            series_list = []
            for row in experiment_rows:
                if row["condition_id"] != item["condition_id"]:
                    continue
                series = per_second_throughput(
                    Path(row["run_dir"]) / "client.event.trace",
                    Path(row["run_dir"]) / "server.event.trace",
                    window_seconds,
                )
                if series:
                    series_list.append(series)

            if series_list:
                for series in series_list:
                    axis.plot(range(len(series)), series,
                              linewidth=1, alpha=0.35)
                length = min(len(series) for series in series_list)
                mean_series = [
                    sum(series[i] for series in series_list) / len(series_list)
                    for i in range(length)
                ]
                axis.plot(range(length), mean_series,
                          color="black", linewidth=2)

            axis.set_title("%s = %g" % (short_label, item[x_key]))
            axis.set_xlabel("Time (s)")
            axis.set_ylabel("Mbps")
            axis.grid(True, alpha=0.3)

        figure.suptitle("TJU_TCP goodput: %s sweep" % short_label)
        figure.subplots_adjust(top=0.90, bottom=0.07, left=0.07, right=0.98)
        output = batch_dir / filename
        figure.savefig(output, dpi=300)
        plt.close(figure)
        outputs.append(output)

    return outputs



def write_batch_config(batch_dir, args, conditions, schedule, conn):
    lines = [
        "批次目录: %s" % batch_dir,
        "生成时间: %s" % time.strftime("%Y-%m-%d %H:%M:%S"),
        "编排脚本: %s" % Path(__file__).resolve(),
        "运行器: %s（未修改）" % RUNNER,
        "带宽: %d Mbps" % args.rate,
        "抖动: %d ms" % args.jitter,
        "观测窗口: %g s" % args.window,
        "重复次数: %d" % args.repeats,
        "最大重试: %d" % args.max_retries,
        "实验选择: %s" % args.only,
        "丢包率水平(A): %s" % args.loss_levels,
        "时延水平(B): %s" % args.delay_levels,
        "A 固定时延: %d ms" % args.fixed_delay,
        "B 固定丢包: %d%%" % args.fixed_loss,
        "tcset 部署: Client 与 Server 两端（由 test_congestion.py 完成）",
        "",
        "条件列表:",
    ]
    for condition in conditions:
        lines.append(
            "  %s rate=%d delay=%d jitter=%d loss=%d" % (
                condition["id"], condition["rate"], condition["delay"],
                condition["jitter"], condition["loss"]))

    lines.append("")
    lines.append("执行顺序:")
    for repeat, condition in schedule:
        lines.append("  r%d %s" % (repeat, condition["id"]))

    try:
        commit = subprocess.check_output(
            ["git", "-C", str(TEST_DIR.parent), "rev-parse", "HEAD"],
            text=True, stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        commit = "(无法获取)"
    lines.append("")
    lines.append("Git commit: %s" % commit)

    try:
        client_date = subprocess.check_output(
            ["date", "+%s.%N"], text=True).strip()
    except Exception:
        client_date = "(无法获取)"
    try:
        server_date = conn.run(
            "date +%s.%N", hide=True, warn=True, timeout=15).stdout.strip()
    except Exception:
        server_date = "(无法获取)"
    lines.append("Client 时间: %s" % client_date)
    lines.append("Server 时间: %s" % server_date)

    (batch_dir / "batch-config.txt").write_text(
        "\n".join(lines) + "\n", encoding="utf-8")


def parse_args():
    parser = argparse.ArgumentParser(
        description="自动重复性能对照实验（不修改现有三个 py 脚本）")
    parser.add_argument(
        "--only", choices=("all", "A", "B"), default="all",
        help="只跑某一类变量；默认 all")
    parser.add_argument(
        "--loss-levels", type=parse_int_list, default=parse_int_list("0,1,3,5"),
        help="实验 A 的丢包率列表，例如 0,1,3,5")
    parser.add_argument(
        "--delay-levels", type=parse_int_list, default=parse_int_list("20,50,100,200"),
        help="实验 B 的单向时延列表，例如 20,50,100,200")
    parser.add_argument("--fixed-delay", type=int, default=20,
                        help="实验 A 固定的单向时延，默认 20 ms")
    parser.add_argument("--fixed-loss", type=int, default=1,
                        help="实验 B 固定的丢包率，默认 1%%")
    parser.add_argument("--rate", type=int, default=100,
                        help="固定带宽，默认 100 Mbps")
    parser.add_argument("--jitter", type=int, default=0,
                        help="固定抖动，默认 0 ms")
    parser.add_argument("--repeats", type=int, default=3,
                        help="每个条件的重复次数，默认 3")
    parser.add_argument("--window", type=float, default=55.0,
                        help="吞吐率观测窗口秒数，默认 55 s")
    parser.add_argument("--max-retries", type=int, default=1,
                        help="单轮无效时的额外重试次数，默认 1")
    parser.add_argument("--run-timeout", type=int, default=300,
                        help="单轮 test_congestion.py 的超时秒数，默认 300")
    parser.add_argument("--archive-images",
                        choices=("all", "throughput", "none"),
                        default="throughput",
                        help="每轮归档哪些图片：all/throughput/none，默认 throughput")
    parser.add_argument("--archive-pcap", action="store_true",
                        help="是否归档每轮的 server.pcap（默认关闭，节省磁盘）")
    parser.add_argument("--outdir", default=None,
                        help="批次输出目录；默认 test/evidence/perf-时间戳")
    parser.add_argument("--python", default=sys.executable,
                        help="运行 test_congestion.py 的 Python 解释器")
    parser.add_argument("--resume", action="store_true",
                        help="跳过已有有效 metrics.json 的轮次")
    parser.add_argument("--dry-run", action="store_true",
                        help="只打印执行计划，不真正运行")
    return parser.parse_args()


def main():
    global STOP_REQUESTED

    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    args = parse_args()
    conditions = build_conditions(args)
    if not conditions:
        log("没有可执行的条件")
        return 2

    if args.outdir:
        batch_dir = Path(args.outdir).expanduser().resolve()
    else:
        batch_dir = TEST_DIR / "evidence" / (
            "perf-" + time.strftime("%Y%m%d-%H%M%S"))
    schedule = print_plan(args, conditions, batch_dir)

    if args.dry_run:
        log("dry-run 结束，未执行任何测试。")
        return 0

    if Connection is None:
        log("错误：未安装 fabric，无法连接 Server VM")
        return 2
    if not RUNNER.exists():
        log("错误：找不到 %s" % RUNNER)
        return 2
    if socket.gethostname() == "server":
        log("错误：本脚本必须在 Client VM 上运行")
        return 2

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    batch_dir.mkdir(parents=True, exist_ok=True)
    rows = []

    with Connection(
        host=SERVER_HOST,
        user=TEST_USER,
        connect_kwargs={"password": TEST_PASS},
    ) as conn:
        try:
            write_batch_config(batch_dir, args, conditions, schedule, conn)
            cleanup(conn)

            total = len(schedule)
            for index, (repeat, condition) in enumerate(schedule, 1):
                if STOP_REQUESTED:
                    break

                run_dir = batch_dir / condition["id"] / ("r%d" % repeat)
                log("")
                log("[%d/%d] %s r%d" % (
                    index, total, condition["id"], repeat))

                if args.resume and (run_dir / "metrics.json").exists():
                    try:
                        previous = json.loads(
                            (run_dir / "metrics.json").read_text(
                                encoding="utf-8"))
                    except Exception:
                        previous = None
                    if previous and previous.get("valid"):
                        log("  已存在有效结果，跳过（--resume）")
                        rows.append(previous)
                        write_summary(batch_dir, rows)
                        continue

                row = run_one(conn, args, condition, repeat, run_dir)
                if row is not None:
                    rows.append(row)
                write_summary(batch_dir, rows)
        finally:
            try:
                cleanup(conn)
            except Exception:
                pass

    group_summary = write_group_summary(batch_dir, rows)
    outputs = plot_sweeps(batch_dir, rows, group_summary)
    report_outputs = plot_report_dashboards(
        batch_dir, rows, group_summary, args.window)

    valid_count = sum(1 for row in rows if row.get("valid"))
    log("")
    log("=" * 72)
    log("批次完成: %s" % batch_dir)
    log("有效轮次: %d / %d" % (valid_count, len(schedule)))
    log("汇总 CSV: %s" % (batch_dir / "summary.csv"))
    if group_summary:
        log("分组统计: %s" % (batch_dir / "summary_by_condition.csv"))
    for output in outputs:
        log("汇总图  : %s" % output)
    for output in report_outputs:
        log("报告汇总图: %s" % output)
    log("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())