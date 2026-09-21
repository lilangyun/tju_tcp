#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NewReno 与基础 Reno 的受控对比测试工具。

该脚本不修改 test_congestion.py，只负责：
1. 在同一个 NR-01 网络场景中调用现有测试程序；
2. 按 baseline/newreno 归档原始 Trace、图片、日志和源码标识；
3. 从 Trace 计算可比指标；
4. 读取两组归档并生成 CSV、Markdown 和报告级对比图。

必须在 Client VM 的 /vagrant/tju_tcp/test 目录运行。
"""

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    plt = None

try:
    from fabric import Connection
except ImportError:
    Connection = None


TEST_DIR = Path(__file__).resolve().parent
RUNNER = TEST_DIR / "test_congestion.py"
EVIDENCE_ROOT = TEST_DIR / "evidence" / "newreno-comparison"
SERVER_HOST = "172.17.0.3"
TEST_USER = "vagrant"
TEST_PASS = "vagrant"
SERVER_PCAP = "/home/vagrant/server.pcap"

SMSS = 1380
TOTAL_SEND_BYTES = 5000 * 10000
SCENARIOS = {
    "NR-01": {
        "rate": 100,
        "delay": 20,
        "jitter": 0,
        "loss": 3,
        "window_seconds": 60,
    }
}

TRACE_RE = re.compile(
    r"^\[(\d+)\]\s+\[(SEND|RECV|CWND|RWND|SWND|RTTS|DELV)\]\s+\[(.+)\]$"
)
INFO_RE = re.compile(r"([^:]+):([^\s]+)")

TRACE_FILES = ("client.event.trace", "server.event.trace")
LOG_FILES = ("client.log", "server.log")
IMAGE_FILES = (
    "SeqNum_VS_Time.png",
    "client_CongestionWindowSize_VS_Time.png",
    "client_RTT_VS_Time.png",
    "client_AllWindowSize_VS_Time.png",
    "client_ReceiveWindowSize_VS_Time.png",
    "client_SendWindowSize_VS_Time.png",
    "server_CongestionWindowSize_VS_Time.png",
    "server_ReceiveWindowSize_VS_Time.png",
    "server_SendWindowSize_VS_Time.png",
    "server_AllWindowSize_VS_Time.png",
    "server_Throuput.png",
)
SOURCE_FILES = (
    "inc/global.h",
    "inc/congestion_control.h",
    "src/congestion_control.c",
    "src/tju_core.c",
    "src/rtt.c",
    "src/timer.c",
)

PALETTE = {
    "baseline": "#B64342",
    "newreno": "#0F4D92",
    "grid": "#D7D3CE",
    "text": "#2F2B28",
}


def log(message):
    print(message, flush=True)


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def git_output(*args):
    result = subprocess.run(
        ["git"] + list(args),
        cwd=str(TEST_DIR.parent),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    return result.stdout.strip()


def detect_newreno_source():
    header = (TEST_DIR.parent / "inc" / "global.h").read_text(
        encoding="utf-8", errors="replace")
    return "recover_seq" in header and "in_fast_recovery" in header


def validate_variant(variant):
    source_is_newreno = detect_newreno_source()
    if variant == "baseline" and source_is_newreno:
        raise SystemExit(
            "当前源码已包含 NewReno 状态，不能在 baseline 模式下采集基础数据。")
    if variant == "newreno" and not source_is_newreno:
        raise SystemExit(
            "当前源码仍是基础 Reno，请在完成 NewReno 实现后再运行 newreno 模式。")


def unique_batch_dir(explicit):
    if explicit:
        return Path(explicit).resolve()
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return EVIDENCE_ROOT / ("NR-01-" + stamp)


def parse_info(text):
    fields = {}
    for key, value in INFO_RE.findall(text):
        fields[key] = value
    return fields


def parse_trace(path):
    series = {
        "send": [],
        "recv": [],
        "cwnd": [],
        "rwnd": [],
        "swnd": [],
        "rtts": [],
        "delv": [],
    }

    with Path(path).open("r", encoding="utf-8-sig") as trace_file:
        for line in trace_file:
            match = TRACE_RE.match(line.strip())
            if match is None:
                continue
            timestamp = int(match.group(1))
            event = match.group(2)
            info = parse_info(match.group(3))
            key = event.lower()
            if event == "DELV":
                key = "delv"
            elif event == "RTTS":
                key = "rtts"
            series[key].append((timestamp, info))
    return series


def compute_metrics(client_trace, server_trace, window_seconds):
    client = parse_trace(client_trace)
    server = parse_trace(server_trace)

    data_sends = []
    for timestamp, info in client["send"]:
        if int(info.get("flag", "0")) != 0:
            continue
        length = int(info.get("length", "0"))
        if length <= 0:
            continue
        data_sends.append((timestamp, int(info["seq"]), length))

    unique_sequences = len(set(item[1] for item in data_sends))
    retransmission_copies = len(data_sends) - unique_sequences

    cwnd_types = {0: 0, 1: 0, 2: 0, 3: 0}
    for _, info in client["cwnd"]:
        cwnd_type = int(info.get("type", "-1"))
        if cwnd_type in cwnd_types:
            cwnd_types[cwnd_type] += 1

    delivered_bytes = 0
    delivery_times = []
    for timestamp, info in server["delv"]:
        delivered_bytes += int(info.get("size", "0"))
        delivery_times.append(timestamp)

    rtt_samples = [float(info["SampleRTT"]) for _, info in client["rtts"]
                   if "SampleRTT" in info]
    first_delivery = min(delivery_times) if delivery_times else None
    last_delivery = max(delivery_times) if delivery_times else None
    active_seconds = (
        (last_delivery - first_delivery) / 1000000.0
        if first_delivery is not None and last_delivery is not None
        else 0.0
    )
    average_throughput_mbps = (
        delivered_bytes * 8 / active_seconds / 1000000.0
        if active_seconds > 0 else 0.0
    )

    return {
        "window_seconds": window_seconds,
        "data_send_events": len(data_sends),
        "unique_data_sequences": unique_sequences,
        "retransmission_copies": retransmission_copies,
        "delivered_bytes": delivered_bytes,
        "completion_ratio": min(1.0, delivered_bytes / TOTAL_SEND_BYTES),
        "active_seconds": active_seconds,
        "average_throughput_mbps": average_throughput_mbps,
        "cwnd_type_0_slow_start": cwnd_types[0],
        "cwnd_type_1_congestion_avoidance": cwnd_types[1],
        "cwnd_type_2_fast_retransmit": cwnd_types[2],
        "cwnd_type_3_timeout": cwnd_types[3],
        "rtt_samples": len(rtt_samples),
        "rtt_min_ms": min(rtt_samples) if rtt_samples else 0.0,
        "rtt_avg_ms": sum(rtt_samples) / len(rtt_samples) if rtt_samples else 0.0,
        "rtt_max_ms": max(rtt_samples) if rtt_samples else 0.0,
    }


def copy_if_exists(source, destination):
    source_path = Path(source)
    if not source_path.exists():
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(source_path), str(destination))
    return True


def archive_run_outputs(run_dir):
    copied = []
    missing = []
    for name in TRACE_FILES + LOG_FILES + IMAGE_FILES:
        source = TEST_DIR / name
        if copy_if_exists(source, run_dir / name):
            copied.append(name)
        else:
            missing.append(name)

    archive_pcap(run_dir, copied, missing)
    return copied, missing


def archive_pcap(run_dir, copied, missing):
    destination = run_dir / "server.pcap"
    if Connection is None:
        missing.append("server.pcap (Fabric 未安装)")
        return
    try:
        with Connection(
            host=SERVER_HOST,
            user=TEST_USER,
            connect_kwargs={"password": TEST_PASS},
        ) as conn:
            conn.get(SERVER_PCAP, str(destination))
        copied.append("server.pcap")
    except Exception as exc:
        missing.append("server.pcap (%s)" % exc)


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def run_protocol_command(scenario, timeout, log_path):
    command = [
        sys.executable,
        str(RUNNER),
        str(scenario["rate"]),
        str(scenario["delay"]),
        str(scenario["jitter"]),
        str(scenario["loss"]),
    ]
    log("执行: " + " ".join(command))
    try:
        result = subprocess.run(
            command,
            cwd=str(TEST_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        output = exc.stdout or ""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        log_path.write_text(output, encoding="utf-8")
        raise SystemExit("测试超过 %d 秒，已终止。" % timeout)

    log_path.write_text(result.stdout or "", encoding="utf-8")
    if result.returncode != 0:
        raise SystemExit("test_congestion.py 退出码为 %d，详见 %s" %
                         (result.returncode, log_path))


def command_run(args):
    validate_variant(args.variant)
    scenario = SCENARIOS[args.scenario]
    batch_dir = unique_batch_dir(args.batch_dir)
    run_dir = batch_dir / args.variant / args.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    source_hashes = {
        name: sha256_file(TEST_DIR.parent / name)
        for name in SOURCE_FILES
    }
    run_info = {
        "variant": args.variant,
        "scenario": args.scenario,
        "scenario_parameters": scenario,
        "run_id": args.run_id,
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "hostname": socket.gethostname(),
        "git_branch": git_output("branch", "--show-current"),
        "git_commit": git_output("rev-parse", "HEAD"),
        "git_status": git_output("status", "--short"),
        "source_sha256": source_hashes,
    }
    write_json(run_dir / "run-info-before.json", run_info)

    runner_log = run_dir / "runner.log"
    run_protocol_command(scenario, args.timeout, runner_log)
    copied, missing = archive_run_outputs(run_dir)

    client_trace = run_dir / "client.event.trace"
    server_trace = run_dir / "server.event.trace"
    if not client_trace.exists() or not server_trace.exists():
        raise SystemExit("Trace 归档不完整: %s" % run_dir)

    metrics = compute_metrics(
        client_trace,
        server_trace,
        scenario["window_seconds"],
    )
    metrics["variant"] = args.variant
    metrics["scenario"] = args.scenario
    metrics["copied_files"] = copied
    metrics["missing_files"] = missing
    write_json(run_dir / "metrics.json", metrics)
    run_info["finished_at"] = datetime.now().isoformat(timespec="seconds")
    run_info["metrics"] = metrics
    write_json(run_dir / "run-info.json", run_info)

    log("")
    log("运行归档: %s" % run_dir)
    log("交付字节: %d" % metrics["delivered_bytes"])
    log("平均吞吐率: %.3f Mbps" % metrics["average_throughput_mbps"])
    log("快速重传: %d，RTO: %d" % (
        metrics["cwnd_type_2_fast_retransmit"],
        metrics["cwnd_type_3_timeout"],
    ))
    if missing:
        log("缺失可选文件: %s" % ", ".join(missing))


def collect_metric_files(batch_dir, variant):
    return sorted((batch_dir / variant).glob("*/metrics.json"))


def average_metrics(metric_files):
    if not metric_files:
        raise SystemExit("缺少 metrics.json")
    rows = [json.loads(path.read_text(encoding="utf-8"))
            for path in metric_files]
    numeric_keys = [
        key for key in rows[0]
        if isinstance(rows[0][key], (int, float)) and not isinstance(rows[0][key], bool)
    ]
    averaged = {}
    for key in numeric_keys:
        averaged[key] = sum(float(row[key]) for row in rows) / len(rows)
    return averaged, len(rows)


def latest_batch_dir():
    candidates = sorted(
        [path for path in EVIDENCE_ROOT.glob("NR-01-*") if path.is_dir()],
        key=lambda path: path.stat().st_mtime,
    )
    if not candidates:
        raise SystemExit("未找到 newreno-comparison/NR-01-* 批次目录")
    return candidates[-1]


def write_comparison_csv(batch_dir, baseline, newreno):
    path = batch_dir / "comparison.csv"
    keys = sorted(set(baseline) | set(newreno))
    with path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["metric", "baseline", "newreno", "improvement"])
        for key in keys:
            base = float(baseline.get(key, 0.0))
            new = float(newreno.get(key, 0.0))
            improvement = ((new - base) / base) if base else 0.0
            writer.writerow([key, base, new, improvement])
    return path


def write_comparison_markdown(batch_dir, baseline, newreno, baseline_runs, newreno_runs):
    path = batch_dir / "comparison.md"
    lines = [
        "# NewReno 与基础 Reno 对比结果",
        "",
        "| 指标 | 基础 Reno | NewReno | 变化 |",
        "| --- | ---: | ---: | ---: |",
    ]
    metrics = [
        ("交付字节", "delivered_bytes"),
        ("平均吞吐率/Mbps", "average_throughput_mbps"),
        ("重传副本数", "retransmission_copies"),
        ("快速重传次数", "cwnd_type_2_fast_retransmit"),
        ("RTO 次数", "cwnd_type_3_timeout"),
    ]
    for label, key in metrics:
        base = float(baseline.get(key, 0.0))
        new = float(newreno.get(key, 0.0))
        delta = ((new - base) / base * 100.0) if base else 0.0
        lines.append("| %s | %.3f | %.3f | %+.2f%% |" % (
            label, base, new, delta))
    lines.extend([
        "",
        "基础 Reno 运行次数: %d" % baseline_runs,
        "NewReno 运行次数: %d" % newreno_runs,
    ])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def plot_comparison(batch_dir, baseline, newreno):
    if plt is None:
        raise SystemExit("matplotlib 未安装，无法生成对比图")

    plt.rcParams.update({
        "font.size": 14,
        "axes.labelsize": 14,
        "axes.titlesize": 16,
        "font.family": ["DejaVu Sans", "Microsoft YaHei", "sans-serif"],
        "axes.spines.top": False,
        "axes.spines.right": False,
    })
    panels = [
        ("交付字节", "delivered_bytes", "{:,.0f}"),
        ("平均吞吐率 (Mbps)", "average_throughput_mbps", "{:.3f}"),
        ("快速重传次数", "cwnd_type_2_fast_retransmit", "{:.0f}"),
        ("RTO 次数", "cwnd_type_3_timeout", "{:.0f}"),
    ]
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    colors = [PALETTE["baseline"], PALETTE["newreno"]]
    labels = ["Basic Reno", "NewReno"]
    for axis, (title, key, fmt) in zip(axes.flat, panels):
        values = [float(baseline.get(key, 0.0)), float(newreno.get(key, 0.0))]
        bars = axis.bar(labels, values, color=colors, edgecolor="#222222",
                        linewidth=1.2, width=0.58)
        axis.set_title(title)
        axis.grid(axis="y", color=PALETTE["grid"], linewidth=0.8, alpha=0.7)
        axis.set_axisbelow(True)
        for bar, value in zip(bars, values):
            axis.text(bar.get_x() + bar.get_width() / 2,
                      bar.get_height(),
                      fmt.format(value),
                      ha="center", va="bottom", fontsize=12)
    fig.suptitle("NR-01: Basic Reno vs NewReno", fontsize=19)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    comparison_dir = batch_dir / "comparison"
    comparison_dir.mkdir(parents=True, exist_ok=True)
    png_path = comparison_dir / "newreno_vs_reno.png"
    svg_path = comparison_dir / "newreno_vs_reno.svg"
    fig.savefig(str(png_path), dpi=300, bbox_inches="tight")
    fig.savefig(str(svg_path), bbox_inches="tight")
    plt.close(fig)
    return png_path, svg_path


def command_compare(args):
    batch_dir = Path(args.batch_dir).resolve() if args.batch_dir else latest_batch_dir()
    baseline_files = collect_metric_files(batch_dir, "baseline")
    newreno_files = collect_metric_files(batch_dir, "newreno")
    baseline, baseline_runs = average_metrics(baseline_files)
    newreno, newreno_runs = average_metrics(newreno_files)

    csv_path = write_comparison_csv(batch_dir, baseline, newreno)
    md_path = write_comparison_markdown(
        batch_dir, baseline, newreno, baseline_runs, newreno_runs)
    png_path, svg_path = plot_comparison(batch_dir, baseline, newreno)

    log("对比 CSV: %s" % csv_path)
    log("对比说明: %s" % md_path)
    log("对比图 PNG: %s" % png_path)
    log("对比图 SVG: %s" % svg_path)


def parse_args():
    parser = argparse.ArgumentParser(description="NewReno 对比实验工具")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="采集一个版本的 NR-01 数据")
    run_parser.add_argument("--variant", choices=("baseline", "newreno"), required=True)
    run_parser.add_argument("--scenario", choices=tuple(SCENARIOS), default="NR-01")
    run_parser.add_argument("--batch-dir", default=None,
                            help="批次目录；默认创建带时间戳的新目录")
    run_parser.add_argument("--run-id", default="r01")
    run_parser.add_argument("--timeout", type=int, default=300)
    run_parser.set_defaults(func=command_run)

    compare_parser = subparsers.add_parser("compare", help="比较同一批次的两组数据")
    compare_parser.add_argument("--batch-dir", default=None,
                                help="未指定时选择最新的 NR-01-* 批次")
    compare_parser.set_defaults(func=command_compare)
    return parser.parse_args()


def main():
    args = parse_args()
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
