# -*- coding: UTF-8 -*-
import argparse
import math
import os
import re
import sys

import matplotlib.pyplot as plt
import numpy as np

OUTPUT_DIR = '.'
SMSS = 1380
FONT_SIZE = 15
LEGACY_OUTPUT_FILES = (
    'ReceiveWindowSize_VS_Time.png',
    'SendWindowSize_VS_Time.png',
    'CongestionWindowSize_VS_Time.png',
    'AllWindowSize_VS_Time.png',
    'RTT_VS_Time.png',
    'RTT.png',
    'Throuput.png',
)
OUTPUT_SUFFIXES = (
    'ReceiveWindowSize_VS_Time.png',
    'SendWindowSize_VS_Time.png',
    'CongestionWindowSize_VS_Time.png',
    'AllWindowSize_VS_Time.png',
    'RTT_VS_Time.png',
    'Throuput.png',
)
TRACE_RE = re.compile(
    r'^\[(\d+)\]\s+\[(SEND|RECV|CWND|RWND|SWND|RTTS|DELV)\]\s+\[(.+)\]$'
)


def parse_info(text):
    fields = {}
    for item in text.split():
        if ':' not in item:
            continue
        key, value = item.split(':', 1)
        fields[key] = value
    return fields


def build_step_series(time_list, values, end_time):
    if not len(time_list):
        return [], []

    step_times = [time_list[0]]
    step_values = [values[0]]
    for time_value, value in zip(time_list[1:], values[1:]):
        if value != step_values[-1]:
            step_times.append(time_value)
            step_values.append(value)

    final_time = max(end_time, step_times[-1])
    if final_time == step_times[-1]:
        final_time += 1e-6
    step_times.append(final_time)
    step_values.append(step_values[-1])
    return step_times, step_values


def plot_win(time_list, win_list, win_type, end_time,
             output_prefix, change_types=None):
    step_times, step_values = build_step_series(
        time_list,
        win_list,
        end_time,
    )
    if not step_times:
        return

    plt.step(step_times, step_values, where='post', color='black')
    plt.margins(x=0.01, y=0.05)
    plt.xlabel('Time (s)', fontdict={'size': FONT_SIZE})
    plt.ylabel('%s Window Size (segment)' % win_type,
               fontdict={'size': FONT_SIZE})

    if win_type == 'Congestion':
        colors = {0: 'red', 1: 'green', 2: 'blue', 3: 'cyan'}
        labels = {
            0: 'slow start',
            1: 'congestion avoidance',
            2: 'fast retransmit',
            3: 'timeout',
        }
        types = np.array(change_types if change_types is not None else [])
        for item in range(4):
            indexes = np.argwhere(types == item).reshape(-1)
            if indexes.size:
                plt.scatter(
                    np.asarray(time_list)[indexes],
                    np.asarray(win_list)[indexes],
                    c=colors[item],
                    label=labels[item],
                )
        plt.legend()

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    output = os.path.join(
        OUTPUT_DIR,
        '%s_%sWindowSize_VS_Time.png' % (output_prefix, win_type),
    )
    plt.savefig(output, dpi=600)
    print('绘制成功, 图像位于%s' % output)
    plt.cla()


def plot_all(cwnd_list, rwnd_list, swnd_list, end_time, output_prefix):
    plotted = False

    if len(cwnd_list[0]):
        times, values = build_step_series(
            cwnd_list[0], cwnd_list[1], end_time
        )
        plt.step(times, values, where='post', color='red', label='cwnd')
        plotted = True
    if len(rwnd_list[0]):
        times, values = build_step_series(
            rwnd_list[0], rwnd_list[1], end_time
        )
        plt.step(times, values, where='post', color='green', label='rwnd')
        plotted = True
    if len(swnd_list[0]):
        times, values = build_step_series(
            swnd_list[0], swnd_list[1], end_time
        )
        plt.step(times, values, where='post', color='blue', label='swnd')
        plotted = True

    if not plotted:
        return

    plt.legend(loc=0, numpoints=1)
    plt.xlabel('Time (s)', fontdict={'size': FONT_SIZE})
    plt.ylabel('Window Size (segment)', fontdict={'size': FONT_SIZE})
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    output = os.path.join(
        OUTPUT_DIR,
        '%s_AllWindowSize_VS_Time.png' % output_prefix,
    )
    plt.savefig(output, dpi=600)
    print('绘制成功, 图像位于%s' % output)
    plt.cla()


def plot_rtt(time_list, sample_rtt, estimated_rtt,
             deviation_rtt, timeout_interval, output_prefix):
    plt.plot(time_list, sample_rtt, color='red', marker='o',
             markersize=5, label='SampleRTT')
    plt.plot(time_list, estimated_rtt, color='green', marker='o',
             markersize=5, label='EstimatedRTT')
    plt.plot(time_list, deviation_rtt, color='blue', marker='o',
             markersize=5, label='DeviationRTT')
    plt.plot(time_list, timeout_interval, color='black',
             marker='o', markersize=5, label='TimeoutInterval')
    plt.legend()
    plt.xlabel('Time (s)', fontdict={'size': FONT_SIZE})
    plt.ylabel('Time (ms)', fontdict={'size': FONT_SIZE})
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    output = os.path.join(
        OUTPUT_DIR,
        '%s_RTT_VS_Time.png' % output_prefix,
    )
    plt.savefig(output, dpi=600)
    print('绘制成功, 图像位于%s' % output)
    plt.cla()


def plot_throughput(time_list, throughput_list, interval, output_prefix):
    if not throughput_list:
        return

    plt.plot(time_list, throughput_list, color='black',
             marker='o', markersize=4)
    plt.xlabel('Time (s)', fontdict={'size': FONT_SIZE})
    plt.ylabel('throuput (bps)', fontdict={'size': FONT_SIZE})
    maximum = max(throughput_list)
    plt.ylim(ymin=0, ymax=(maximum * 1.05) if maximum > 0 else 1)
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    output = os.path.join(
        OUTPUT_DIR,
        '%s_Throuput.png' % output_prefix,
    )
    plt.savefig(output, dpi=600)
    print('绘制成功, 图像位于%s [注: 每%.3fs计算一次瞬时吞吐率]'
          % (output, interval))
    plt.cla()


def read_trace(file_path):
    send = {'utctime': [], 'seq': [], 'ack': [], 'flag': [], 'length': []}
    recv = {'utctime': [], 'seq': [], 'ack': [], 'flag': [], 'length': []}
    cwnd = {'utctime': [], 'type': [], 'size': []}
    rwnd = {'utctime': [], 'size': []}
    swnd = {'utctime': [], 'size': []}
    rtts = {
        'utctime': [],
        'SampleRTT': [],
        'EstimatedRTT': [],
        'DeviationRTT': [],
        'TimeoutInterval': [],
    }
    delv = {'utctime': [], 'seq': [], 'size': [], 'throughput': []}
    start_time = None

    with open(file_path, 'r', encoding='utf-8-sig') as trace_file:
        for line in trace_file:
            match = TRACE_RE.match(line.strip())
            if match is None:
                continue

            timestamp = int(match.group(1))
            event = match.group(2)
            info = parse_info(match.group(3))
            if start_time is None:
                start_time = timestamp

            if event == 'SEND':
                send['utctime'].append(timestamp)
                send['seq'].append(int(info['seq']))
                send['ack'].append(int(info['ack']))
                send['flag'].append(int(info['flag']))
                send['length'].append(int(info['length']))
            elif event == 'RECV':
                recv['utctime'].append(timestamp)
                recv['seq'].append(int(info['seq']))
                recv['ack'].append(int(info['ack']))
                recv['flag'].append(int(info['flag']))
                recv['length'].append(int(info['length']))
            elif event == 'CWND':
                cwnd['utctime'].append(timestamp)
                cwnd['type'].append(int(info['type']))
                cwnd['size'].append(int(info['size']) / SMSS)
            elif event == 'RWND':
                rwnd['utctime'].append(timestamp)
                rwnd['size'].append(int(info['size']) / SMSS)
            elif event == 'SWND':
                swnd['utctime'].append(timestamp)
                swnd['size'].append(int(info['size']) / SMSS)
            elif event == 'RTTS':
                if timestamp not in rtts['utctime']:
                    rtts['utctime'].append(timestamp)
                    rtts['SampleRTT'].append(float(info['SampleRTT']))
                    rtts['EstimatedRTT'].append(float(info['EstimatedRTT']))
                    rtts['DeviationRTT'].append(float(info['DeviationRTT']))
                    rtts['TimeoutInterval'].append(
                        float(info['TimeoutInterval'])
                    )
            elif event == 'DELV':
                delv['utctime'].append(timestamp)
                delv['seq'].append(int(info['seq']))
                delv['size'].append(int(info['size']))

    if start_time is None:
        start_time = 0

    for series in (send, recv, cwnd, rwnd, swnd, rtts, delv):
        series['time'] = np.divide(
            [item - start_time for item in series['utctime']],
            1000000,
        )
    return send, recv, cwnd, rwnd, swnd, rtts, delv


def crop_series(series, start_time, end_time):
    times = np.asarray(series['time'])
    if end_time is None:
        selected = times >= start_time
        plot_end = None
    else:
        selected = (times >= start_time) & (times < end_time)
        plot_end = end_time - start_time

    cropped = {}
    for key, values in series.items():
        if key == 'time':
            cropped[key] = times[selected] - start_time
        elif key == 'throughput':
            cropped[key] = []
        else:
            cropped[key] = list(np.asarray(values)[selected])
    return cropped, plot_end


def parse_args():
    parser = argparse.ArgumentParser(
        description='根据 TJU_TCP event trace 绘制窗口、RTT 和吞吐率图'
    )
    parser.add_argument(
        'trace_file',
        nargs='?',
        default='./client.event.trace',
        help='event trace 文件路径',
    )
    parser.add_argument(
        '--start',
        type=float,
        default=0.0,
        help='相对 trace 第一条事件的起始秒数',
    )
    parser.add_argument(
        '--duration',
        type=float,
        default=None,
        help='绘图区间长度，单位秒；不指定则绘制到文件末尾',
    )
    args = parser.parse_args()
    if args.start < 0:
        parser.error('--start 不能小于 0')
    if args.duration is not None and args.duration <= 0:
        parser.error('--duration 必须大于 0')
    return args


def main():
    args = parse_args()
    file_to_read = args.trace_file

    print('正在使用 %s Trace文件绘图' % file_to_read)
    send, recv, cwnd, rwnd, swnd, rtts, delv = read_trace(file_to_read)
    end_time = (
        args.start + args.duration
        if args.duration is not None
        else None
    )
    send, _ = crop_series(send, args.start, end_time)
    recv, _ = crop_series(recv, args.start, end_time)
    cwnd, _ = crop_series(cwnd, args.start, end_time)
    rwnd, selected_end = crop_series(rwnd, args.start, end_time)
    swnd, _ = crop_series(swnd, args.start, end_time)
    rtts, _ = crop_series(rtts, args.start, end_time)
    delv, _ = crop_series(delv, args.start, end_time)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    trace_name = os.path.basename(file_to_read)
    if trace_name.endswith('.event.trace'):
        trace_name = trace_name[:-len('.event.trace')]
    else:
        trace_name = os.path.splitext(trace_name)[0]
    if args.duration is not None:
        trace_name = '%s_%g-%gs' % (
            trace_name,
            args.start,
            args.duration,
        )
    elif args.start > 0:
        trace_name = '%s_from-%gs' % (trace_name, args.start)

    for output_name in LEGACY_OUTPUT_FILES:
        output_path = os.path.join(OUTPUT_DIR, output_name)
        if os.path.exists(output_path):
            os.remove(output_path)
    for output_suffix in OUTPUT_SUFFIXES:
        output_path = os.path.join(
            OUTPUT_DIR,
            '%s_%s' % (trace_name, output_suffix),
        )
        if os.path.exists(output_path):
            os.remove(output_path)

    series = (send, recv, cwnd, rwnd, swnd, rtts, delv)
    if selected_end is None:
        selected_end = max(
            (item['time'][-1] for item in series if len(item['time'])),
            default=0,
        )
    if not any(len(item['time']) for item in series):
        print('指定区间内没有事件')
        return

    if len(cwnd['utctime']):
        plot_win(cwnd['time'], np.asarray(cwnd['size']),
                 'Congestion', selected_end, trace_name, cwnd['type'])
    else:
        print('未找到 CWND 事件，跳过拥塞窗口图')
    if len(rwnd['utctime']):
        plot_win(rwnd['time'], rwnd['size'], 'Receive',
                 selected_end, trace_name)
    else:
        print('未找到 RWND 事件，跳过接收窗口图')
    if len(swnd['utctime']):
        plot_win(swnd['time'], swnd['size'], 'Send',
                 selected_end, trace_name)
    else:
        print('未找到 SWND 事件，跳过发送窗口图')

    plot_all(
        [cwnd['time'], cwnd['size']],
        [rwnd['time'], rwnd['size']],
        [swnd['time'], swnd['size']],
        selected_end,
        trace_name,
    )

    if len(rtts['utctime']):
        plot_rtt(
            rtts['time'],
            rtts['SampleRTT'],
            rtts['EstimatedRTT'],
            rtts['DeviationRTT'],
            rtts['TimeoutInterval'],
            trace_name,
        )
    else:
        print('未找到 RTTS 事件，跳过 RTT 图')

    if len(delv['utctime']):
        throughput_interval = 1
        time_start = 0
        duration = max(0, delv['time'][-1] - time_start)
        interval_count = max(1, int(math.ceil(duration)))
        throughput = []
        delv_times = np.asarray(delv['time'])
        delv_sizes = np.asarray(delv['size'])

        for _ in range(interval_count):
            time_end = time_start + throughput_interval
            selected = (delv_times >= time_start) & (delv_times < time_end)
            throughput.append(
                float(np.sum(delv_sizes[selected]) * 8 / throughput_interval)
            )
            time_start += throughput_interval

        plot_throughput(
            range(interval_count),
            throughput,
            throughput_interval,
            trace_name,
        )
    else:
        print('未找到 DELV 事件，跳过吞吐率图')


if __name__ == '__main__':
    main()
