# -*- coding: UTF-8 -*-
import argparse
import json
import os
import time

SMSS = 1380
SYN = 8
ACK = 4
FIN = 2
SYN_ACK = SYN | ACK
FIN_ACK = FIN | ACK


def info_seq(seq, ack, flag, length):
    return 'seq:%d ack:%d flag:%d length:%d' % (seq, ack, flag, length)


class Trace:
    def __init__(self, start_us):
        self.start_us = start_us
        self.events = {'client': [], 'server': []}

    def add(self, actor, offset_us, event, info):
        self.events[actor].append(
            (self.start_us + int(offset_us), event, info)
        )

    def send(self, actor, offset_us, seq, ack, flag, length):
        self.add(actor, offset_us, 'SEND', info_seq(seq, ack, flag, length))

    def recv(self, actor, offset_us, seq, ack, flag, length):
        self.add(actor, offset_us, 'RECV', info_seq(seq, ack, flag, length))

    def cwnd(self, actor, offset_us, change_type, size):
        self.add(actor, offset_us, 'CWND', 'type:%d size:%d'
                 % (change_type, size))

    def rwnd(self, actor, offset_us, size):
        self.add(actor, offset_us, 'RWND', 'size:%d' % size)

    def swnd(self, actor, offset_us, size):
        self.add(actor, offset_us, 'SWND', 'size:%d' % size)

    def rtts(self, actor, offset_us, sample_ms, estimated_ms,
             deviation_ms, timeout_ms):
        self.add(
            actor,
            offset_us,
            'RTTS',
            'SampleRTT:%.6f EstimatedRTT:%.6f '
            'DeviationRTT:%.6f TimeoutInterval:%.6f'
            % (sample_ms, estimated_ms, deviation_ms, timeout_ms),
        )

    def deliver(self, actor, offset_us, seq, size):
        self.add(actor, offset_us, 'DELV', 'seq:%d size:%d' % (seq, size))


def write_pair(output_dir, name, trace, manifest):
    os.makedirs(output_dir, exist_ok=True)
    paths = {}

    for actor in ('client', 'server'):
        path = os.path.join(output_dir, '%s_%s.event.trace'
                            % (name, actor))
        with open(path, 'w', encoding='utf-8', newline='\n') as output:
            for timestamp, event, info in sorted(trace.events[actor]):
                output.write('[%d] [%s] [%s]\n'
                             % (timestamp, event, info))
        paths[actor] = path

    manifest[name] = {
        'client_trace': paths['client'],
        'server_trace': paths['server'],
        'synthetic': True,
    }


def handshake(trace, syn_seq=1000, synack_seq=2000):
    client_ack = synack_seq + 1
    server_ack = syn_seq + 1

    trace.send('client', 0, syn_seq, 0, SYN, 0)
    trace.recv('server', 100, syn_seq, 0, SYN, 0)
    trace.rwnd('server', 120, 10 * SMSS)
    trace.send('server', 150, synack_seq, server_ack, SYN_ACK, 0)
    trace.recv('client', 250, synack_seq, server_ack, SYN_ACK, 0)
    trace.swnd('client', 260, 10 * SMSS)
    trace.send('client', 300, server_ack, client_ack, ACK, 0)
    trace.recv('server', 400, server_ack, client_ack, ACK, 0)
    trace.swnd('server', 410, 10 * SMSS)
    return server_ack, client_ack


def scenario_normal():
    trace = Trace(int(time.time() * 1000000))
    client_seq, server_seq = handshake(trace)
    advertised = 10 * SMSS
    now = 1000000

    for batch in range(4):
        for _ in range(10):
            seq = client_seq
            trace.send('client', now, seq, server_seq, 0, SMSS)
            trace.recv('server', now + 20000, seq, server_seq, 0, SMSS)
            advertised -= SMSS
            trace.rwnd('server', now + 21000, advertised)
            trace.deliver('server', now + 22000, seq, SMSS)
            trace.send('server', now + 24000, server_seq, seq + SMSS,
                       ACK, 0)
            trace.recv('client', now + 26000, server_seq, seq + SMSS,
                       ACK, 0)
            trace.swnd('client', now + 27000, advertised)
            client_seq = seq + SMSS
            now += 25000

        now += 50000
        advertised = 10 * SMSS
        trace.rwnd('server', now, advertised)
        trace.send('server', now + 10000, server_seq, client_seq, ACK, 0)
        trace.recv('client', now + 25000, server_seq, client_seq,
                   ACK, 0)
        trace.swnd('client', now + 26000, advertised)
        trace.rtts('client', now + 27000, 24 + batch, 24 + batch * 0.5,
                   4 + batch, 1000)
        now += 100000

    return trace


def scenario_fast_retransmit():
    trace = Trace(int(time.time() * 1000000))
    client_seq, server_seq = handshake(trace, 3000, 4000)
    advertised = 6 * SMSS
    now = 1000000
    sequences = [client_seq + i * SMSS for i in range(6)]
    dropped = sequences[2]

    for index, seq in enumerate(sequences):
        trace.send('client', now + index * 20000, seq, server_seq, 0, SMSS)
        if seq == dropped:
            continue

        trace.recv('server', now + index * 20000 + 5000,
                   seq, server_seq, 0, SMSS)
        advertised -= SMSS
        trace.rwnd('server', now + index * 20000 + 6000, advertised)
        trace.deliver('server', now + index * 20000 + 7000, seq, SMSS)

        expected = dropped if seq > dropped else seq + SMSS
        trace.send('server', now + index * 20000 + 9000,
                   server_seq, expected, ACK, 0)
        trace.recv('client', now + index * 20000 + 11000,
                   server_seq, expected, ACK, 0)
        trace.swnd('client', now + index * 20000 + 12000, advertised)

    for offset in (140000, 160000, 180000):
        trace.recv('client', now + offset, server_seq, dropped, ACK, 0)
    trace.send('client', now + 200000, dropped, server_seq, 0, SMSS)
    trace.recv('server', now + 205000, dropped, server_seq, 0, SMSS)
    trace.deliver('server', now + 206000, dropped, SMSS)
    trace.send('server', now + 208000, server_seq, sequences[-1] + SMSS,
               ACK, 0)
    trace.recv('client', now + 210000, server_seq,
               sequences[-1] + SMSS, ACK, 0)
    trace.rtts('client', now + 212000, 31, 30, 5, 1000)
    return trace


def scenario_rto_timeout():
    trace = Trace(int(time.time() * 1000000))
    client_seq, server_seq = handshake(trace, 5000, 6000)
    lost_seq = client_seq
    trace.send('client', 1000000, lost_seq, server_seq, 0, SMSS)
    trace.send('client', 2000000, lost_seq, server_seq, 0, SMSS)
    trace.send('client', 4000000, lost_seq, server_seq, 0, SMSS)
    trace.recv('server', 4005000, lost_seq, server_seq, 0, SMSS)
    trace.rwnd('server', 4006000, 9 * SMSS)
    trace.deliver('server', 4007000, lost_seq, SMSS)
    trace.send('server', 4009000, server_seq, lost_seq + SMSS, ACK, 0)
    trace.recv('client', 4011000, server_seq, lost_seq + SMSS, ACK, 0)

    next_seq = lost_seq + SMSS
    trace.send('client', 4050000, next_seq, server_seq, 0, SMSS)
    trace.recv('server', 4055000, next_seq, server_seq, 0, SMSS)
    trace.deliver('server', 4056000, next_seq, SMSS)
    trace.send('server', 4058000, server_seq, next_seq + SMSS, ACK, 0)
    trace.recv('client', 4060000, server_seq, next_seq + SMSS, ACK, 0)
    trace.rtts('client', 4062000, 42, 40, 8, 1000)
    return trace


def scenario_rtt_rto():
    trace = Trace(int(time.time() * 1000000))
    client_seq, server_seq = handshake(trace, 6500, 7500)
    advertised = 8 * SMSS
    samples = (
        (1100, 1100, 550, 3300),
        (1300, 1125, 462.5, 2975),
        (1500, 1171.875, 440.625, 2934.375),
        (900, 1137.891, 398.438, 2731.646),
    )

    for index, (sample, estimated, deviation, timeout) in enumerate(samples):
        now = 1000000 + index * 1600000
        seq = client_seq + index * SMSS
        trace.send('client', now, seq, server_seq, 0, SMSS)
        trace.recv('server', now + 500000, seq, server_seq, 0, SMSS)
        advertised = max(0, advertised - SMSS)
        trace.rwnd('server', now + 520000, advertised)
        trace.deliver('server', now + 540000, seq, SMSS)
        trace.send('server', now + 560000, server_seq, seq + SMSS, ACK, 0)
        trace.recv('client', now + 580000, server_seq, seq + SMSS, ACK, 0)
        trace.swnd('client', now + 600000, advertised)
        trace.rtts('client', now + 620000, sample, estimated,
                   deviation, timeout)

    return trace


def scenario_zero_window():
    trace = Trace(int(time.time() * 1000000))
    client_seq, server_seq = handshake(trace, 7000, 8000)
    advertised = 4 * SMSS
    now = 1000000

    for _ in range(4):
        seq = client_seq
        trace.send('client', now, seq, server_seq, 0, SMSS)
        trace.recv('server', now + 10000, seq, server_seq, 0, SMSS)
        advertised -= SMSS
        trace.rwnd('server', now + 11000, advertised)
        trace.deliver('server', now + 12000, seq, SMSS)
        trace.send('server', now + 14000, server_seq, seq + SMSS, ACK, 0)
        trace.recv('client', now + 16000, server_seq, seq + SMSS, ACK, 0)
        trace.swnd('client', now + 17000, advertised)
        client_seq = seq + SMSS
        now += 30000

    probe = client_seq
    trace.send('client', 2500000, probe, server_seq, 0, 1)
    trace.recv('server', 2505000, probe, server_seq, 0, 1)
    trace.rwnd('server', 2506000, 0)
    trace.send('server', 2508000, server_seq, probe, ACK, 0)
    trace.recv('client', 2510000, server_seq, probe, ACK, 0)

    trace.send('client', 4500000, probe, server_seq, 0, 1)
    trace.recv('server', 4505000, probe, server_seq, 0, 1)
    trace.rwnd('server', 4506000, 0)
    trace.send('server', 4508000, server_seq, probe, ACK, 0)
    trace.recv('client', 4510000, server_seq, probe, ACK, 0)

    advertised = 4 * SMSS
    trace.rwnd('server', 5500000, advertised)
    trace.send('server', 5502000, server_seq, probe, ACK, 0)
    trace.recv('client', 5504000, server_seq, probe, ACK, 0)
    trace.swnd('client', 5505000, advertised)
    trace.send('client', 5550000, probe, server_seq, 0, SMSS)
    trace.recv('server', 5555000, probe, server_seq, 0, SMSS)
    trace.deliver('server', 5556000, probe, SMSS)
    trace.send('server', 5558000, server_seq, probe + SMSS, ACK, 0)
    trace.recv('client', 5560000, server_seq, probe + SMSS, ACK, 0)
    trace.rtts('client', 5562000, 31, 30, 6, 1000)
    return trace


def scenario_close():
    trace = Trace(int(time.time() * 1000000))
    client_seq, server_seq = handshake(trace, 9000, 10000)
    trace.send('client', 1000000, client_seq, server_seq, 0, 12)
    trace.recv('server', 1005000, client_seq, server_seq, 0, 12)
    trace.deliver('server', 1006000, client_seq, 12)
    trace.send('server', 1008000, server_seq, client_seq + 12, ACK, 0)
    trace.recv('client', 1010000, server_seq, client_seq + 12, ACK, 0)

    trace.send('client', 2000000, client_seq + 12,
               server_seq, FIN_ACK, 0)
    trace.recv('server', 2005000, client_seq + 12,
               server_seq, FIN_ACK, 0)
    trace.send('server', 2008000, server_seq, client_seq + 13, ACK, 0)
    trace.recv('client', 2010000, server_seq,
               client_seq + 13, ACK, 0)

    trace.send('server', 3000000, server_seq,
               client_seq + 13, FIN_ACK, 0)
    trace.recv('client', 3005000, server_seq,
               client_seq + 13, FIN_ACK, 0)
    trace.send('client', 3008000, client_seq + 13,
               server_seq + 1, ACK, 0)
    trace.recv('server', 3010000, client_seq + 13,
               server_seq + 1, ACK, 0)
    return trace


SCENARIOS = {
    'normal': scenario_normal,
    'fast_retransmit': scenario_fast_retransmit,
    'rto_timeout': scenario_rto_timeout,
    'rtt_rto': scenario_rtt_rto,
    'zero_window': scenario_zero_window,
    'close': scenario_close,
}

SCENARIO_NOTES = {
    'normal': {
        'purpose': '正常发送、ACK 推进、窗口变化和 RTT 更新',
        'expected': '发送序号持续前进，DELV 与发送字节对应，RTT 样本有限且稳定',
    },
    'fast_retransmit': {
        'purpose': '三个重复 ACK 触发快速重传',
        'expected': '丢失序号在三次重复 ACK 后立即重发，随后累计 ACK 跳过该序号',
    },
    'rto_timeout': {
        'purpose': '验证 RTO 超时和指数退避',
        'expected': '同一序号约在 1 秒和 2 秒后重发，重传段不产生 RTT 样本',
    },
    'rtt_rto': {
        'purpose': '验证 RTO 随大于 1 秒的 RTT 样本变化',
        'expected': 'SampleRTT 和 TimeoutInterval 均超过 1000 ms 且逐次变化',
    },
    'zero_window': {
        'purpose': '验证零窗口探测和窗口恢复',
        'expected': 'RWND 和 SWND 降为 0，发送 1 字节探测，窗口恢复后继续传输',
    },
    'close': {
        'purpose': '验证 FIN 前的数据交付和双向 FIN/ACK 顺序',
        'expected': 'DELV 出现在 FIN 前，双方 FIN 均得到 ACK',
    },
}


def parse_args():
    parser = argparse.ArgumentParser(
        description='生成确定性的 TJU_TCP event trace 模拟场景'
    )
    parser.add_argument(
        '--output-dir',
        default='test/simulated',
        help='trace 输出目录，默认 test/simulated',
    )
    parser.add_argument(
        '--scenario',
        choices=('all',) + tuple(SCENARIOS),
        default='all',
        help='选择生成一个场景或全部场景',
    )
    return parser.parse_args()


def main():
    args = parse_args()
    names = SCENARIOS.keys() if args.scenario == 'all' else [args.scenario]
    manifest = {
        'notice': 'Synthetic traces for deterministic behavior checks.',
        'smss': SMSS,
        'scenarios': {},
    }

    for name in names:
        trace = SCENARIOS[name]()
        write_pair(args.output_dir, name, trace, manifest['scenarios'])
        manifest['scenarios'][name].update(SCENARIO_NOTES[name])
        print('已生成场景 %s：%s/%s_*.event.trace'
              % (name, args.output_dir, name))

    manifest_path = os.path.join(args.output_dir, 'manifest.json')
    with open(manifest_path, 'w', encoding='utf-8') as output:
        json.dump(manifest, output, indent=2, ensure_ascii=False)
        output.write('\n')
    print('场景说明：%s' % manifest_path)


if __name__ == '__main__':
    main()
