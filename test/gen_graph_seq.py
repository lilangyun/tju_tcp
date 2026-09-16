from scapy.all import IP, UDP, Raw, rdpcap
import matplotlib.pyplot as plt
import os
import sys

OUTPUT_FILE = '/vagrant/tju_tcp/test/SeqNum_VS_Time.png'
SERVER_PORT = 20218
SEQ_MODULUS = 1 << 32
SEQ_HALF = 1 << 31


def unwrap_sequences(sequence_numbers):
    unwrapped = []
    offset = 0
    previous = None

    for sequence_number in sequence_numbers:
        if previous is not None and sequence_number < previous - SEQ_HALF:
            offset += SEQ_MODULUS
        value = sequence_number + offset
        unwrapped.append(value)
        previous = value

    return unwrapped


def main():
    file_to_read = '/home/vagrant/server.pcap'
    if len(sys.argv) >= 2:
        file_to_read = sys.argv[1]

    print('正在使用 %s 抓包结果绘图' % file_to_read)
    packets = rdpcap(file_to_read)
    sequence_numbers = []
    times = []

    for packet in packets:
        if IP not in packet or UDP not in packet or Raw not in packet:
            continue
        if packet[UDP].dport != SERVER_PORT:
            continue

        payload = bytes(packet[Raw].load)
        if len(payload) < 20:
            continue

        packet_length = int.from_bytes(payload[14:16], byteorder='big')
        if packet_length <= 20 or packet_length > len(payload):
            continue

        sequence_numbers.append(
            int.from_bytes(payload[4:8], byteorder='big')
        )
        times.append(packet.time)

    if not sequence_numbers:
        raise SystemExit('未找到目标端口 20218 的有效 TJU_TCP 数据报文')

    base_time = times[0]
    relative_times = [packet_time - base_time for packet_time in times]
    unwrapped_sequences = unwrap_sequences(sequence_numbers)

    plt.plot(relative_times, unwrapped_sequences)
    plt.xlabel('Time (s)', fontdict={'size': 24})
    plt.ylabel('Sequence Number', fontdict={'size': 24})
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    plt.savefig(OUTPUT_FILE, dpi=600)
    print('绘制成功, 图像位于%s' % OUTPUT_FILE)


if __name__ == '__main__':
    main()
