#ifndef _GLOBAL_H_
#define _GLOBAL_H_

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

/* 基本长度单位：字节。 */
#define SIZE32 4
#define SIZE16 2
#define SIZE8  1

/* 常用布尔值和状态返回值。 */
#define NO_FLAG 0
#define NO_WAIT 1
#define TIMEOUT 2
#define FALSE 0
#define TRUE 1

/* 单个 TCP 报文的数据和总长度上限，避免 IP 层分片。 */
#define MAX_DLEN 1380
#define MAX_LEN 1400

/* TCP 连接状态。 */
#define CLOSED 0
#define LISTEN 1
#define SYN_SENT 2
#define SYN_RECV 3
#define ESTABLISHED 4
#define FIN_WAIT_1 5
#define FIN_WAIT_2 6
#define CLOSE_WAIT 7
#define CLOSING 8
#define LAST_ACK 9
#define TIME_WAIT 10

/* 基础 Reno 状态和 Trace CWND 事件类型。 */
#define SLOW_START 0
#define CONGESTION_AVOIDANCE 1
#define FAST_RECOVERY 2

#define TCP_SEND_WINDOW_SIZE (32 * SMSS)
#define TCP_RECV_WINDOW_SIZE (32 * SMSS)

/*
 * 发送窗口。
 *
 * base_seq 指向最早已发送但尚未累计确认的字节，next_seq 指向下一个
 * 尚未发送的字节。二者之差就是当前 FlightSize。
 */
typedef struct {
	uint16_t rwnd;              /* 对端最近通告的接收窗口。 */
	uint32_t base_seq;           /* 最早未确认序列号。 */
	uint32_t next_seq;           /* 下一个待发送序列号。 */

	/* RFC 5681 基础 Reno 状态，cwnd 和 ssthresh 的单位均为字节。 */
	uint32_t cwnd;               /* 拥塞窗口，字节。 */
	uint32_t ssthresh;           /* 慢启动阈值，字节。 */
	int congestion_state;        /* SLOW_START 或 CONGESTION_AVOIDANCE。 */
	int fast_retransmit_ack_pending; /* 等待快速重传后的首个新 ACK。 */

	struct timeval retransmit_timeout; /* 下次重传定时器的间隔。 */
	int duplicate_ack_count;           /* 连续重复 ACK 数。 */

	/* RFC 6298 RTT/RTO 状态，单位均为微秒。 */
	int has_rtt_sample;                    /* 是否已有有效 RTT 样本。 */
	uint64_t srtt_us;                      /* 平滑 RTT，微秒。 */
	uint64_t rttvar_us;                    /* RTT 偏差，微秒。 */
	uint64_t estimated_rto_us;             /* RFC 6298 估算的 RTO。 */
	uint64_t effective_rto_us;             /* 退避后实际定时使用的 RTO。 */
	uint64_t last_rtt_sample_time_us;      /* 最近一次有效采样时刻。 */
	struct timeval rtt_send_time;          /* 当前 RTT 测量起始时刻。 */
	uint32_t rtt_measurement_seq;          /* 当前测量对应报文序号。 */
	int rtt_measurement_pending;           /* 是否有 RTT 测量尚未完成。 */
	int rtt_measurement_retransmitted;     /* Karn 算法使用的重传标记。 */

	int data_retransmission_timer_active;        /* 最早未确认数据定时器是否运行。 */
	int zero_window_probe_timer_active; /* 零窗口探测定时器是否运行。 */
} send_window_t;

/*
 * 接收窗口。
 *
 * next_expected_seq 是接收端下一个期望的字节序号。last_advertised_window 用于
 * 接收端 SWS 避免：窗口从 0 恢复时，必须先达到可接收阈值才重新通告。
 */
typedef struct {
	uint16_t advertised_window;      /* 本端当前通告的接收窗口。 */
	uint32_t next_expected_seq;      /* 下一个期望接收的字节序号。 */
	uint16_t last_advertised_window; /* 上次通告窗口，用于 SWS 判断。 */
} receive_window_t;

typedef struct {
	uint32_t ip;
	uint16_t port;
} tju_sock_addr;

/*
 * 发送缓冲区采用环形数组。
 *
 * ack_base_index 指向最早未确认数据，send_next_index 指向下一个待发送数据，
 * write_index 指向应用下一次写入位置，unacked_bytes 表示尚未确认的字节数。
 */
typedef struct {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	char* mem;
	int capacity;
	int ack_base_index;
	int send_next_index;
	int write_index;
	int unacked_bytes;
} send_buffer_t;

/*
 * 接收缓冲区只保存已经按序到达的数据。read_index 指向应用下一次读取位置，
 * write_index 指向内核下一次写入位置，buffered_bytes 为尚未读取的字节数。
 */
typedef struct {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	char* mem;
	int capacity;
	int read_index;
	int write_index;
	int buffered_bytes;
} receive_buffer_t;

typedef struct {
	int state;
	int close_requested;
	pthread_mutex_t state_lock;

	uint32_t local_isn;
	uint32_t remote_isn;
	uint32_t local_fin_seq;
	uint32_t remote_fin_seq;

	tju_sock_addr bind_addr;
	tju_sock_addr established_local_addr;
	tju_sock_addr established_remote_addr;

	send_buffer_t* send_buffer;
	receive_buffer_t* receive_buffer;
	send_window_t* send_window;
	receive_window_t* receive_window;

	pthread_t send_thread_id;
	pthread_t retransmission_thread_id;
} tju_tcp_t;

#endif
