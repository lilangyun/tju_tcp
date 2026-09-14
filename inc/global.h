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

/* 第二阶段暂不实现拥塞控制，仅保留状态常量供后续阶段使用。 */
#define SLOW_START 0
#define CONGESTION_AVOIDANCE 1
#define FAST_RECOVERY 2

#define TCP_SENDWN_SIZE (32 * SMSS)
#define TCP_RECVWN_SIZE (32 * SMSS)

/*
 * 发送窗口。
 *
 * base_seq 指向最早已发送但尚未累计确认的字节，next_seq 指向下一个
 * 尚未发送的字节。二者之差就是当前 FlightSize。
 */
typedef struct {
	uint16_t window_size;        /* 对端最近通告的接收窗口 rwnd。 */
	uint32_t base_seq;           /* 最早未确认序列号。 */
	uint32_t next_seq;           /* 下一个待发送序列号。 */

	struct timeval send_time;    /* 保留字段；当前 RTT 计时使用 rtt_send_time。 */
	struct timeval timeout;      /* 下次使用 setitimer 的间隔。 */
	int ack_cnt;                 /* 连续重复 ACK 数。 */

	/* RFC 6298 RTT/RTO 状态，单位均为微秒。 */
	int has_rtt_sample;
	uint64_t srtt_us;
	uint64_t rttvar_us;
	uint64_t rto_us;
	uint64_t current_rto_us;
	uint64_t last_rtt_sample_time_us;
	struct timeval rtt_send_time;
	uint32_t rtt_seq;
	int rtt_sample_pending;
	int rtt_retransmitted;

	int data_timer_active;        /* 最早未确认数据定时器是否运行。 */
	int zero_window_timer_active; /* 零窗口探测定时器是否运行。 */
} sender_window_t;

/*
 * 接收窗口。
 *
 * expect_seq 是接收端下一个期望的字节序号。last_advertised_window 用于
 * 接收端 SWS 避免：窗口从 0 恢复时，必须先达到可接收阈值才重新通告。
 */
typedef struct {
	uint16_t window_size;
	uint32_t expect_seq;
	uint16_t last_advertised_window;
} receiver_window_t;

typedef struct {
	uint32_t ip;
	uint16_t port;
} tju_sock_addr;

/*
 * 发送缓冲区采用环形数组。
 *
 * base 指向最早未确认数据，next 指向下一个待发送数据，store 指向应用
 * 下一次写入位置；count 表示缓冲区中尚未确认的总字节数。
 */
typedef struct {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	char* mem;
	int capacity;
	int base;
	int next;
	int store;
	int count;
} send_buf_t;

/*
 * 接收缓冲区只保存已经按序到达的数据。head 指向应用下一次读取位置，
 * expect 指向内核下一次写入位置，count 为尚未被应用读取的字节数。
 */
typedef struct {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	char* mem;
	int capacity;
	int head;
	int expect;
	int count;
} recv_buf_t;

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

	send_buf_t* send_buf;
	recv_buf_t* recv_buf;
	sender_window_t* send_wnd;
	receiver_window_t* recv_wnd;

	pthread_t send_thread_id;
	pthread_t retry_thread_id;
} tju_tcp_t;

#endif
