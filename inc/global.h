#ifndef _GLOBAL_H_
#define _GLOBAL_H_

#include <netinet/in.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "global.h"
#include <pthread.h>
#include <sys/select.h>
#include <arpa/inet.h>

// 信号处理
#include <signal.h>

// 单位是byte
#define SIZE32 4
#define SIZE16 2
#define SIZE8  1

// 一些Flag
#define NO_FLAG 0
#define NO_WAIT 1
#define TIMEOUT 2
#define TRUE 1
#define FALSE 0

// 定义最大包长 防止IP层分片
#define MAX_DLEN 1380 	// 最大包内数据长度
#define MAX_LEN 1400 	// 最大包长度

// TCP socket 状态定义
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

// TCP 拥塞控制状态
#define SLOW_START 0
#define CONGESTION_AVOIDANCE 1
#define FAST_RECOVERY 2

// TCP 发送窗口大小
// #define TCP_SENDWN_SIZE (3200 * MAX_DLEN)
#define TCP_SENDWN_SIZE (SEND_BUF_SIZE)

// TCP 接受窗口大小
// #define TCP_RECVWN_SIZE (3200 * MAX_DLEN) // 比如最多放32个满载数据包
#define TCP_RECVWN_SIZE (RECV_BUF_SIZE) // 比如最多放32个满载数据包

// TCP 发送窗口
typedef struct {
	uint16_t window_size;  // 窗口大小
  	uint32_t base_seq;     // 已经发送出去，但是没有ACK的首个Seq
	uint32_t next_seq;     // 仍未发送出去，但是准备发送的首个Seq
  	struct timeval timeout;  // 记录发送出去，但是没有ACK的全部数据的超时时间

	// uint32_t estmated_rtt;
  	// int ack_cnt;
  	// pthread_mutex_t ack_cnt_lock;
  	// struct timeval send_time;
  	// uint16_t rwnd;
  	// int congestion_status;
  	// uint16_t cwnd;
  	// uint16_t ssthresh;
} sender_window_t;

// TCP 接收窗口
typedef struct {
	uint16_t window_size; // 窗口大小
	uint32_t expect_seq;  // 接收方内核下一个想要接收的Seq

	// char received[TCP_RECVWN_SIZE];
	// received_packet_t* head;
	// char buf[TCP_RECVWN_SIZE];
	// uint8_t marked[TCP_RECVWN_SIZE];
} receiver_window_t;

typedef struct {
	uint32_t ip;
	uint16_t port;
} tju_sock_addr;

typedef struct {
    pthread_mutex_t lock; // 发送数据锁
	char* mem; // 发送数据缓存区
	int capacity; // 发送数据缓存容量
    int base;  // 内核在缓冲区重传旧数据处
    int next;  // 内核在缓冲区发送新数据处
    int store;  // 用户在缓冲区中存放新数据处
}send_buf_t;

typedef struct {
    pthread_mutex_t lock; // 接收缓冲区数据锁
	char* mem; // 接收数据缓存区
	int capacity; // 接收数据缓存容量
	int head;    // 用户下一个读入位置
    int expect;  // 内核在缓冲区存放新数据处
}recv_buf_t;

typedef struct {
	int state; // TCP的状态
	pthread_mutex_t state_lock;  // 用于防止用户线程和后台内核线程竞争

	uint32_t local_isn;  // 本地初始序号
	uint32_t remote_isn;  // 远程初始序号
	uint32_t local_fin_seq;  // 本地发FIN时的Seq
	uint32_t remote_fin_seq; // 远程发FIN时的Seq

	tju_sock_addr bind_addr; // 存放bind和listen时该socket绑定的IP和端口
	tju_sock_addr established_local_addr; // 存放建立连接后 本机的 IP和端口
	tju_sock_addr established_remote_addr; // 存放建立连接后 连接对方的 IP和端口

	send_buf_t* send_buf;
	recv_buf_t* recv_buf;

	sender_window_t* send_wnd;
	receiver_window_t* recv_wnd;

	pthread_t send_thread_id;

} tju_tcp_t;



#endif
