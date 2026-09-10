#ifndef _TJU_TCP_H_
#define _TJU_TCP_H_

#include "global.h"
#include "tju_packet.h"
#include "kernel.h"
#include "queue.h"
#include "timer.h"

#define SERVER_IP "172.17.0.3"
#define CLIENT_IP "172.17.0.2"

// 缓冲区
#define SMSS MAX_DLEN
#define RECV_BUF_SIZE 10000 * SMSS
#define SEND_BUF_SIZE 10000 * SMSS

// Sequence Number
#define SEQ_BIT_WIDTH 32
#define SEQ_MAX (uint32_t)(((uint64_t)1 << SEQ_BIT_WIDTH) - 1)

/*
###################################################################
*/

/*
创建 TCP socket 
初始化对应的结构体
设置初始状态为 CLOSED
*/
tju_tcp_t* tju_socket();

/*
绑定监听的地址 包括ip和端口
*/
int tju_bind(tju_tcp_t* sock, tju_sock_addr bind_addr);

/*
被动打开 监听bind的地址和端口
设置socket的状态为LISTEN
*/
int tju_listen(tju_tcp_t* sock);

/*
接受连接 
返回与客户端通信用的socket
这里返回的socket一定是已经完成3次握手建立了连接的socket
因为只要该函数返回, 用户就可以马上使用该socket进行send和recv
*/
tju_tcp_t* tju_accept(tju_tcp_t* sock);


/*
连接到服务端
该函数以一个socket为参数
调用函数前, 该socket还未建立连接
函数正常返回后, 该socket一定是已经完成了3次握手, 建立了连接
因为只要该函数返回, 用户就可以马上使用该socket进行send和recv
*/
int tju_connect(tju_tcp_t* sock, tju_sock_addr target_addr);


int tju_send (tju_tcp_t* sock, const void *buffer, int len);
int tju_recv (tju_tcp_t* sock, void *buffer, int len);

/*
关闭一个TCP连接
这里涉及到四次挥手
*/
int tju_close (tju_tcp_t* sock);


int tju_handle_packet(tju_tcp_t* sock, char* pkt);

/*
######################## 辅助函数 #########################
*/

// 用于单独发送三次握手的flags
static int send_flags(tju_tcp_t* sock, uint8_t flags, uint32_t seq_num, uint32_t ack_num);

// 释放sock对应的连接资源
static void free_sock(tju_tcp_t* sock);

// 初始化窗口
static void init_buf_wnd(tju_tcp_t* sock);

/*
######################## 线程函数 #########################
*/

// 发送线程
static void* send_thread(void* arg);

#endif