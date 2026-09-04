#ifndef _TJU_TCP_H_
#define _TJU_TCP_H_

#include "global.h"
#include "tju_packet.h"
#include "kernel.h"

#define SERVER_IP "172.17.0.3"
#define CLIENT_IP "172.17.0.2"

#define SMSS MAX_DLEN
#define RECV_BUF_SIZE 5000 * SMSS
#define SEND_BUF_SIZE 5000 * SMSS

#define MAX_SOCK 32

// listen_socks的连接队列节点
typedef struct conn_node{
    tju_tcp_t* sock;
    struct conn_node* next;
}conn_node;

// listen_socks的队列结构
typedef struct conn_queue{
    conn_node* front;
    conn_node* rear;
    pthread_mutex_t* lock;  // 每个connect queue都有自己的锁
}conn_queue;

conn_queue* half_conn_queue[MAX_SOCK];  // 半连接队列
conn_queue* full_conn_queue[MAX_SOCK];  // 全连接队列

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

// ### 三次握手辅助函数 ### //
static int send_flags(tju_tcp_t* sock,
                      uint32_t seq,
                      uint32_t ack_num,
                      tju_sock_addr local_addr,
                      tju_sock_addr remote_addr,
                      uint8_t flags);

/*
################ 全连接队列辅助函数 #####################
*/
// 初始化队列
static int init_full_conn_queue(int listen_hashval);

// 创建节点
static conn_node* create_conn_node(tju_tcp_t *new_conn);

// 入队
static int push_full_conn_queue(int listen_hashval, conn_node* new_conn_node);

// 出队
static conn_node* pop_full_conn_queue(int listen_hashval);

// 销毁队列
static int destroy_full_conn_queue(int listen_hashval);

// 判断非空
static int empty_full_conn_queue(int listen_hashval);
/*
######################半连接队列辅助函数###################
*/
// 初始化队列
static int init_half_conn_queue(int listen_hashval);

// 入队
static int push_half_conn_queue(int listen_hashval, conn_node* new_conn_node);

// 出队
static conn_node* pop_half_conn_queue(int listen_hashval);

// 销毁队列
static int destroy_half_conn_queue(int listen_hashval);

// 判断非空
static int empty_half_conn_queue(int listen_hashval);

// 移除指定元素
static conn_node* remove_half_conn_queue(int listen_hashval, int conn_hashval);

#endif

