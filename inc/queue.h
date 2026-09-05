#ifndef QUEUE_H
#define QUEUE_H

#include "global.h"
#include "kernel.h"

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

// kernel.h
#define MAX_SOCK 32

extern conn_queue* half_conn_queue[MAX_SOCK];  // 半连接队列
extern conn_queue* full_conn_queue[MAX_SOCK];  // 全连接队列

/*
################ 全连接队列辅助函数 #########################
*/
// 初始化队列
int init_full_conn_queue(int listen_hashval);

// 创建节点
conn_node* create_conn_node(tju_tcp_t *new_conn);

// 入队
int push_full_conn_queue(int listen_hashval, conn_node* new_conn_node);

// 出队
conn_node* pop_full_conn_queue(int listen_hashval);

// 销毁队列
int destroy_full_conn_queue(int listen_hashval);

// 判断非空
int empty_full_conn_queue(int listen_hashval);
/*
################ 半连接队列辅助函数 ##########################
*/
// 初始化队列
int init_half_conn_queue(int listen_hashval);

// 入队
int push_half_conn_queue(int listen_hashval, conn_node* new_conn_node);

// 出队
conn_node* pop_half_conn_queue(int listen_hashval);

// 销毁队列
int destroy_half_conn_queue(int listen_hashval);

// 判断非空
int empty_half_conn_queue(int listen_hashval);

// 移除指定元素
conn_node* remove_half_conn_queue(int listen_hashval, int conn_hashval);

/*
############################################################
*/

#endif