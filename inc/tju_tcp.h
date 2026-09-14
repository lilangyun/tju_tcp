#ifndef _TJU_TCP_H_
#define _TJU_TCP_H_

#include "global.h"
#include "kernel.h"

#define SERVER_IP "172.17.0.3"
#define CLIENT_IP "172.17.0.2"

/* 单个报文最大数据长度；收发缓冲区按说明书的 5000 * SMSS 下限设置。 */
#define SMSS MAX_DLEN
#define RECV_BUF_SIZE (5000 * SMSS)
#define SEND_BUF_SIZE (5000 * SMSS)

#define SEQ_BIT_WIDTH 32
#define SEQ_MAX (uint32_t)(((uint64_t)1 << SEQ_BIT_WIDTH) - 1)

/* 课程规定接口。实现位于 tju_core.c，tju_tcp.c 只负责转发。 */
tju_tcp_t* tju_socket(void);
int tju_bind(tju_tcp_t* sock, tju_sock_addr bind_addr);
int tju_listen(tju_tcp_t* sock);
tju_tcp_t* tju_accept(tju_tcp_t* sock);
int tju_connect(tju_tcp_t* sock, tju_sock_addr target_addr);
int tju_send(tju_tcp_t* sock, const void* buffer, int len);
int tju_recv(tju_tcp_t* sock, void* buffer, int len);
int tju_close(tju_tcp_t* sock);

/* 仿真内核收到完整报文后的内部回调，不属于面向用户的八个 API。 */
int tju_handle_packet(tju_tcp_t* sock, char* pkt);

#endif
