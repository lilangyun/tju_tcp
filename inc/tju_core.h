#ifndef _TJU_CORE_H_
#define _TJU_CORE_H_

#include "global.h"

/* tju_tcp.c 的规定接口只负责转发到这些内部实现。 */
tju_tcp_t* tju_core_socket(void);
int tju_core_bind(tju_tcp_t* sock, tju_sock_addr bind_addr);
int tju_core_listen(tju_tcp_t* sock);
tju_tcp_t* tju_core_accept(tju_tcp_t* listen_sock);
int tju_core_connect(tju_tcp_t* sock, tju_sock_addr target_addr);
int tju_core_send(tju_tcp_t* sock, const void* buffer, int len);
int tju_core_recv(tju_tcp_t* sock, void* buffer, int len);
int tju_core_close(tju_tcp_t* sock);
int tju_core_handle_packet(tju_tcp_t* sock, char* pkt);

#endif
