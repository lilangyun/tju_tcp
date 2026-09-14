#include "tju_tcp.h"

#include "tju_core.h"

/*
 * 本文件只保留课程规定的公开接口。
 * 协议状态机、缓冲区、收发线程和资源管理全部位于 tju_core.c。
 */

tju_tcp_t* tju_socket(void){
    return tju_core_socket();
}

int tju_bind(tju_tcp_t* sock, tju_sock_addr bind_addr){
    return tju_core_bind(sock, bind_addr);
}

int tju_listen(tju_tcp_t* sock){
    return tju_core_listen(sock);
}

tju_tcp_t* tju_accept(tju_tcp_t* sock){
    return tju_core_accept(sock);
}

int tju_connect(tju_tcp_t* sock, tju_sock_addr target_addr){
    return tju_core_connect(sock, target_addr);
}

int tju_send(tju_tcp_t* sock, const void* buffer, int len){
    return tju_core_send(sock, buffer, len);
}

int tju_recv(tju_tcp_t* sock, void* buffer, int len){
    return tju_core_recv(sock, buffer, len);
}

int tju_close(tju_tcp_t* sock){
    return tju_core_close(sock);
}

int tju_handle_packet(tju_tcp_t* sock, char* pkt){
    return tju_core_handle_packet(sock, pkt);
}
