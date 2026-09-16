#ifndef _TIMER_H_
#define _TIMER_H_

#include "global.h"

/* 各定时器共享一个 SIGALRM，retransmission_pending 表示对应事件是否已触发。 */
#define SYN_RETRY 1
#define SYNACK_RETRY 2
#define FIN_RETRY 3
#define FINACK_RETRY 4
#define DATA_RETRY 5
#define ZERO_WINDOW_RETRY 6
#define RETRY_FLAG_NUM 7

#define MSL 3

extern volatile sig_atomic_t retransmission_pending[RETRY_FLAG_NUM];

void timer_arm_event(tju_tcp_t* sock, int retransmission_event);
void timer_cancel_event(int retransmission_event);

void timer_state_reset(send_window_t* send_window);
void timer_arm_data_retransmission(tju_tcp_t* sock);
void timer_cancel_data_retransmission(tju_tcp_t* sock);
void timer_arm_zero_window_probe(tju_tcp_t* sock);
void timer_cancel_zero_window_probe(tju_tcp_t* sock);
int timer_zero_window_probe_active(const send_window_t* send_window);

void handle_syn_timeout(int signal);
void handle_synack_timeout(int signal);
void handle_fin_timeout(int signal);
void handle_finack_timeout(int signal);
void handle_data_timeout(int signal);
void handle_zero_window_timeout(int signal);

#endif
