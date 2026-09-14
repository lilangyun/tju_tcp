#ifndef _TIMER_H_
#define _TIMER_H_

#include "global.h"

/* 各定时器共享一个 SIGALRM，retry 表示对应事件是否已触发。 */
#define SYN_RETRY 1
#define SYNACK_RETRY 2
#define FIN_RETRY 3
#define FINACK_RETRY 4
#define DATA_RETRY 5
#define ZERO_WINDOW_RETRY 6
#define RETRY_FLAG_NUM 7

#define MSL 3

extern volatile sig_atomic_t retry[RETRY_FLAG_NUM];

void set_timeout(tju_tcp_t* sock, int re_num);
void stop_timeout(int re_num);

void timer_state_init(sender_window_t* wnd);
void timer_arm_data(tju_tcp_t* sock);
void timer_stop_data(tju_tcp_t* sock);
void timer_arm_zero_window(tju_tcp_t* sock);
void timer_cancel_zero_window(tju_tcp_t* sock);
int timer_zero_window_active(const sender_window_t* wnd);

void syn_timeout_check(int signal);
void synack_timeout_check(int signal);
void fin_timeout_check(int signal);
void finack_timeout_check(int signal);
void data_timeout_check(int signal);
void zero_window_timeout_check(int signal);

#endif
