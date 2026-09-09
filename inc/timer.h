#ifndef _TIMER_H_
#define _TIMER_H_

#include "global.h"

// 连接管理重传对应Flags
#define SYN_RETRY 1
#define SYNACK_RETRY 2
#define ACK_RETRY 3
#define FIN_RETRY 4
#define FINACK_RETRY 5

// 最大报文生存时间
#define MSL 3

extern volatile sig_atomic_t retry[6];

static void (*timeout_check[6])(int);

void set_timeout(int sec, int re_num);

void stop_timeout(int re_num);

void syn_timeout_check(int signal);

void synack_timeout_check(int signal);

void ack_timeout_check(int signal);

void fin_timeout_check(int signal);

void finack_timeout_check(int signal);

#endif