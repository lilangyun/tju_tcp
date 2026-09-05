#ifndef TIMER_H
#define TIMER_H

#define SYN_RETRY 1
#define SYNACK_RETRY 2
#define ACK_RETRY 3
#define FIN_RETRY 4

// 最大报文生存时间
#define MSL 3

#include "global.h"

extern int retry[5];
static void (*timeout_check[5])(int);

void set_timeout(int sec, int re_num);

void stop_timeout(int re_num);

void syn_timeout_check(int signal);

void synack_timeout_check(int signal);

void ack_timeout_check(int signal);

void fin_timeout_check(int signal);

#endif