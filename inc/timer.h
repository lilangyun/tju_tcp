#ifndef TIMER_H
#define TIMER_H

#define SYN_RETRY 1
#define SYNACK_RETRY 2
#define ACK_RETRY 3

#include "global.h"

extern int retry[4];
static void (*timeout_check[4])(int);

void set_timeout(int sec, int re_num);

void stop_timeout(int re_num);

void syn_timeout_check(int signal);

void synack_timeout_check(int signal);

void ack_timeout_check(int signal);

#endif