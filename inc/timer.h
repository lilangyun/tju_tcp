#ifndef TIMER_H
#define TIMER_H

#include "global.h"

extern int retransmit;

void set_timeout(int time);

void stop_timeout();

void timeout_check(int signal);

#endif