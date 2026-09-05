#include "timer.h"

int retry[5] = {0};

static void (*timeout_check[5])(int) = {
    NULL, 
    syn_timeout_check, 
    synack_timeout_check,
    ack_timeout_check,
    fin_timeout_check,
};

void set_timeout(int sec, int re_num){
    signal(SIGALRM, timeout_check[re_num]);
    retry[re_num] = 0;
    struct timeval timeout = {sec, 0};
    struct itimerval timer = {0};
    timer.it_value = timeout;
    setitimer(ITIMER_REAL, &timer, NULL);
}

void stop_timeout(int re_num){
    retry[re_num] = 0;
    struct itimerval timer = {0};
    setitimer(ITIMER_REAL, &timer, NULL);
}

void syn_timeout_check(int signal){
    retry[SYN_RETRY] = 1;
}

void synack_timeout_check(int signal){
    retry[SYNACK_RETRY] = 1;
}

void ack_timeout_check(int signal){
    retry[ACK_RETRY] = 1;
}

void fin_timeout_check(int signal){
    retry[FIN_RETRY] = 1;
}