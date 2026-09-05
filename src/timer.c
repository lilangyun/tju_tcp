#include "timer.h"

int retransmit = 0;

void set_timeout(int sec){
    signal(SIGALRM, timeout_check);
    retransmit = 0;
    struct timeval timeout = {sec, 0};
    struct itimerval timer = {0};
    timer.it_value = timeout;
    setitimer(ITIMER_REAL, &timer, NULL);
}

void stop_timeout(){
    retransmit = 0;
    struct itimerval timer = {0};
    setitimer(ITIMER_REAL, &timer, NULL);
}

void timeout_check(int signal){
    retransmit = 1;
}