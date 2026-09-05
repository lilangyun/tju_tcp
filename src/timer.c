#include "timer.h"

int retry[5] = {0};
pthread_mutex_t *retry_lock[5];

static void (*timeout_check[5])(int) = {
    NULL, 
    syn_timeout_check, 
    synack_timeout_check,
    ack_timeout_check,
    fin_timeout_check,
};

void init_retry(){
    for(int i = 0; i < 5; i++){
        pthread_mutex_init(retry_lock[i], NULL);
    }
}

void set_timeout(int sec, int re_num){
    signal(SIGALRM, timeout_check[re_num]);
    pthread_mutex_lock(retry_lock[re_num]);
    retry[re_num] = 0;
    pthread_mutex_unlock(retry_lock[re_num]);
    struct timeval timeout = {sec, 0};
    struct itimerval timer = {0};
    timer.it_value = timeout;
    setitimer(ITIMER_REAL, &timer, NULL);
}

void stop_timeout(int re_num){
    pthread_mutex_lock(retry_lock[re_num]);
    retry[re_num] = 0;
    pthread_mutex_unlock(retry_lock[re_num]);    
    struct itimerval timer = {0};
    setitimer(ITIMER_REAL, &timer, NULL);
}

void syn_timeout_check(int signal){
    pthread_mutex_lock(retry_lock[SYN_RETRY]);
    retry[SYN_RETRY] = 1;
    pthread_mutex_unlock(retry_lock[SYN_RETRY]);
}

void synack_timeout_check(int signal){
    pthread_mutex_lock(retry_lock[SYNACK_RETRY]);
    retry[SYNACK_RETRY] = 1;
    pthread_mutex_unlock(retry_lock[SYNACK_RETRY]);
}

void ack_timeout_check(int signal){
    pthread_mutex_lock(retry_lock[ACK_RETRY]);
    retry[ACK_RETRY] = 1;
    pthread_mutex_unlock(retry_lock[ACK_RETRY]);
}

void fin_timeout_check(int signal){
    pthread_mutex_lock(retry_lock[FIN_RETRY]);
    retry[FIN_RETRY] = 1;
    pthread_mutex_unlock(retry_lock[FIN_RETRY]);
}