#include "timer.h"

volatile sig_atomic_t retry[RETRY_FLAG_NUM] = {0};

static void (*timeout_check[RETRY_FLAG_NUM])(int) = {
    NULL,
    syn_timeout_check,
    synack_timeout_check,
    fin_timeout_check,
    finack_timeout_check,
    data_timeout_check,
    zero_window_timeout_check,
};

void set_timeout(tju_tcp_t* sock, int re_num){
    struct itimerval timer = {0};

    signal(SIGALRM, timeout_check[re_num]);
    retry[re_num] = 0;
    timer.it_value = sock->send_wnd->timeout;
    setitimer(ITIMER_REAL, &timer, NULL);
}

void stop_timeout(int re_num){
    struct itimerval timer = {0};

    retry[re_num] = 0;
    setitimer(ITIMER_REAL, &timer, NULL);
}

void timer_state_init(sender_window_t* wnd){
    wnd->data_timer_active = FALSE;
    wnd->zero_window_timer_active = FALSE;
}

void timer_arm_data(tju_tcp_t* sock){
    sender_window_t* wnd = sock->send_wnd;

    wnd->timeout.tv_sec = (time_t)(wnd->current_rto_us / 1000000ULL);
    wnd->timeout.tv_usec = (suseconds_t)(wnd->current_rto_us % 1000000ULL);
    wnd->data_timer_active = TRUE;
    set_timeout(sock, DATA_RETRY);
}

void timer_stop_data(tju_tcp_t* sock){
    sock->send_wnd->data_timer_active = FALSE;
    stop_timeout(DATA_RETRY);
}

void timer_arm_zero_window(tju_tcp_t* sock){
    sender_window_t* wnd = sock->send_wnd;

    if(wnd->zero_window_timer_active){
        return;
    }

    wnd->timeout.tv_sec = (time_t)(wnd->current_rto_us / 1000000ULL);
    wnd->timeout.tv_usec = (suseconds_t)(wnd->current_rto_us % 1000000ULL);
    wnd->zero_window_timer_active = TRUE;
    set_timeout(sock, ZERO_WINDOW_RETRY);
}

void timer_cancel_zero_window(tju_tcp_t* sock){
    if(!sock->send_wnd->zero_window_timer_active){
        return;
    }

    sock->send_wnd->zero_window_timer_active = FALSE;
    stop_timeout(ZERO_WINDOW_RETRY);
}

int timer_zero_window_active(const sender_window_t* wnd){
    return wnd->zero_window_timer_active;
}

void syn_timeout_check(int signal){
    (void)signal;
    retry[SYN_RETRY] = 1;
}

void synack_timeout_check(int signal){
    (void)signal;
    retry[SYNACK_RETRY] = 1;
}

void fin_timeout_check(int signal){
    (void)signal;
    retry[FIN_RETRY] = 1;
}

void finack_timeout_check(int signal){
    (void)signal;
    retry[FINACK_RETRY] = 1;
}

void data_timeout_check(int signal){
    (void)signal;
    retry[DATA_RETRY] = 1;
}

void zero_window_timeout_check(int signal){
    (void)signal;
    retry[ZERO_WINDOW_RETRY] = 1;
}
