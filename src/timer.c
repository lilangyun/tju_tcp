#include "timer.h"

volatile sig_atomic_t retransmission_pending[RETRY_FLAG_NUM] = {0};

static void (*timeout_handlers[RETRY_FLAG_NUM])(int) = {
    NULL,
    handle_syn_timeout,
    handle_synack_timeout,
    handle_fin_timeout,
    handle_finack_timeout,
    handle_data_timeout,
    handle_zero_window_timeout,
};

void timer_arm_event(tju_tcp_t* sock, int retransmission_event){
    struct itimerval timer = {0};

    signal(SIGALRM, timeout_handlers[retransmission_event]);
    retransmission_pending[retransmission_event] = 0;
    timer.it_value = sock->send_window->retransmit_timeout;
    setitimer(ITIMER_REAL, &timer, NULL);
}

void timer_cancel_event(int retransmission_event){
    struct itimerval timer = {0};

    retransmission_pending[retransmission_event] = 0;
    setitimer(ITIMER_REAL, &timer, NULL);
}

void timer_state_reset(send_window_t* send_window){
    send_window->data_retransmission_timer_active = FALSE;
    send_window->zero_window_probe_timer_active = FALSE;
}

void timer_arm_data_retransmission(tju_tcp_t* sock){
    send_window_t* send_window = sock->send_window;

    send_window->retransmit_timeout.tv_sec = (time_t)(send_window->effective_rto_us / 1000000ULL);
    send_window->retransmit_timeout.tv_usec = (suseconds_t)(send_window->effective_rto_us % 1000000ULL);
    send_window->data_retransmission_timer_active = TRUE;
    timer_arm_event(sock, DATA_RETRY);
}

void timer_cancel_data_retransmission(tju_tcp_t* sock){
    sock->send_window->data_retransmission_timer_active = FALSE;
    timer_cancel_event(DATA_RETRY);
}

void timer_arm_zero_window_probe(tju_tcp_t* sock){
    send_window_t* send_window = sock->send_window;

    if(send_window->zero_window_probe_timer_active){
        return;
    }

    send_window->retransmit_timeout.tv_sec = (time_t)(send_window->effective_rto_us / 1000000ULL);
    send_window->retransmit_timeout.tv_usec = (suseconds_t)(send_window->effective_rto_us % 1000000ULL);
    send_window->zero_window_probe_timer_active = TRUE;
    timer_arm_event(sock, ZERO_WINDOW_RETRY);
}

void timer_cancel_zero_window_probe(tju_tcp_t* sock){
    if(!sock->send_window->zero_window_probe_timer_active){
        return;
    }

    sock->send_window->zero_window_probe_timer_active = FALSE;
    timer_cancel_event(ZERO_WINDOW_RETRY);
}

int timer_zero_window_probe_active(const send_window_t* send_window){
    return send_window->zero_window_probe_timer_active;
}

void handle_syn_timeout(int signal){
    (void)signal;
    retransmission_pending[SYN_RETRY] = 1;
}

void handle_synack_timeout(int signal){
    (void)signal;
    retransmission_pending[SYNACK_RETRY] = 1;
}

void handle_fin_timeout(int signal){
    (void)signal;
    retransmission_pending[FIN_RETRY] = 1;
}

void handle_finack_timeout(int signal){
    (void)signal;
    retransmission_pending[FINACK_RETRY] = 1;
}

void handle_data_timeout(int signal){
    (void)signal;
    retransmission_pending[DATA_RETRY] = 1;
}

void handle_zero_window_timeout(int signal){
    (void)signal;
    retransmission_pending[ZERO_WINDOW_RETRY] = 1;
}
