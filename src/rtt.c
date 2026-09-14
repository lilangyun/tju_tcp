#include "rtt.h"

#include <time.h>

#include "trace.h"

static uint64_t timeval_to_us(const struct timeval* tv){
    return (uint64_t)tv->tv_sec * 1000000ULL + (uint64_t)tv->tv_usec;
}

uint64_t rtt_current_time_us(void){
    struct timespec now;

    clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * 1000000ULL +
           (uint64_t)now.tv_nsec / 1000ULL;
}

void rtt_init(sender_window_t* wnd){
    wnd->has_rtt_sample = FALSE;
    wnd->srtt_us = 0;
    wnd->rttvar_us = 0;
    wnd->rto_us = RTO_INITIAL_US;
    wnd->current_rto_us = RTO_INITIAL_US;
    wnd->last_rtt_sample_time_us = 0;
    wnd->timeout.tv_sec = 1;
    wnd->timeout.tv_usec = 0;
    wnd->rtt_seq = wnd->base_seq;
    wnd->rtt_sample_pending = FALSE;
    wnd->rtt_retransmitted = FALSE;
}

int rtt_can_start_measurement(tju_tcp_t* sock){
    sender_window_t* wnd = sock->send_wnd;
    uint64_t now;

    if(!wnd->has_rtt_sample ||
       wnd->last_rtt_sample_time_us == 0 ||
       wnd->srtt_us == 0){
        return TRUE;
    }

    now = rtt_current_time_us();
    return now - wnd->last_rtt_sample_time_us >= wnd->srtt_us;
}

void rtt_start_measurement(tju_tcp_t* sock){
    sender_window_t* wnd = sock->send_wnd;
    struct timespec now;

    clock_gettime(CLOCK_MONOTONIC, &now);
    wnd->rtt_send_time.tv_sec = now.tv_sec;
    wnd->rtt_send_time.tv_usec = now.tv_nsec / 1000;
    wnd->rtt_seq = wnd->base_seq;
    wnd->rtt_sample_pending = TRUE;
    wnd->rtt_retransmitted = FALSE;
}

void rtt_reset_measurement(sender_window_t* wnd){
    wnd->rtt_sample_pending = FALSE;
    wnd->rtt_retransmitted = FALSE;
}

void rtt_mark_retransmitted(tju_tcp_t* sock){
    sender_window_t* wnd = sock->send_wnd;

    if(wnd->rtt_seq == wnd->base_seq){
        wnd->rtt_retransmitted = TRUE;
    }
}

void rtt_update_from_ack(tju_tcp_t* sock, uint32_t acknowledged_seq){
    sender_window_t* wnd = sock->send_wnd;
    uint64_t rtt_us;

    if(!wnd->rtt_sample_pending ||
       wnd->rtt_seq != acknowledged_seq ||
       wnd->rtt_retransmitted){
        return;
    }

    rtt_us = rtt_current_time_us() - timeval_to_us(&wnd->rtt_send_time);
    if(rtt_us > 0){
        update_rto(sock, rtt_us);
        wnd->last_rtt_sample_time_us = rtt_current_time_us();
    }
}

void rtt_backoff(tju_tcp_t* sock){
    sender_window_t* wnd = sock->send_wnd;

    if(wnd->current_rto_us > UINT64_MAX / 2){
        wnd->current_rto_us = UINT64_MAX;
    }
    else{
        wnd->current_rto_us *= 2;
    }
}

void update_rto(tju_tcp_t* sock, uint64_t rtt_us){
    sender_window_t* wnd = sock->send_wnd;
    uint64_t variation;
    uint64_t rto;

    if(rtt_us == 0){
        return;
    }

    if(!wnd->has_rtt_sample){
        wnd->srtt_us = rtt_us;
        wnd->rttvar_us = rtt_us / 2;
        wnd->has_rtt_sample = TRUE;
    }
    else{
        variation = (wnd->srtt_us > rtt_us)?
                    (wnd->srtt_us - rtt_us):
                    (rtt_us - wnd->srtt_us);
        wnd->rttvar_us =
            (3 * wnd->rttvar_us + variation + 2) / 4;
        wnd->srtt_us =
            (7 * wnd->srtt_us + rtt_us + 4) / 8;
    }

    if(wnd->rttvar_us > UINT64_MAX / 4){
        variation = UINT64_MAX;
    }
    else{
        variation = 4 * wnd->rttvar_us;
    }
    rto = wnd->srtt_us +
          ((variation > RTO_CLOCK_GRANULARITY_US)?
           variation: RTO_CLOCK_GRANULARITY_US);
    if(rto < RTO_INITIAL_US){
        rto = RTO_INITIAL_US;
    }

    wnd->rto_us = rto;
    wnd->current_rto_us = rto;
    trace_rtt(rtt_us, wnd->srtt_us, wnd->rttvar_us, wnd->rto_us);
}
