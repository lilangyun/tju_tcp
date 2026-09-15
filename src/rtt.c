#include "rtt.h"

#include <time.h>

#include "trace.h"

static uint64_t timeval_to_microseconds(const struct timeval* tv){
    return (uint64_t)tv->tv_sec * 1000000ULL + (uint64_t)tv->tv_usec;
}

uint64_t rtt_current_time_us(void){
    struct timespec now;

    clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * 1000000ULL +
           (uint64_t)now.tv_nsec / 1000ULL;
}

void rtt_init(send_window_t* send_window){
    send_window->has_rtt_sample = FALSE;
    send_window->srtt_us = 0;
    send_window->rttvar_us = 0;
    send_window->estimated_rto_us = RTO_INITIAL_US;
    send_window->effective_rto_us = RTO_INITIAL_US;
    send_window->last_rtt_sample_time_us = 0;
    send_window->retransmit_timeout.tv_sec = 1;
    send_window->retransmit_timeout.tv_usec = 0;
    send_window->rtt_measurement_seq = send_window->base_seq;
    send_window->rtt_measurement_pending = FALSE;
    send_window->rtt_measurement_retransmitted = FALSE;
}

int rtt_can_start_measurement(tju_tcp_t* sock){
    send_window_t* send_window = sock->send_window;
    uint64_t now;

    if(!send_window->has_rtt_sample ||
       send_window->last_rtt_sample_time_us == 0 ||
       send_window->srtt_us == 0){
        return TRUE;
    }

    now = rtt_current_time_us();
    return now - send_window->last_rtt_sample_time_us >= send_window->srtt_us;
}

void rtt_start_measurement(tju_tcp_t* sock){
    send_window_t* send_window = sock->send_window;
    struct timespec now;

    clock_gettime(CLOCK_MONOTONIC, &now);
    send_window->rtt_send_time.tv_sec = now.tv_sec;
    send_window->rtt_send_time.tv_usec = now.tv_nsec / 1000;
    send_window->rtt_measurement_seq = send_window->base_seq;
    send_window->rtt_measurement_pending = TRUE;
    send_window->rtt_measurement_retransmitted = FALSE;
}

void rtt_reset_measurement(send_window_t* send_window){
    send_window->rtt_measurement_pending = FALSE;
    send_window->rtt_measurement_retransmitted = FALSE;
}

void rtt_mark_retransmitted(tju_tcp_t* sock){
    send_window_t* send_window = sock->send_window;

    if(send_window->rtt_measurement_seq == send_window->base_seq){
        send_window->rtt_measurement_retransmitted = TRUE;
    }
}

void rtt_update_from_ack(tju_tcp_t* sock, uint32_t acknowledged_seq){
    send_window_t* send_window = sock->send_window;
    uint64_t rtt_sample_us;

    if(!send_window->rtt_measurement_pending ||
       send_window->rtt_measurement_seq != acknowledged_seq ||
       send_window->rtt_measurement_retransmitted){
        return;
    }

    rtt_sample_us = rtt_current_time_us() - timeval_to_microseconds(&send_window->rtt_send_time);
    if(rtt_sample_us > 0){
        update_rto(sock, rtt_sample_us);
        send_window->last_rtt_sample_time_us = rtt_current_time_us();
    }
}

void rtt_backoff(tju_tcp_t* sock){
    send_window_t* send_window = sock->send_window;

    if(send_window->effective_rto_us > UINT64_MAX / 2){
        send_window->effective_rto_us = UINT64_MAX;
    }
    else{
        send_window->effective_rto_us *= 2;
    }
}

void update_rto(tju_tcp_t* sock, uint64_t rtt_sample_us){
    send_window_t* send_window = sock->send_window;
    uint64_t rtt_difference;
    uint64_t computed_rto;

    if(rtt_sample_us == 0){
        return;
    }

    if(!send_window->has_rtt_sample){
        send_window->srtt_us = rtt_sample_us;
        send_window->rttvar_us = rtt_sample_us / 2;
        send_window->has_rtt_sample = TRUE;
    }
    else{
        rtt_difference = (send_window->srtt_us > rtt_sample_us)?
                    (send_window->srtt_us - rtt_sample_us):
                    (rtt_sample_us - send_window->srtt_us);
        send_window->rttvar_us =
            (3 * send_window->rttvar_us + rtt_difference + 2) / 4;
        send_window->srtt_us =
            (7 * send_window->srtt_us + rtt_sample_us + 4) / 8;
    }

    if(send_window->rttvar_us > UINT64_MAX / 4){
        rtt_difference = UINT64_MAX;
    }
    else{
        rtt_difference = 4 * send_window->rttvar_us;
    }
    computed_rto = send_window->srtt_us +
          ((rtt_difference > RTO_CLOCK_GRANULARITY_US)?
           rtt_difference: RTO_CLOCK_GRANULARITY_US);
    if(computed_rto < RTO_INITIAL_US){
        computed_rto = RTO_INITIAL_US;
    }

    send_window->estimated_rto_us = computed_rto;
    send_window->effective_rto_us = computed_rto;
    trace_rtt(rtt_sample_us, send_window->srtt_us, send_window->rttvar_us, send_window->estimated_rto_us);
}
