#include "rtt.h"

#include <time.h>

#include "trace.h"

static uint64_t timeval_to_microseconds(const struct timeval* tv){
    return (uint64_t)tv->tv_sec * 1000000ULL + (uint64_t)tv->tv_usec;
}

/* 序号窗口远小于 2^31，使用有符号差值可正确处理 32 位回绕。 */
static int sequence_after_or_equal(uint32_t lhs, uint32_t rhs){
    return (int32_t)(lhs - rhs) >= 0;
}

static int sequence_before(uint32_t lhs, uint32_t rhs){
    return (int32_t)(lhs - rhs) < 0;
}

static int sequence_ranges_overlap(uint32_t first_seq, uint32_t first_length,
                                   uint32_t second_seq, uint32_t second_length){
    if(first_length == 0 || second_length == 0){
        return FALSE;
    }

    return sequence_before(first_seq, second_seq + second_length) &&
           sequence_before(second_seq, first_seq + first_length);
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
    send_window->retransmit_timeout.tv_sec = 1;
    send_window->retransmit_timeout.tv_usec = 0;
    send_window->rtt_measurement_seq = send_window->base_seq;
    send_window->rtt_measurement_end_seq = send_window->base_seq;
    send_window->rtt_measurement_pending = FALSE;
    send_window->rtt_measurement_retransmitted = FALSE;
}

void rtt_start_measurement(tju_tcp_t* sock, uint32_t seq_num,
                           uint32_t data_length){
    send_window_t* send_window = sock->send_window;
    struct timespec now;

    if(data_length == 0 || send_window->rtt_measurement_pending){
        return;
    }

    clock_gettime(CLOCK_MONOTONIC, &now);
    send_window->rtt_send_time.tv_sec = now.tv_sec;
    send_window->rtt_send_time.tv_usec = now.tv_nsec / 1000;
    send_window->rtt_measurement_seq = seq_num;
    send_window->rtt_measurement_end_seq = seq_num + data_length;
    send_window->rtt_measurement_pending = TRUE;
    send_window->rtt_measurement_retransmitted = FALSE;
}

void rtt_reset_measurement(send_window_t* send_window){
    send_window->rtt_measurement_pending = FALSE;
    send_window->rtt_measurement_retransmitted = FALSE;
    send_window->rtt_measurement_end_seq = send_window->rtt_measurement_seq;
}

void rtt_mark_retransmitted(tju_tcp_t* sock, uint32_t seq_num,
                            uint32_t data_length){
    send_window_t* send_window = sock->send_window;
    uint32_t measurement_length;

    if(!send_window->rtt_measurement_pending || data_length == 0){
        return;
    }

    measurement_length =
        send_window->rtt_measurement_end_seq - send_window->rtt_measurement_seq;
    if(sequence_ranges_overlap(seq_num, data_length,
                               send_window->rtt_measurement_seq,
                               measurement_length)){
        send_window->rtt_measurement_retransmitted = TRUE;
    }
}

void rtt_update_from_ack(tju_tcp_t* sock, uint32_t acknowledged_seq){
    send_window_t* send_window = sock->send_window;
    uint64_t rtt_sample_us;

    if(!send_window->rtt_measurement_pending ||
       !sequence_after_or_equal(acknowledged_seq,
                                send_window->rtt_measurement_end_seq)){
        return;
    }

    if(send_window->rtt_measurement_retransmitted){
        rtt_reset_measurement(send_window);
        return;
    }

    rtt_sample_us = rtt_current_time_us() - timeval_to_microseconds(&send_window->rtt_send_time);
    if(rtt_sample_us > 0){
        update_rto(sock, rtt_sample_us);
    }
    rtt_reset_measurement(send_window);
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
