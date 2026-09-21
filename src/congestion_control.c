#include "congestion_control.h"

#include "tju_tcp.h"

#define RENO_MAX_CWND_BYTES (UINT32_MAX / 2U)

static uint32_t initial_cwnd_bytes(void){
    /* RFC 5681 第 3.1 节规定的初始拥塞窗口上限。 */
    if(SMSS > 2190U){
        return 2U * SMSS;
    }
    if(SMSS > 1095U){
        return 3U * SMSS;
    }
    return 4U * SMSS;
}

static uint32_t add_cwnd(uint32_t cwnd, uint32_t cwnd_increment){
    if(cwnd >= RENO_MAX_CWND_BYTES ||
       cwnd_increment > RENO_MAX_CWND_BYTES - cwnd){
        return RENO_MAX_CWND_BYTES;
    }
    return cwnd + cwnd_increment;
}

static uint32_t congestion_threshold(uint32_t flight_size){
    uint32_t half_flight_size = flight_size / 2U;
    uint32_t minimum_ssthresh = 2U * SMSS;

    return (half_flight_size > minimum_ssthresh)? half_flight_size: minimum_ssthresh;
}

static int sequence_after_or_equal(uint32_t lhs, uint32_t rhs){
    return (int32_t)(lhs - rhs) >= 0;
}

void congestion_init(send_window_t* send_window, uint16_t initial_ssthresh){
    send_window->cwnd = initial_cwnd_bytes();
    send_window->ssthresh = initial_ssthresh;
    send_window->congestion_state = SLOW_START;
    send_window->in_fast_recovery = FALSE;
    send_window->recover_seq = send_window->base_seq;
}

uint32_t congestion_effective_window(const send_window_t* send_window){
    uint32_t rwnd = send_window->rwnd;

    /* 实际发送窗口取接收窗口和拥塞窗口的较小值。 */
    return (rwnd < send_window->cwnd)? rwnd: send_window->cwnd;
}

int congestion_ack_covers_recover(const send_window_t* send_window,
                                  uint32_t acknowledged_seq){
    return sequence_after_or_equal(acknowledged_seq, send_window->recover_seq);
}

congestion_ack_result_t congestion_on_new_ack(send_window_t* send_window,
                                              uint32_t acknowledged_bytes,
                                              uint32_t acknowledged_seq){
    uint32_t cwnd_increment;

    if(acknowledged_bytes == 0){
        return CONGESTION_ACK_NORMAL;
    }

    if(send_window->in_fast_recovery){
        if(congestion_ack_covers_recover(send_window, acknowledged_seq)){
            /* Full ACK：覆盖 recover_seq 后退出快速恢复。 */
            send_window->cwnd = send_window->ssthresh;
            send_window->congestion_state = CONGESTION_AVOIDANCE;
            send_window->in_fast_recovery = FALSE;
            return CONGESTION_ACK_FULL_RECOVERY;
        }

        /* Partial ACK：按 RFC 6582 对窗口做部分 deflate。 */
        if(send_window->cwnd > acknowledged_bytes){
            send_window->cwnd -= acknowledged_bytes;
        }
        else{
            send_window->cwnd = SMSS;
        }
        if(acknowledged_bytes >= SMSS){
            send_window->cwnd = add_cwnd(send_window->cwnd, SMSS);
        }
        if(send_window->cwnd < SMSS){
            send_window->cwnd = SMSS;
        }
        send_window->congestion_state = FAST_RECOVERY;
        return CONGESTION_ACK_PARTIAL;
    }

    /* 慢启动：每个累计 ACK 最多增加一个 SMSS。 */
    if(send_window->cwnd < send_window->ssthresh){
        cwnd_increment = (acknowledged_bytes < SMSS)? acknowledged_bytes: SMSS;
        send_window->cwnd = add_cwnd(send_window->cwnd, cwnd_increment);
        send_window->congestion_state =
            (send_window->cwnd < send_window->ssthresh)? SLOW_START:
                                        CONGESTION_AVOIDANCE;
        return CONGESTION_ACK_NORMAL;
    }

    /* 加法增长：一个窗口内的增量之和约为一个 SMSS。 */
    cwnd_increment = (uint32_t)(((uint64_t)SMSS * SMSS) / send_window->cwnd);
    if(cwnd_increment == 0){
        cwnd_increment = 1;
    }
    send_window->cwnd = add_cwnd(send_window->cwnd, cwnd_increment);
    send_window->congestion_state = CONGESTION_AVOIDANCE;
    return CONGESTION_ACK_NORMAL;
}

void congestion_on_duplicate_ack(send_window_t* send_window){
    if(!send_window->in_fast_recovery){
        return;
    }

    /* RFC 5681 步骤 4：快速恢复期间每个额外重复 ACK 膨胀一个 SMSS。 */
    send_window->cwnd = add_cwnd(send_window->cwnd, SMSS);
    send_window->congestion_state = FAST_RECOVERY;
}

void congestion_on_fast_retransmit(send_window_t* send_window,
                                   uint32_t flight_size,
                                   uint32_t recover_seq){
    /* RFC 5681/6582：进入快速恢复，窗口设置为 ssthresh + 3*SMSS。 */
    send_window->ssthresh = congestion_threshold(flight_size);
    send_window->cwnd = add_cwnd(send_window->ssthresh, 3U * SMSS);
    send_window->congestion_state = FAST_RECOVERY;
    send_window->recover_seq = recover_seq;
    send_window->in_fast_recovery = TRUE;
}

void congestion_on_timeout(send_window_t* send_window, uint32_t flight_size){
    /* RTO 是更强的拥塞信号：重新从一个 SMSS 开始慢启动。 */
    send_window->ssthresh = congestion_threshold(flight_size);
    send_window->cwnd = SMSS;
    send_window->congestion_state = SLOW_START;
    send_window->recover_seq = send_window->next_seq;
    send_window->in_fast_recovery = FALSE;
}
