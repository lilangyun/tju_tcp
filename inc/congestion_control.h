#ifndef _CONGESTION_CONTROL_H_
#define _CONGESTION_CONTROL_H_

#include "global.h"

#define CONGESTION_FAST_RETRANSMIT 2
#define CONGESTION_TIMEOUT 3

typedef enum {
    CONGESTION_ACK_NORMAL = 0,
    CONGESTION_ACK_PARTIAL = 1,
    CONGESTION_ACK_FULL_RECOVERY = 2
} congestion_ack_result_t;

/*
 * 基础 Reno 状态：
 * cwnd < ssthresh 时执行慢启动，否则执行拥塞避免；
 * 三次重复 ACK 进入 NewReno 快速恢复；
 * partial ACK 重传下一个缺口并继续恢复，覆盖 recover_seq 后退出。
 */
void congestion_init(send_window_t* send_window, uint16_t initial_ssthresh);
uint32_t congestion_effective_window(const send_window_t* send_window);
congestion_ack_result_t congestion_on_new_ack(send_window_t* send_window,
                                              uint32_t acknowledged_bytes,
                                              uint32_t acknowledged_seq);
void congestion_on_duplicate_ack(send_window_t* send_window);
int congestion_ack_covers_recover(const send_window_t* send_window,
                                  uint32_t acknowledged_seq);
void congestion_on_fast_retransmit(send_window_t* send_window,
                                   uint32_t flight_size,
                                   uint32_t recover_seq);
void congestion_on_timeout(send_window_t* send_window, uint32_t flight_size);

#endif
