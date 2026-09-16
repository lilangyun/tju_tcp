#ifndef _CONGESTION_CONTROL_H_
#define _CONGESTION_CONTROL_H_

#include "global.h"

#define CONGESTION_FAST_RETRANSMIT 2
#define CONGESTION_TIMEOUT 3

/*
 * 基础 Reno 状态：
 * cwnd < ssthresh 时执行慢启动，否则执行拥塞避免；
 * 三次重复 ACK 只执行基本快速重传，并等待首个推进 ACK；
 * 完整快速恢复不属于本阶段的必做范围。
 */
void congestion_init(send_window_t* send_window, uint16_t initial_ssthresh);
uint32_t congestion_effective_window(const send_window_t* send_window);
void congestion_on_new_ack(send_window_t* send_window, uint32_t acknowledged_bytes);
void congestion_on_fast_retransmit(send_window_t* send_window, uint32_t flight_size);
void congestion_on_timeout(send_window_t* send_window, uint32_t flight_size);

#endif
