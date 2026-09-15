#ifndef _RTT_H_
#define _RTT_H_

#include "global.h"

#define RTO_INITIAL_US 1000000ULL
#define RTO_CLOCK_GRANULARITY_US 1000ULL

void rtt_init(send_window_t* send_window);
uint64_t rtt_current_time_us(void);
int rtt_can_start_measurement(tju_tcp_t* sock);
void rtt_start_measurement(tju_tcp_t* sock);
void rtt_reset_measurement(send_window_t* send_window);
void rtt_mark_retransmitted(tju_tcp_t* sock);
void rtt_update_from_ack(tju_tcp_t* sock, uint32_t acknowledged_seq);
void rtt_backoff(tju_tcp_t* sock);
void update_rto(tju_tcp_t* sock, uint64_t rtt_sample_us);

#endif
