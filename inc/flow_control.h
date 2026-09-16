#ifndef _FLOW_CONTROL_H_
#define _FLOW_CONTROL_H_

#include "global.h"

uint32_t flow_receive_available_locked(tju_tcp_t* sock);
uint16_t flow_advertised_window_locked(tju_tcp_t* sock);
uint16_t flow_advertised_window(tju_tcp_t* sock);
/* 返回本轮允许发送的新数据字节数，并执行发送端 SWS 避免。 */
uint32_t flow_limited_sendable_bytes(uint32_t flight_size,
                                     uint32_t unsent_bytes,
                                     uint32_t window_allowed_bytes);

#endif
