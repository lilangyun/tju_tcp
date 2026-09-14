#ifndef _FLOW_CONTROL_H_
#define _FLOW_CONTROL_H_

#include "global.h"

uint32_t flow_receive_available_locked(tju_tcp_t* sock);
uint16_t flow_advertised_window_locked(tju_tcp_t* sock);
uint16_t flow_advertised_window(tju_tcp_t* sock);
int flow_should_defer_small_segment(int outstanding, int unsent);

#endif
