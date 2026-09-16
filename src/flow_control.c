#include "flow_control.h"

#include "tju_tcp.h"

static uint32_t receive_sws_threshold(tju_tcp_t* sock){
    uint32_t half_buffer_capacity = (uint32_t)sock->receive_buffer->capacity / 2;
    return (half_buffer_capacity < SMSS)? half_buffer_capacity: (uint32_t)SMSS;
}

uint32_t flow_receive_available_locked(tju_tcp_t* sock){
    /* 连续可读数据和乱序缓存都占用接收缓冲区。 */
    int available_bytes =
        sock->receive_buffer->capacity - sock->receive_buffer->reserved_bytes;
    return (available_bytes > 0)? (uint32_t)available_bytes: 0;
}

uint16_t flow_advertised_window_locked(tju_tcp_t* sock){
    receive_window_t* receive_window = sock->receive_window;
    uint32_t available_bytes = flow_receive_available_locked(sock);
    uint16_t advertised_window;

    if(available_bytes == 0){
        receive_window->last_advertised_window = 0;
        receive_window->advertised_window = 0;
        return 0;
    }

    if(receive_window->last_advertised_window == 0 &&
       available_bytes < receive_sws_threshold(sock)){
        receive_window->advertised_window = 0;
        return 0;
    }

    advertised_window = (available_bytes > UINT16_MAX)?
                 (uint16_t)UINT16_MAX: (uint16_t)available_bytes;
    receive_window->last_advertised_window = advertised_window;
    receive_window->advertised_window = advertised_window;
    return advertised_window;
}

uint16_t flow_advertised_window(tju_tcp_t* sock){
    uint16_t advertised_window;

    pthread_mutex_lock(&(sock->receive_buffer->lock));
    advertised_window = flow_advertised_window_locked(sock);
    pthread_mutex_unlock(&(sock->receive_buffer->lock));
    return advertised_window;
}

/*
 * 计算本轮允许发送的新数据量，并实施发送端 SWS 避免。
 *
 * 如果发送缓冲区可以在本轮全部清空，允许发送最后一个小报文；如果仍有
 * 在途数据，则只有窗口至少容纳一个完整 SMSS 时才继续发送。窗口允许多个
 * SMSS 但仍有后续数据时，本轮只发送完整报文，避免 ACK 再把窗口放大后
 * 反复产生窗口边界处的小包。
 */
uint32_t flow_limited_sendable_bytes(uint32_t flight_size,
                                     uint32_t unsent_bytes,
                                     uint32_t window_allowed_bytes){
    uint32_t full_segments_bytes;

    if(unsent_bytes == 0 || window_allowed_bytes == 0){
        return 0;
    }

    if(unsent_bytes <= window_allowed_bytes){
        return unsent_bytes;
    }

    if(window_allowed_bytes < SMSS){
        return (flight_size == 0)? window_allowed_bytes: 0;
    }

    full_segments_bytes = (window_allowed_bytes / SMSS) * SMSS;
    return full_segments_bytes;
}
