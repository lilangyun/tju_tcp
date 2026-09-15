#include "flow_control.h"

#include "tju_tcp.h"

static uint32_t receive_sws_threshold(tju_tcp_t* sock){
    uint32_t half_buffer_capacity = (uint32_t)sock->receive_buffer->capacity / 2;
    return (half_buffer_capacity < SMSS)? half_buffer_capacity: (uint32_t)SMSS;
}

uint32_t flow_receive_available_locked(tju_tcp_t* sock){
    int available_bytes =
        sock->receive_buffer->capacity - sock->receive_buffer->buffered_bytes;
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

int flow_should_defer_small_segment(uint32_t flight_size,
                                    uint32_t unsent_bytes){
    if(unsent_bytes == 0 || unsent_bytes >= SMSS){
        return FALSE;
    }
    return flight_size > 0;
}
