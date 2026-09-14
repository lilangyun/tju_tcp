#include "flow_control.h"

#include "tju_tcp.h"

static uint32_t receive_sws_threshold(tju_tcp_t* sock){
    uint32_t half_buffer = (uint32_t)sock->recv_buf->capacity / 2;
    return (half_buffer < SMSS)? half_buffer: (uint32_t)SMSS;
}

uint32_t flow_receive_available_locked(tju_tcp_t* sock){
    int available = sock->recv_buf->capacity - sock->recv_buf->count;
    return (available > 0)? (uint32_t)available: 0;
}

uint16_t flow_advertised_window_locked(tju_tcp_t* sock){
    receiver_window_t* wnd = sock->recv_wnd;
    uint32_t available = flow_receive_available_locked(sock);
    uint16_t advertised;

    if(available == 0){
        wnd->last_advertised_window = 0;
        wnd->window_size = 0;
        return 0;
    }

    if(wnd->last_advertised_window == 0 &&
       available < receive_sws_threshold(sock)){
        wnd->window_size = 0;
        return 0;
    }

    advertised = (available > UINT16_MAX)?
                 (uint16_t)UINT16_MAX: (uint16_t)available;
    wnd->last_advertised_window = advertised;
    wnd->window_size = advertised;
    return advertised;
}

uint16_t flow_advertised_window(tju_tcp_t* sock){
    uint16_t window;

    pthread_mutex_lock(&(sock->recv_buf->lock));
    window = flow_advertised_window_locked(sock);
    pthread_mutex_unlock(&(sock->recv_buf->lock));
    return window;
}

int flow_should_defer_small_segment(int outstanding, int unsent){
    if(unsent <= 0 || unsent >= SMSS){
        return FALSE;
    }
    return outstanding > 0;
}
