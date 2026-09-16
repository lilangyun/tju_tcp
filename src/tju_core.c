#include "tju_core.h"

#include "congestion_control.h"
#include "flow_control.h"
#include "queue.h"
#include "rtt.h"
#include "timer.h"
#include "trace.h"
#include "tju_tcp.h"

static int send_control_packet_with_advertised_window(tju_tcp_t* sock, uint8_t flags,
                                  uint32_t seq_num, uint32_t ack_num,
                                  uint16_t advertised_window);
static int send_control_packet(tju_tcp_t* sock, uint8_t flags, uint32_t seq_num,
                      uint32_t ack_num);
static void free_sock(tju_tcp_t* sock);
static void init_windows(tju_tcp_t* sock);
static void init_buffers(tju_tcp_t* sock);
static void* send_thread(void* arg);
static void* retransmission_thread(void* arg);
static void retransmit_oldest_segment_locked(tju_tcp_t* sock);
static int receive_bitmap_test(const receive_buffer_t* receive_buffer,
                               uint32_t index);
static void receive_bitmap_set(receive_buffer_t* receive_buffer,
                               uint32_t index);
static void receive_bitmap_clear(receive_buffer_t* receive_buffer,
                                 uint32_t index);
static uint32_t flush_contiguous_receive_data_locked(tju_tcp_t* sock);
static void store_receive_segment_locked(tju_tcp_t* sock,
                                        uint32_t sequence_offset,
                                        const char* data,
                                        uint32_t data_length);

static int send_control_packet(tju_tcp_t* sock, uint8_t flags, uint32_t seq_num,
                               uint32_t ack_num){
    return send_control_packet_with_advertised_window(sock, flags, seq_num, ack_num,
                                  flow_advertised_window(sock));
}

static int receive_bitmap_test(const receive_buffer_t* receive_buffer,
                               uint32_t index){
    uint32_t byte_index = index / 8U;
    uint8_t bit_mask = (uint8_t)(1U << (index % 8U));

    return (receive_buffer->received_bitmap[byte_index] & bit_mask) != 0;
}

static void receive_bitmap_set(receive_buffer_t* receive_buffer,
                               uint32_t index){
    uint32_t byte_index = index / 8U;
    uint8_t bit_mask = (uint8_t)(1U << (index % 8U));

    receive_buffer->received_bitmap[byte_index] |= bit_mask;
}

static void receive_bitmap_clear(receive_buffer_t* receive_buffer,
                                 uint32_t index){
    uint32_t byte_index = index / 8U;
    uint8_t bit_mask = (uint8_t)(1U << (index % 8U));

    receive_buffer->received_bitmap[byte_index] &=
        (uint8_t)~bit_mask;
}

static uint32_t flush_contiguous_receive_data_locked(tju_tcp_t* sock){
    receive_buffer_t* receive_buffer = sock->receive_buffer;
    uint32_t delivered_bytes = 0;

    /* bitmap 中连续的 bit 表示累计 ACK 可以推进，清除后数据转为可读。 */
    while(receive_bitmap_test(receive_buffer,
                              (uint32_t)receive_buffer->next_expected_index)){
        receive_bitmap_clear(receive_buffer,
                             (uint32_t)receive_buffer->next_expected_index);
        receive_buffer->next_expected_index =
            (receive_buffer->next_expected_index + 1) % receive_buffer->capacity;
        sock->receive_window->next_expected_seq++;
        receive_buffer->buffered_bytes++;
        delivered_bytes++;
    }

    return delivered_bytes;
}

static void store_receive_segment_locked(tju_tcp_t* sock,
                                        uint32_t sequence_offset,
                                        const char* data,
                                        uint32_t data_length){
    receive_buffer_t* receive_buffer = sock->receive_buffer;
    uint32_t index;
    uint32_t offset;
    uint32_t buffer_index;

    /* 乱序数据按序列号偏移写入环形缓冲区，并使用 bitmap 去重。 */
    for(index = 0; index < data_length; index++){
        offset = sequence_offset + index;
        buffer_index = ((uint32_t)receive_buffer->next_expected_index + offset) %
                       (uint32_t)receive_buffer->capacity;

        if(receive_bitmap_test(receive_buffer, buffer_index)){
            continue;
        }

        receive_buffer->mem[buffer_index] = data[index];
        receive_bitmap_set(receive_buffer, buffer_index);
        receive_buffer->reserved_bytes++;
    }
}

/* ============================ 规定 socket API ============================ */

/*
创建 TCP socket
初始化对应的结构体
设置初始状态为 CLOSED
*/
tju_tcp_t* tju_core_socket(void){
    tju_tcp_t* sock = (tju_tcp_t*)malloc(sizeof(tju_tcp_t));

    trace_init(); // 初始化Trace
    pthread_mutex_init(&(sock->state_lock), NULL);
    sock->state = CLOSED;
    sock->close_requested = FALSE;

    init_buffers(sock);

    return sock;
}

/*
绑定监听的地址 包括ip和端口
*/
int tju_core_bind(tju_tcp_t* sock, tju_sock_addr bind_addr){
    sock->bind_addr = bind_addr;
    return 0;
}

/*
被动打开 监听bind的地址和端口
设置socket的状态为LISTEN
注册该socket到内核的监听socket哈希表
*/
int tju_core_listen(tju_tcp_t* sock){
    sock->state = LISTEN;
    int hashval = cal_hash(sock->bind_addr.ip, sock->bind_addr.port, 0, 0);
    listen_socks[hashval] = sock;

    // 初始化半/全连接队列
    init_half_conn_queue(hashval);
    init_full_conn_queue(hashval);
    return 0;
}

/*
接受连接
返回与客户端通信用的socket
这里返回的socket一定是已经完成3次握手建立了连接的socket
因为只要该函数返回, 用户就可以马上使用该socket进行send和recv
*/
tju_tcp_t* tju_core_accept(tju_tcp_t* listen_sock){
    int hashval = cal_hash(listen_sock->bind_addr.ip, listen_sock->bind_addr.port, 0, 0);
    while(empty_full_conn_queue(hashval)){
        // 阻塞等待全连接队列不为空
        if(retransmission_pending[SYNACK_RETRY] && !empty_half_conn_queue(hashval)){
            // 重发SYN|ACK
            pthread_mutex_lock(half_conn_queue[hashval]->lock);
            conn_node front_node = *(half_conn_queue[hashval]->front);
            pthread_mutex_unlock(half_conn_queue[hashval]->lock);
            send_control_packet(front_node.sock, (SYN_FLAG_MASK | ACK_FLAG_MASK),
                        front_node.sock->local_isn, front_node.sock->remote_isn + 1);

            // 重新设置 SYN + ACK 超时监测
            front_node.sock->send_window->retransmit_timeout.tv_sec = 3;
            front_node.sock->send_window->retransmit_timeout.tv_usec = 0;
            timer_arm_event(front_node.sock, SYNACK_RETRY);
        }
    }
    // 从全连接队列中出队
    conn_node* new_conn_node = pop_full_conn_queue(hashval);
    tju_tcp_t* new_conn = new_conn_node->sock;

    // 连接已建立，启动发送和重传线程
    pthread_t send_thread_id, retransmission_thread_id;
    int result = pthread_create(&send_thread_id, NULL, send_thread, (void*)(new_conn));
    if (result<0){
        printf("ERROR create send_thread!\n");
        exit(-1);
    }
    new_conn->send_thread_id = send_thread_id;
    result = pthread_create(&retransmission_thread_id, NULL, retransmission_thread, (void*)(new_conn));
    if (result<0){
        printf("ERROR create retransmission_thread!\n");
        exit(-1);
    }
    new_conn->retransmission_thread_id = retransmission_thread_id;

    return new_conn;
}


/*
连接到服务端
该函数以一个socket为参数
调用函数前, 该socket还未建立连接
函数正常返回后, 该socket一定是已经完成了3次握手, 建立了连接
因为只要该函数返回, 用户就可以马上使用该socket进行send和recv
*/
int tju_core_connect(tju_tcp_t* sock, tju_sock_addr target_addr){
    // 设置四元组
    sock->established_remote_addr = target_addr;
    tju_sock_addr local_addr;
    local_addr.ip = inet_network(CLIENT_IP);
    local_addr.port = 5678; // 连接方进行connect连接的时候 内核中是随机分配一个可用的端口
    sock->established_local_addr = local_addr;

    // 三次握手
    // 递增时钟分量取模作为client_isn
    struct timespec sp;
    clock_gettime(CLOCK_MONOTONIC, &sp);
    uint32_t seq_num = sp.tv_sec;  // client_isn

    // 初始化 window
    sock->local_isn = seq_num;
    sock->remote_isn = 0;
    init_windows(sock);

    // 放入listen_socks哈希表，用于发送SYN后接收SYN+ACK
    int hashval = cal_hash(local_addr.ip, local_addr.port, 0, 0);
    listen_socks[hashval] = sock;

    // 发送 SYN
    send_control_packet(sock, SYN_FLAG_MASK, sock->local_isn, sock->remote_isn);
    pthread_mutex_lock(&sock->state_lock);
    sock->state = SYN_SENT;
    pthread_mutex_unlock(&sock->state_lock);

    // 设置 SYN 超时检测
    sock->send_window->retransmit_timeout.tv_sec = 1;
    sock->send_window->retransmit_timeout.tv_usec = 0;
    timer_arm_event(sock, SYN_RETRY);

    // 接收 SYN + ACK
    while (sock->state != ESTABLISHED){
        // 阻塞
        if(retransmission_pending[SYN_RETRY]){
            // 将连接建立后的重传超时设置为3s
            sock->send_window->retransmit_timeout.tv_sec = 3;
            sock->send_window->retransmit_timeout.tv_usec = 0;

            // 重传 SYN
            send_control_packet(sock, SYN_FLAG_MASK, sock->local_isn, sock->remote_isn);
            sock->send_window->retransmit_timeout.tv_sec = 3;
            sock->send_window->retransmit_timeout.tv_usec = 0;
            timer_arm_event(sock, SYN_RETRY);
        }
    }

    // 连接已建立，启动发送和重传线程
    pthread_t send_thread_id, retransmission_thread_id;
    int result = pthread_create(&send_thread_id, NULL, send_thread, (void*)(sock));
    if (result<0){
        printf("ERROR create send_thread!\n");
        exit(-1);
    }
    sock->send_thread_id = send_thread_id;
    result = pthread_create(&retransmission_thread_id, NULL, retransmission_thread, (void*)(sock));
    if (result<0){
        printf("ERROR create retransmission_thread!\n");
        exit(-1);
    }
    sock->retransmission_thread_id = retransmission_thread_id;

    return 0;
}

int tju_core_send(tju_tcp_t* sock, const void *buffer, int len){
    if(len <= 0) return len;

    int buffer_capacity, write_index, free_bytes;

    // 判断状态
    pthread_mutex_lock(&(sock->state_lock));
    if(sock->state != ESTABLISHED || sock->close_requested){
        printf("Now the connect is break! [SEND]\n");
        pthread_mutex_unlock(&(sock->state_lock));
        return 0;  // 不接受连接断开之后的发送请求
    }
    pthread_mutex_unlock(&(sock->state_lock));

    pthread_mutex_lock(&(sock->send_buffer->lock));  // 与发送线程抢占缓冲区
    buffer_capacity = sock->send_buffer->capacity;
    if(buffer_capacity < len) {
        printf("Send_buf doesn't have enough space!\n");
        pthread_mutex_unlock(&(sock->send_buffer->lock));
        return -1;
    }
    pthread_mutex_unlock(&(sock->send_buffer->lock));

    pthread_mutex_lock(&(sock->send_buffer->lock));
    // pthread_cond_wait returns with the mutex reacquired.
    while(1){
        write_index = sock->send_buffer->write_index;
        free_bytes = buffer_capacity - sock->send_buffer->unacked_bytes;
        if(free_bytes >= len){
            break;
        }
        pthread_cond_wait(&(sock->send_buffer->cond), &(sock->send_buffer->lock));
    }

    // 处理环形缓冲区
    if(write_index + len <= buffer_capacity){
        memcpy(sock->send_buffer->mem + write_index, buffer, len);
    }
    else{
        int first_part_bytes = buffer_capacity - write_index;
        int second_part_bytes = len - first_part_bytes;
        memcpy(sock->send_buffer->mem + write_index, buffer, first_part_bytes);
        memcpy(sock->send_buffer->mem, (const char*)buffer + first_part_bytes, second_part_bytes);
    }

    sock->send_buffer->write_index = (write_index + len) % buffer_capacity;
    sock->send_buffer->unacked_bytes += len;
    pthread_cond_broadcast(&(sock->send_buffer->cond));

    pthread_mutex_unlock(&(sock->send_buffer->lock));
    return len;
}

int tju_core_recv(tju_tcp_t* sock, void *buffer, int len){
    int read_index, buffer_capacity, bytes_read;
    int should_send_window_update = 0;
    uint16_t old_advertised_window, new_advertised_window;
    uint32_t window_update_seq, window_update_ack;
    char* dst = (char*)buffer;

    if(len <= 0){
        return 0;
    }

    pthread_mutex_lock(&(sock->state_lock));
    if(sock->state != ESTABLISHED && sock->state != CLOSE_WAIT){
        printf("Now the connect is break! [RECV]\n");
        pthread_mutex_unlock(&(sock->state_lock));
        return 0;  // 不接受连接断开之后的发送请求
    }
    pthread_mutex_unlock(&(sock->state_lock));

    // 有数据即可返回，避免最后一次读取不足len时永久阻塞。
    pthread_mutex_lock(&(sock->receive_buffer->lock));
    // pthread_cond_wait returns with the mutex reacquired.
    while(sock->receive_buffer->buffered_bytes == 0){
        pthread_cond_wait(&(sock->receive_buffer->cond), &(sock->receive_buffer->lock));
    }
    read_index = sock->receive_buffer->read_index;
    buffer_capacity = sock->receive_buffer->capacity;
    bytes_read = (sock->receive_buffer->buffered_bytes < len)? sock->receive_buffer->buffered_bytes: len;
    old_advertised_window = flow_advertised_window_locked(sock);

    if(read_index + bytes_read <= buffer_capacity){
        memcpy(dst, sock->receive_buffer->mem + read_index, bytes_read);
    }
    else{
        int first_part_bytes = buffer_capacity - read_index;
        int second_part_bytes = bytes_read - first_part_bytes;
        memcpy(dst, sock->receive_buffer->mem + read_index, first_part_bytes);
        memcpy(dst + first_part_bytes, sock->receive_buffer->mem, second_part_bytes);
    }
    // 推进接收缓冲区读取位置。
    sock->receive_buffer->read_index = (sock->receive_buffer->read_index + bytes_read) % buffer_capacity;
    sock->receive_buffer->buffered_bytes -= bytes_read;
    sock->receive_buffer->reserved_bytes -= (uint32_t)bytes_read;
    pthread_cond_broadcast(&(sock->receive_buffer->cond));
    new_advertised_window = flow_advertised_window_locked(sock);
    trace_rwnd(new_advertised_window);
    if(old_advertised_window == 0 && new_advertised_window > 0){
        should_send_window_update = 1;
        window_update_seq = sock->send_window->next_seq;
        window_update_ack = sock->receive_window->next_expected_seq;
    }

    pthread_mutex_unlock(&(sock->receive_buffer->lock)); // 解锁

    if(should_send_window_update){
        send_control_packet(sock, ACK_FLAG_MASK, window_update_seq, window_update_ack);
    }

    return bytes_read;
}

/* ========================== 收包处理与状态机 ========================== */

/*
 * 仿真内核收到完整 TCP 报文后的统一入口。
 *
 * 控制报文在连接状态机中处理；纯 ACK 报文负责推进发送窗口；带 payload
 * 的报文按字节序号进行重复检测、重叠裁剪、按序写入和累计确认。
 */
int tju_core_handle_packet(tju_tcp_t* sock, char* pkt){
    // 四元组
    tju_sock_addr local_addr, remote_addr;
    remote_addr.ip = inet_network(CLIENT_IP);
    remote_addr.port = get_src(pkt);
    local_addr.ip = inet_network(SERVER_IP);
    local_addr.port = get_dst(pkt);

    // 其余 Header 参数
    uint32_t data_len = get_plen(pkt) - DEFAULT_HEADER_LEN;
    uint8_t flags = get_flags(pkt);
    uint32_t seq_num = get_seq(pkt);
    uint32_t ack_num = get_ack(pkt);

    trace_recv(seq_num, ack_num, flags, data_len);

    pthread_mutex_lock(&sock->state_lock);
    int state = sock->state;

    /*##################################### 连接管理 ###########################################*/
    // Receiver: [server], Flags: [SYN]
    if(state == LISTEN && data_len == 0 && flags == SYN_FLAG_MASK){
        pthread_mutex_unlock(&sock->state_lock);

        // 新建socket: new_conn
        tju_tcp_t* new_conn = (tju_tcp_t*)malloc(sizeof(tju_tcp_t));
        memcpy(new_conn, sock, sizeof(tju_tcp_t));
        init_buffers(new_conn);
        new_conn->established_local_addr.port = local_addr.port; // 随机分配端口
        new_conn->established_local_addr.ip = local_addr.ip;
        new_conn->established_remote_addr = remote_addr;

        // 设置 server_isn
        struct timespec sp;
        clock_gettime(CLOCK_MONOTONIC, &sp);
        uint32_t server_isn = sp.tv_sec;

        // 初始化 window -> 用于用户线程重传 SYNACK
        new_conn->local_isn = server_isn;
        new_conn->remote_isn = seq_num;
        init_windows(new_conn);

        // 将new_conn加入listen_socket half_conn_queue
        int hashval = cal_hash(sock->bind_addr.ip, sock->bind_addr.port, 0, 0);
        pthread_mutex_lock(half_conn_queue[hashval]->lock);
        conn_node* new_conn_node = create_conn_node(new_conn);
        push_half_conn_queue(hashval, new_conn_node);
        pthread_mutex_unlock(half_conn_queue[hashval]->lock);

        // 放入内核连接表中
        hashval = cal_hash(new_conn->established_local_addr.ip,
                           new_conn->established_local_addr.port,
                           new_conn->established_remote_addr.ip,
                           new_conn->established_remote_addr.port);
        established_socks[hashval] = new_conn;

        // 发送 SYN + ACK
        send_control_packet(new_conn, (SYN_FLAG_MASK | ACK_FLAG_MASK), new_conn->local_isn, new_conn->remote_isn + 1);

        // 设置状态
        new_conn->state = SYN_RECV;

        // 设置 SYN + ACK 超时检测
        new_conn->send_window->retransmit_timeout.tv_sec = 1;
        new_conn->send_window->retransmit_timeout.tv_usec = 0;
        timer_arm_event(new_conn, SYNACK_RETRY);

        return 0;
    }
    // Receiver: [client], Flags: [SYN + ACK]
    else if(state == SYN_SENT && data_len == 0 && flags == (SYN_FLAG_MASK | ACK_FLAG_MASK)){
        pthread_mutex_unlock(&sock->state_lock);

        // 取消 SYN 超时检测
        timer_cancel_event(SYN_RETRY);

        // 更换连接的远程地址为new_conn，而不是bind_addr
        sock->established_remote_addr = remote_addr;

        // 设置 window
        sock->remote_isn = seq_num;
        sock->receive_window->next_expected_seq = seq_num + 1;  // 更新
        sock->send_window->rwnd = get_advertised_window(pkt);
        trace_swnd(congestion_effective_window(sock->send_window));

        // 发送ACK
        send_control_packet(sock, ACK_FLAG_MASK, sock->local_isn + 1, sock->remote_isn + 1);
        sock->state = ESTABLISHED;

        // 从listen_socks哈希表中取消
        int hashval = cal_hash(local_addr.ip, local_addr.port, 0, 0);
        listen_socks[hashval] = NULL;

        // 放入established_socks哈希表中
        int conn_hashval = cal_hash(local_addr.ip,
                                    local_addr.port,
                                    remote_addr.ip,
                                    remote_addr.port);
        established_socks[conn_hashval] = sock;

        return 0;
    }
    // Receiver: [client], Flags: [SYN + ACK]  /* 收到重复 SYNACK */
    else if(state == ESTABLISHED && data_len == 0 && flags == (SYN_FLAG_MASK | ACK_FLAG_MASK)){
        pthread_mutex_unlock(&sock->state_lock);

        // 对SYNACK重复响应
        send_control_packet(sock, ACK_FLAG_MASK, sock->local_isn + 1, sock->remote_isn + 1);

        return 0;
    }
    // Receiver: [server], Flags: [ACK]
    else if(state == SYN_RECV && data_len == 0 && flags == ACK_FLAG_MASK){
        pthread_mutex_unlock(&sock->state_lock);

        // 取消 SYN + ACK 超时检测
        timer_cancel_event(SYNACK_RETRY);

        // 出半连接队列
        int hashval = cal_hash(sock->bind_addr.ip,
                               sock->bind_addr.port,
                               0,
                               0);
        int conn_hashval = cal_hash(local_addr.ip,
                                    local_addr.port,
                                    remote_addr.ip,
                                    remote_addr.port);
        pthread_mutex_lock(half_conn_queue[hashval]->lock);
        conn_node* new_conn_node = remove_half_conn_queue(hashval, conn_hashval);
        if(new_conn_node == NULL){
            printf("Server can't find hash = %d node, only has hash = %d.\n", conn_hashval, hashval);
            exit(1);
        }
        pthread_mutex_unlock(half_conn_queue[hashval]->lock);

        // 入全连接队列
        pthread_mutex_lock(full_conn_queue[hashval]->lock);
        push_full_conn_queue(hashval, new_conn_node);
        pthread_mutex_unlock(full_conn_queue[hashval]->lock);

        // 状态转移
        tju_tcp_t* new_conn = new_conn_node->sock;
        new_conn->state = ESTABLISHED;
        new_conn->send_window->rwnd = get_advertised_window(pkt);
        trace_swnd(congestion_effective_window(new_conn->send_window));

        return 0;
    /* ################################# 三次握手状态机结束 #######################################*/
    }
    /* ################################# 四次挥手状态机开始 #######################################*/
    // Receiver: [Passive], Flags: [ACK | FIN]
    else if((state == ESTABLISHED) && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){
        // 设置 window
        sock->local_fin_seq = ack_num;
        sock->remote_fin_seq = seq_num;

        send_control_packet(sock, (ACK_FLAG_MASK), sock->local_fin_seq, sock->remote_fin_seq + 1);

        sock->state = CLOSE_WAIT;
        pthread_mutex_unlock(&sock->state_lock);
        return 0;
    }
    // Receiver: [Active], Flags: [ACK]
    else if((state == FIN_WAIT_1) && data_len == 0 && flags == (ACK_FLAG_MASK)){
        if(ack_num != sock->local_fin_seq + 1){  // 校验，有可能是对发送数据的ACK
            pthread_mutex_unlock(&(sock->state_lock));
            return 0;
        }

        timer_cancel_event(FINACK_RETRY);

        // 状态转移->FIN_WAIT_2
        sock->state = FIN_WAIT_2;

        pthread_mutex_unlock(&sock->state_lock);

        return 0;
    }
    // Receiver: [Active], Flags: [ACK | FIN]
    else if((state == FIN_WAIT_2) && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){
        /* 保存对端 FIN 序号，最终 ACK 必须确认该 FIN。 */
        sock->remote_fin_seq = seq_num;

        // 取消FINACK超时监控
        timer_cancel_event(FINACK_RETRY);

        // 发送ACK
        send_control_packet(sock, (ACK_FLAG_MASK), sock->local_fin_seq + 1, sock->remote_fin_seq + 1);

        sock->state = TIME_WAIT;

        pthread_mutex_unlock(&sock->state_lock);

        return 0;
    }
    // Receiver: [Passive], Flags: [ACK]
    else if((state == LAST_ACK) && data_len == 0 && flags == ACK_FLAG_MASK){

        // 最终 ACK 到达，被动关闭结束，停掉 FINACK 重传计时后再释放连接。
        timer_cancel_event(FINACK_RETRY);

        sock->state = CLOSED;
        pthread_mutex_unlock(&sock->state_lock);
        return 0;
    }
    // Receiver: [Active], Flags: [FINACK]   /* TIME_WAIT 期间收到重复 FINACK */
    else if((state == TIME_WAIT) && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){
        pthread_mutex_unlock(&sock->state_lock);

        // 最终 ACK 可能已经丢失，对端仍停留在 LAST_ACK 并重传 FINACK。
        // 本端在 TIME_WAIT 中应再次发送 ACK，否则对端会一直重传。
        send_control_packet(sock, (ACK_FLAG_MASK), sock->local_fin_seq + 1, sock->remote_fin_seq + 1);

        return 0;
    }
    /*################################### 同时关闭连接 #######################################*/
    // Receiver: [peer], Flags: [FINACK]
    else if((state == FIN_WAIT_1) && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){
        sock->remote_fin_seq = seq_num;

        if(ack_num == sock->local_fin_seq + 1){
            /* 对端的FIN|ACK已同时确认了我方的FIN（被动方发FIN时ack=remote_fin_seq+1），
             * 一次交互即完成同时关闭：回最终ACK后直接进入TIME_WAIT。
             * 若仍走CLOSING，将等待一个永远不会到来的独立ACK而阻塞。 */
            send_control_packet(sock, (ACK_FLAG_MASK), sock->local_fin_seq + 1, sock->remote_fin_seq + 1);
            sock->state = TIME_WAIT;
        }
        else{
            /* 对端只确认了数据，我方FIN尚未被确认：进入CLOSING等待独立ACK */
            send_control_packet(sock, (ACK_FLAG_MASK), sock->local_fin_seq + 1, sock->remote_fin_seq + 1);
            sock->state = CLOSING;
        }

        pthread_mutex_unlock(&sock->state_lock);

        return 0;
    }
    // Receiver: [peer], Flags: [ACK]
    else if((state == CLOSING) && data_len == 0 && flags == ACK_FLAG_MASK){

        // 取消 FINACK 计时
        timer_cancel_event(FINACK_RETRY);

        sock->state = TIME_WAIT;
        pthread_mutex_unlock(&sock->state_lock);

        return 0;
    /*################################### 连接管理结束 #######################################*/
    }
    /*################################# 数据发送端处理ACK ####################################*/
    else if((state == ESTABLISHED || state == CLOSE_WAIT) && data_len == 0 && flags == ACK_FLAG_MASK){
        pthread_mutex_unlock(&sock->state_lock);

        pthread_mutex_lock(&(sock->send_buffer->lock));
        uint32_t base_seq = sock->send_window->base_seq;
        uint32_t next_seq = sock->send_window->next_seq;
        int buffer_capacity = sock->send_buffer->capacity;
        uint32_t acked_bytes = 0;
        uint32_t flight_size = next_seq - base_seq;
        if(flight_size > (uint32_t)sock->send_buffer->unacked_bytes){
            flight_size = (uint32_t)sock->send_buffer->unacked_bytes;
        }
        uint32_t ack_advance = ack_num - base_seq;
        uint16_t advertised_window = get_advertised_window(pkt);
        int rwnd_changed = (sock->send_window->rwnd != advertised_window);

        sock->send_window->rwnd = advertised_window;  // 限制
        if(rwnd_changed){
            trace_swnd(congestion_effective_window(sock->send_window));
            if(advertised_window > 0){
                timer_cancel_zero_window_probe(sock);
            }
            pthread_cond_broadcast(&(sock->send_buffer->cond));
        }
        /* 校验 ACK 是否落在当前发送窗口的合法范围内。 */
        if(ack_advance > 0 && ack_advance <= flight_size){
            acked_bytes = ack_advance;
        }

        // 推进发送缓冲区确认基址和发送窗口左边界。
        if(acked_bytes > 0){
            /* 在移动 base_seq 前判断该累计 ACK 是否覆盖当前测量报文。 */
            rtt_update_from_ack(sock, ack_num);

            /* 新 ACK 推进拥塞窗口；恢复阶段的类型为拥塞避免。 */
            int cwnd_event_type =
                (sock->send_window->fast_retransmit_ack_pending ||
                 sock->send_window->cwnd >= sock->send_window->ssthresh)?
                CONGESTION_AVOIDANCE: SLOW_START;
            congestion_on_new_ack(sock->send_window, acked_bytes);
            trace_cwnd(cwnd_event_type, sock->send_window->cwnd);
            trace_swnd(congestion_effective_window(sock->send_window));

            sock->send_window->duplicate_ack_count = 0;
            sock->send_buffer->ack_base_index = (sock->send_buffer->ack_base_index + acked_bytes) % buffer_capacity;
            sock->send_buffer->unacked_bytes -= acked_bytes;
            pthread_cond_broadcast(&(sock->send_buffer->cond));
            sock->send_window->base_seq = ack_num;  // 累计确认
            if(sock->send_window->base_seq == sock->send_window->next_seq){
                rtt_reset_measurement(sock->send_window);
                timer_cancel_data_retransmission(sock);
            }
            else{
                /* 仍有未确认数据时，以最新 ACK 时刻重置定时器。 */
                timer_arm_data_retransmission(sock);
            }
        }
        else if(ack_num == base_seq){
            if(base_seq != next_seq && sock->send_window->rwnd > 0){
                sock->send_window->duplicate_ack_count++;
            }
            if(sock->send_window->duplicate_ack_count >= 3 &&
               !sock->send_window->fast_retransmit_ack_pending){
                // 三次重复 ACK：降低拥塞窗口并立即重传最早未确认数据。
                congestion_on_fast_retransmit(sock->send_window, flight_size);
                trace_cwnd(CONGESTION_FAST_RETRANSMIT,
                           sock->send_window->cwnd);
                trace_swnd(congestion_effective_window(sock->send_window));
                retransmit_oldest_segment_locked(sock);
                timer_arm_data_retransmission(sock);
                sock->send_window->duplicate_ack_count = 0;
            }
        }

        pthread_mutex_unlock(&(sock->send_buffer->lock));

        return 0;
    }
    pthread_mutex_unlock(&sock->state_lock);
    /*#################################### 接收真实数据 #####################################*/

    // 返回值非0，说明需要加锁失败，要一直尝试加锁
    while(pthread_mutex_lock(&(sock->receive_buffer->lock)) != 0); // 加锁

    // 只接收当前通告窗口允许的数据。
    uint32_t expected_seq = sock->receive_window->next_expected_seq;
    uint16_t advertised_window = flow_advertised_window_locked(sock);
    uint32_t accepted_bytes = data_len;
    uint32_t segment_offset = 0;
    uint32_t delivered_bytes = 0;
    uint32_t ack_to_send;

    if(accepted_bytes == 0){
        pthread_mutex_unlock(&(sock->receive_buffer->lock)); // 解锁
        return 0;
    }

    int32_t sequence_delta = (int32_t)(seq_num - expected_seq);
    if(sequence_delta < 0){
        segment_offset = (uint32_t)(-sequence_delta);
        if(segment_offset >= accepted_bytes){
            accepted_bytes = 0;
        }
        else{
            accepted_bytes -= segment_offset;
            sequence_delta = 0;
        }
    }

    if(accepted_bytes > 0 && advertised_window > 0 &&
       (uint32_t)sequence_delta < advertised_window){
        uint32_t window_remaining =
            (uint32_t)advertised_window - (uint32_t)sequence_delta;

        if(accepted_bytes > window_remaining){
            accepted_bytes = window_remaining;
        }
    }
    else{
        accepted_bytes = 0;
    }

    if(accepted_bytes > 0){
        store_receive_segment_locked(sock, (uint32_t)sequence_delta,
                                     pkt + DEFAULT_HEADER_LEN + segment_offset,
                                     accepted_bytes);
        delivered_bytes = flush_contiguous_receive_data_locked(sock);

        if(delivered_bytes > 0){
            trace_deliver(expected_seq, delivered_bytes);
            pthread_cond_broadcast(&(sock->receive_buffer->cond));
        }
    }

    // 无论是否补齐缺口，都回送当前累计确认点和最新窗口。
    advertised_window = flow_advertised_window_locked(sock);
    trace_rwnd(advertised_window);
    ack_to_send = sock->receive_window->next_expected_seq;
    // 注意：此处不能 timer_cancel_event(DATA_RETRY) —— 全局itimer被所有通道共用，
    // 双向传输时收到数据会误杀本端发送方向的重传定时器
    pthread_mutex_unlock(&(sock->receive_buffer->lock)); // 解锁
    send_control_packet_with_advertised_window(sock, ACK_FLAG_MASK, sock->send_window->next_seq,
                           ack_to_send, advertised_window);
    return 0;
}

/* ============================== 连接关闭 ============================== */

int tju_core_close(tju_tcp_t* sock){
    uint32_t seq_num, ack_num;

    pthread_mutex_lock(&(sock->state_lock));  // 与后台线程抢锁
    int active_close = (sock->state == ESTABLISHED);  // 确定主动被动
    sock->close_requested = TRUE;

    if(active_close){
        pthread_mutex_unlock(&(sock->state_lock));
        // 此时还取着状态锁
        pthread_mutex_lock(&(sock->send_buffer->lock));
        while(sock->send_buffer->unacked_bytes > 0){
            pthread_cond_wait(&(sock->send_buffer->cond), &(sock->send_buffer->lock));
        }
        pthread_mutex_unlock(&(sock->send_buffer->lock));

        // 如果刷完数据之后收到FIN，可以切换为被动路径
        pthread_mutex_lock(&(sock->state_lock));
        if(sock->state == CLOSE_WAIT){
            active_close = 0;  // 保持持锁状态，交给下方被动路径开头的unlock
        }
        else{
            /* 决策、发FIN、置FIN_WAIT_1必须在同一把state_lock内原子完成，
             * 保证收包线程不可能在此间隙把对端FIN按被动分支消费掉 */
            seq_num = sock->send_window->next_seq;
            ack_num = sock->receive_window->next_expected_seq;
            sock->local_fin_seq = seq_num;
            sock->remote_fin_seq = ack_num;

            send_control_packet(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK), seq_num, ack_num);
            sock->state = FIN_WAIT_1;
            pthread_mutex_unlock(&(sock->state_lock));  // 必须更新状态后再放锁
            sock->send_window->retransmit_timeout.tv_sec = 3;
            sock->send_window->retransmit_timeout.tv_usec = 0;
            timer_arm_event(sock, FINACK_RETRY);

            pthread_mutex_lock(&(sock->send_buffer->lock));
            pthread_cond_broadcast(&(sock->send_buffer->cond));
            pthread_mutex_unlock(&(sock->send_buffer->lock));
        }
    }

    if(active_close){
        while(1){
            pthread_mutex_lock(&(sock->state_lock));
            if(sock->state == TIME_WAIT){
                pthread_mutex_unlock(&(sock->state_lock));
                break;
            }
            if(retransmission_pending[FINACK_RETRY]){
                // 重传 FINACK
                uint32_t retransmit_ack = sock->remote_fin_seq;
                if(sock->state == CLOSING){
                    retransmit_ack += 1;
                }
                send_control_packet(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK),
                           sock->local_fin_seq, retransmit_ack);
                sock->send_window->retransmit_timeout.tv_sec = 3;
                sock->send_window->retransmit_timeout.tv_usec = 0;
                timer_arm_event(sock, FINACK_RETRY);
            }
            pthread_mutex_unlock(&(sock->state_lock));
        }
        // 主动方进入 TIME_WAIT 后先清掉关闭相关计时器，再等待 2 * MSL
        timer_cancel_event(FINACK_RETRY);
        sleep(2 * MSL);

        pthread_mutex_lock(&sock->state_lock);
        sock->state = CLOSED;
        pthread_mutex_unlock(&sock->state_lock);

    }
    // 被动关闭：本端是被动方，只需等待对端最终 ACK 后进入 CLOSED。
    else{
        pthread_mutex_unlock(&(sock->state_lock));
        while(1){
            pthread_mutex_lock(&(sock->state_lock));
            if(sock->state == CLOSE_WAIT){  // 等待发送ACK
                pthread_mutex_unlock(&(sock->state_lock));
                break;
            }
            pthread_mutex_unlock(&(sock->state_lock));
            usleep(1000);  // 避免忙等抢占CPU
        }

        // 等待应用读取完接收缓冲区。
        pthread_mutex_lock(&(sock->receive_buffer->lock));
        while(sock->receive_buffer->buffered_bytes > 0){
            pthread_cond_wait(&(sock->receive_buffer->cond), &(sock->receive_buffer->lock));
        }
        pthread_mutex_unlock(&(sock->receive_buffer->lock));

        pthread_mutex_lock(&(sock->send_buffer->lock));
        while(sock->send_buffer->unacked_bytes > 0){
            pthread_cond_wait(&(sock->send_buffer->cond), &(sock->send_buffer->lock));
        }
        pthread_mutex_unlock(&(sock->send_buffer->lock));

        send_control_packet(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK), sock->local_fin_seq, sock->remote_fin_seq + 1);
        sock->send_window->retransmit_timeout.tv_sec = 3;
        sock->send_window->retransmit_timeout.tv_usec = 0;
        timer_arm_event(sock, FINACK_RETRY);

        pthread_mutex_lock(&(sock->state_lock));
        sock->state = LAST_ACK;
        pthread_mutex_unlock(&(sock->state_lock));
        pthread_mutex_lock(&(sock->send_buffer->lock));
        pthread_cond_broadcast(&(sock->send_buffer->cond));
        pthread_mutex_unlock(&(sock->send_buffer->lock));

        while(1){
            pthread_mutex_lock(&(sock->state_lock));
            if(sock->state == CLOSED){
                pthread_mutex_unlock(&(sock->state_lock));
                break;
            }
            if(retransmission_pending[FINACK_RETRY]){
                // 被动方已发送 FINACK，仍未收到最终 ACK 时重传 FINACK。
                send_control_packet(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK), sock->local_fin_seq, sock->remote_fin_seq + 1);
                sock->send_window->retransmit_timeout.tv_sec = 3;
                sock->send_window->retransmit_timeout.tv_usec = 0;
                timer_arm_event(sock, FINACK_RETRY);
            }
            pthread_mutex_unlock(&(sock->state_lock));
        }
        timer_cancel_event(FINACK_RETRY);
    }
    /* 回收线程 */
    if(sock->send_thread_id != 0){
        pthread_join(sock->send_thread_id, NULL);
    }
    if(sock->retransmission_thread_id != 0){
        pthread_join(sock->retransmission_thread_id, NULL);
    }

    return 0;
}

/* =========================== 发送与重传线程 =========================== */

/*
 * 发送线程在每个循环重新计算 FlightSize。
 * 实际发送上限为 min(rwnd, cwnd)，避免将接收窗口和拥塞窗口混用。
 */
static void* send_thread(void* arg){
    tju_tcp_t* sock = (tju_tcp_t*)(arg);
    uint32_t seq_num, ack_num;
    uint32_t rwnd, cwnd, flight_size, unsent_bytes;
    uint32_t usable_window, usable_window_remaining, sendable_bytes;
    uint32_t segment_size, remaining_bytes;
    int packet_length;
    int send_buffer_index, buffer_capacity;
    char* data, *msg;
    uint32_t current_seq, buffer_offset;

    while(1){
        // 确认状态
        pthread_mutex_lock(&(sock->state_lock));
        if(sock->state != ESTABLISHED && sock->state != CLOSE_WAIT){
            pthread_mutex_unlock(&(sock->state_lock));
            break;
        }
        pthread_mutex_unlock(&(sock->state_lock));

        /* 锁定发送缓冲区及窗口状态。 */
        pthread_mutex_lock(&(sock->send_buffer->lock));
        ack_num = sock->receive_window->next_expected_seq;
        seq_num = sock->send_window->next_seq;
        rwnd = sock->send_window->rwnd;
        cwnd = sock->send_window->cwnd;
        usable_window = (rwnd < cwnd)? rwnd: cwnd;
        send_buffer_index = sock->send_buffer->send_next_index;
        buffer_capacity = sock->send_buffer->capacity;
        flight_size = seq_num - sock->send_window->base_seq;
        if(flight_size > (uint32_t)sock->send_buffer->unacked_bytes){
            flight_size = (uint32_t)sock->send_buffer->unacked_bytes;
        }
        unsent_bytes = (uint32_t)sock->send_buffer->unacked_bytes - flight_size;
        usable_window_remaining = (usable_window > flight_size)?
                           (usable_window - flight_size): 0U;
        sendable_bytes = flow_limited_sendable_bytes(flight_size, unsent_bytes,
                                                     usable_window_remaining);

        if(sendable_bytes > 0){
            timer_cancel_zero_window_probe(sock);
        }

        remaining_bytes = sendable_bytes;
        current_seq = seq_num;
        buffer_offset = send_buffer_index;

        while(remaining_bytes > 0){
            /* SMSS约束 */
            segment_size = (remaining_bytes < SMSS)? remaining_bytes: SMSS;

            /* 只在实际发送新数据时启动测量，避免从 ACK 时刻伪造发送时间。 */
            if(!sock->send_window->rtt_measurement_pending){
                rtt_start_measurement(sock, current_seq, (uint32_t)segment_size);
            }

            /* 从缓冲区中取数据 */
            if(segment_size != 0 &&
               buffer_offset + (uint32_t)segment_size <= (uint32_t)buffer_capacity){
                data = (char*)malloc(segment_size * sizeof(char));
                memcpy(data, sock->send_buffer->mem + buffer_offset, segment_size);
            }
            else{
                uint32_t first_part_bytes = buffer_capacity - buffer_offset;
                uint32_t second_part_bytes = segment_size - first_part_bytes;
                data = (char*)malloc(segment_size * sizeof(char));
                memcpy(data, sock->send_buffer->mem + buffer_offset,
                       first_part_bytes);
                memcpy(data + first_part_bytes, sock->send_buffer->mem,
                       second_part_bytes);
            }

            /* 构造并发送一个不超过 SMSS 的数据报文。 */
            packet_length = DEFAULT_HEADER_LEN + segment_size;
            msg = create_packet_buf(sock->established_local_addr.port,
                    sock->established_remote_addr.port, current_seq, ack_num,
                    DEFAULT_HEADER_LEN, packet_length,
                    NO_FLAG, flow_advertised_window(sock), 0, data, segment_size);
            sendToLayer3(msg, packet_length);
            trace_send(current_seq, ack_num, NO_FLAG, (uint32_t)segment_size);
            free(data);

            free(msg);

            /* 同时推进序列号和环形缓冲区下标。 */
            current_seq = (uint32_t)(current_seq + segment_size);
            buffer_offset = (buffer_offset + segment_size) % buffer_capacity;
            remaining_bytes -= segment_size;
        }
        /* 提交本轮发送进度，零窗口探测线程后续可能继续推进。 */
        sock->send_window->next_seq = current_seq;
        sock->send_buffer->send_next_index = buffer_offset;
        pthread_cond_broadcast(&(sock->send_buffer->cond));

        if(sendable_bytes == 0){
            if(rwnd == 0 && flight_size == 0 && unsent_bytes > 0){
                timer_arm_zero_window_probe(sock);
            }
            pthread_cond_wait(&(sock->send_buffer->cond), &(sock->send_buffer->lock));
        }

        if(sendable_bytes > 0 && sock->send_window->base_seq == seq_num){
            timer_arm_data_retransmission(sock);
        }

        /* 释放发送缓冲区锁。 */
        pthread_mutex_unlock(&(sock->send_buffer->lock));
    }

    return NULL;
}

/*
 * 零窗口探测。
 *
 * 接收窗口为 0 且发送缓冲区仍有数据时，发送一个尚未发送的字节。
 * 如果接收端仍无空间，会返回当前 ACK 和 0 窗口；如果窗口已恢复，
 * 该字节会被正常接收并推进累计确认。探测报文也进入普通数据重传
 * 定时器，因此后续探测间隔会按 RTO 指数退避。
 */
static int send_zero_window_probe_locked(tju_tcp_t* sock){
    send_window_t* send_window = sock->send_window;
    uint32_t seq_num;
    uint32_t ack_num;
    uint32_t buffer_offset;
    uint16_t advertised_window;
    char data;
    char* msg;
    int packet_length;

    if(send_window->base_seq != send_window->next_seq ||
       sock->send_buffer->unacked_bytes <= 0 ||
       sock->send_buffer->send_next_index == sock->send_buffer->write_index){
        return 0;
    }

    seq_num = send_window->next_seq;
    ack_num = sock->receive_window->next_expected_seq;
    buffer_offset = (uint32_t)sock->send_buffer->send_next_index;
    data = sock->send_buffer->mem[buffer_offset];
    advertised_window = flow_advertised_window(sock);

    packet_length = DEFAULT_HEADER_LEN + 1;
    msg = create_packet_buf(sock->established_local_addr.port,
                            sock->established_remote_addr.port,
                            seq_num, ack_num, DEFAULT_HEADER_LEN, packet_length,
                            NO_FLAG, advertised_window, 0, &data, 1);
    sendToLayer3(msg, packet_length);
    trace_send(seq_num, ack_num, NO_FLAG, 1);
    free(msg);

    send_window->next_seq = seq_num + 1;
    sock->send_buffer->send_next_index = (sock->send_buffer->send_next_index + 1) % sock->send_buffer->capacity;
    rtt_reset_measurement(send_window);
    timer_arm_data_retransmission(sock);
    pthread_cond_broadcast(&(sock->send_buffer->cond));
    return 1;
}

/* 处理数据超时和零窗口超时。 */
static void* retransmission_thread(void* arg){
    tju_tcp_t* sock = (tju_tcp_t*)(arg);

    while(1){
        // 确认状态是否为连接中
        pthread_mutex_lock(&(sock->state_lock));
        if(sock->state != ESTABLISHED && sock->state != CLOSE_WAIT){
            pthread_mutex_unlock(&(sock->state_lock));
            break;
        }
        pthread_mutex_unlock(&(sock->state_lock));

        // 判断当前是否数据超时
        if(retransmission_pending[DATA_RETRY]){  // 重传 [ack_base_index, send_next_index) 中的最早一段。
            pthread_mutex_lock(&(sock->send_buffer->lock));
            int has_flight_size =
                (sock->send_window->base_seq != sock->send_window->next_seq);
            if(has_flight_size){
                uint32_t flight_size =
                    sock->send_window->next_seq - sock->send_window->base_seq;

                if(flight_size > (uint32_t)sock->send_buffer->unacked_bytes){
                    flight_size = (uint32_t)sock->send_buffer->unacked_bytes;
                }

                congestion_on_timeout(sock->send_window, flight_size);
                trace_cwnd(CONGESTION_TIMEOUT, sock->send_window->cwnd);
                trace_swnd(congestion_effective_window(sock->send_window));
                sock->send_window->duplicate_ack_count = 0;
                rtt_backoff(sock);
                retransmit_oldest_segment_locked(sock);
            }
            if(has_flight_size){
                timer_arm_data_retransmission(sock);
            }
            else{
                timer_cancel_data_retransmission(sock);
            }
            pthread_mutex_unlock(&(sock->send_buffer->lock));
        }

        if(retransmission_pending[ZERO_WINDOW_RETRY]){
            pthread_mutex_lock(&(sock->send_buffer->lock));
            int probe_active = timer_zero_window_probe_active(sock->send_window);

            if(probe_active){
                timer_cancel_zero_window_probe(sock);
            }
            if(probe_active &&
               sock->send_window->rwnd == 0 &&
               sock->send_window->base_seq == sock->send_window->next_seq){
                send_zero_window_probe_locked(sock);
            }
            else{
                timer_cancel_event(ZERO_WINDOW_RETRY);
            }
            pthread_mutex_unlock(&(sock->send_buffer->lock));
        }
        usleep(100);  // 避免纯自旋抢占CPU
    }
    return NULL;
}

/* ======================== 报文发送与资源释放辅助函数 ======================== */

static int send_control_packet_with_advertised_window(tju_tcp_t* sock, uint8_t flags,
                                  uint32_t seq_num, uint32_t ack_num,
                                  uint16_t advertised_window){
    char* msg;
    uint16_t packet_length = DEFAULT_HEADER_LEN;  // 无 Payload

    tju_sock_addr local_addr = sock->established_local_addr;
    tju_sock_addr remote_addr = sock->established_remote_addr;

    msg = create_packet_buf(local_addr.port, remote_addr.port, seq_num, ack_num,
              DEFAULT_HEADER_LEN, packet_length, flags, advertised_window, 0, NULL, 0);
    sendToLayer3(msg, packet_length);
    trace_send(seq_num, ack_num, flags, 0);
    free(msg);

    return 0;
}

// 释放sock对应的连接资源
static void free_sock(tju_tcp_t* sock){
    int hashval = cal_hash(sock->established_local_addr.ip, sock->established_local_addr.port,
                           sock->established_remote_addr.ip, sock->established_remote_addr.port);
    established_socks[hashval] = NULL;

    free(sock->send_buffer->mem);
    free(sock->send_window);
    pthread_cond_destroy(&(sock->send_buffer->cond));
    pthread_mutex_destroy(&(sock->send_buffer->lock));
    free(sock->send_buffer);

    free(sock->receive_buffer->mem);
    free(sock->receive_buffer->received_bitmap);
    free(sock->receive_window);
    pthread_cond_destroy(&(sock->receive_buffer->cond));
    pthread_mutex_destroy(&(sock->receive_buffer->lock));
    free(sock->receive_buffer);

    pthread_mutex_destroy(&(sock->state_lock));

    free(sock);

    destroy_half_conn_queue(hashval);
    destroy_full_conn_queue(hashval);
}

static void init_buffers(tju_tcp_t* sock){
    int receive_bitmap_size;

    sock->send_buffer = (send_buffer_t*)malloc(sizeof(send_buffer_t));
    pthread_mutex_init(&(sock->send_buffer->lock), NULL);
    pthread_cond_init(&(sock->send_buffer->cond), NULL);
    sock->send_buffer->mem = (char*)malloc(SEND_BUFFER_SIZE * sizeof(char));
    sock->send_buffer->capacity = SEND_BUFFER_SIZE;
    sock->send_buffer->ack_base_index = 0;
    sock->send_buffer->send_next_index = 0;
    sock->send_buffer->write_index = 0;
    sock->send_buffer->unacked_bytes = 0;

    sock->receive_buffer = (receive_buffer_t*)malloc(sizeof(receive_buffer_t));
    pthread_mutex_init(&(sock->receive_buffer->lock), NULL);
    pthread_cond_init(&(sock->receive_buffer->cond), NULL);
    sock->receive_buffer->mem =
        (char*)malloc(RECEIVE_BUFFER_SIZE * sizeof(char));
    sock->receive_buffer->capacity = RECEIVE_BUFFER_SIZE;
    receive_bitmap_size = (RECEIVE_BUFFER_SIZE + 7) / 8;
    sock->receive_buffer->received_bitmap =
        (uint8_t*)calloc((size_t)receive_bitmap_size, sizeof(uint8_t));
    if(sock->receive_buffer->received_bitmap == NULL){
        perror("calloc receive buffer bitmap");
        exit(-1);
    }
    sock->receive_buffer->received_bitmap_size = receive_bitmap_size;
    sock->receive_buffer->read_index = 0;
    sock->receive_buffer->next_expected_index = 0;
    sock->receive_buffer->buffered_bytes = 0;
    sock->receive_buffer->reserved_bytes = 0;
}

// 初始化窗口
static void init_windows(tju_tcp_t* sock){
    sock->send_window = (send_window_t *)(malloc(sizeof(send_window_t)));
    sock->send_window->rwnd = UINT16_MAX;
    sock->send_window->base_seq = sock->local_isn + 1;
    sock->send_window->duplicate_ack_count = 0;
    timer_state_reset(sock->send_window);
    rtt_init(sock->send_window);
    congestion_init(sock->send_window, TCP_RECV_WINDOW_SIZE);
    trace_cwnd(SLOW_START, sock->send_window->cwnd);
    trace_swnd(congestion_effective_window(sock->send_window));
    sock->send_buffer->ack_base_index = 0;  // 没发出任何数据
    sock->send_window->next_seq = sock->local_isn + 1;
    sock->send_buffer->unacked_bytes = 0;
    sock->send_buffer->send_next_index = 0;
    sock->send_buffer->write_index = 0; // 从0开始存放数据

    sock->receive_window = (receive_window_t *)(malloc(sizeof(receive_window_t)));
    sock->receive_window->advertised_window = UINT16_MAX;
    sock->receive_window->next_expected_seq = sock->remote_isn + 1;
    sock->receive_window->last_advertised_window = UINT16_MAX;
    memset(sock->receive_buffer->received_bitmap, 0,
           (size_t)sock->receive_buffer->received_bitmap_size);
    sock->receive_buffer->read_index = 0;
    sock->receive_buffer->next_expected_index = 0;
    sock->receive_buffer->buffered_bytes = 0;
    sock->receive_buffer->reserved_bytes = 0;
    trace_rwnd(flow_advertised_window_locked(sock));
}

static void retransmit_oldest_segment_locked(tju_tcp_t* sock){
    uint32_t seq_num = sock->send_window->base_seq;
    uint32_t ack_num = sock->receive_window->next_expected_seq;
    uint32_t retransmit_bytes =
        sock->send_window->next_seq - sock->send_window->base_seq;
    uint32_t buffer_offset = (uint32_t)sock->send_buffer->ack_base_index;
    int buffer_capacity = sock->send_buffer->capacity;
    int packet_length;
    char* data;
    char* msg;

    if(retransmit_bytes == 0 || sock->send_buffer->unacked_bytes == 0){
        return;
    }
    if(retransmit_bytes > (uint32_t)sock->send_buffer->unacked_bytes){
        retransmit_bytes = (uint32_t)sock->send_buffer->unacked_bytes;
    }
    if(retransmit_bytes > SMSS){
        retransmit_bytes = SMSS;
    }

    rtt_mark_retransmitted(sock, seq_num, retransmit_bytes);

    data = (char*)malloc(retransmit_bytes);
    if(buffer_offset + retransmit_bytes <= (uint32_t)buffer_capacity){
        memcpy(data, sock->send_buffer->mem + buffer_offset, retransmit_bytes);
    }
    else{
        uint32_t first_part_bytes = (uint32_t)buffer_capacity - buffer_offset;
        uint32_t second_part_bytes = retransmit_bytes - first_part_bytes;
        memcpy(data, sock->send_buffer->mem + buffer_offset, first_part_bytes);
        memcpy(data + first_part_bytes, sock->send_buffer->mem, second_part_bytes);
    }

    packet_length = DEFAULT_HEADER_LEN + (int)retransmit_bytes;
    msg = create_packet_buf(sock->established_local_addr.port,
                            sock->established_remote_addr.port,
                            seq_num, ack_num, DEFAULT_HEADER_LEN, packet_length,
                            NO_FLAG, flow_advertised_window(sock), 0,
                            data, (int)retransmit_bytes);
    sendToLayer3(msg, packet_length);
    trace_send(seq_num, ack_num, NO_FLAG, retransmit_bytes);
    free(data);
    free(msg);
}
