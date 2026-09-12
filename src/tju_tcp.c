#include "tju_tcp.h"

volatile sig_atomic_t retry[RETRY_FLAG_NUM];  // 重传检测标志位
conn_queue* half_conn_queue[MAX_SOCK] = {NULL};  // 半连接队列
conn_queue* full_conn_queue[MAX_SOCK] = {NULL};  // 全连接队列

static int send_flags_with_window(tju_tcp_t* sock, uint8_t flags,
                                  uint32_t seq_num, uint32_t ack_num,
                                  uint16_t advertised_window);
static void resend_data_locked(tju_tcp_t* sock);

static uint16_t advertised_window_locked(tju_tcp_t* sock){
    int available = sock->recv_buf->capacity - sock->recv_buf->count;
    if(available < 0){
        available = 0;
    }
    return (available > UINT16_MAX)? (uint16_t)UINT16_MAX: (uint16_t)available;
}

static uint16_t advertised_window(tju_tcp_t* sock){
    pthread_mutex_lock(&(sock->recv_buf->lock));
    uint16_t window = advertised_window_locked(sock);
    pthread_mutex_unlock(&(sock->recv_buf->lock));
    return window;
}

static int send_flags(tju_tcp_t* sock, uint8_t flags, uint32_t seq_num,
                      uint32_t ack_num){
    return send_flags_with_window(sock, flags, seq_num, ack_num,
                                  advertised_window(sock));
}

/*
创建 TCP socket 
初始化对应的结构体
设置初始状态为 CLOSED
*/
tju_tcp_t* tju_socket(){
    tju_tcp_t* sock = (tju_tcp_t*)malloc(sizeof(tju_tcp_t));
    pthread_mutex_init(&(sock->state_lock), NULL);
    sock->state = CLOSED;
    
    sock->send_buf = (send_buf_t*)malloc(sizeof(send_buf_t));
    pthread_mutex_init(&(sock->send_buf->lock), NULL);
    pthread_cond_init(&(sock->send_buf->cond), NULL);
    sock->send_buf->mem = (char*)(malloc(SEND_BUF_SIZE * sizeof(char)));
    sock->send_buf->capacity = SEND_BUF_SIZE;
    sock->send_buf->base = 0;
    sock->send_buf->next = 0;
    sock->send_buf->store = 0;
    sock->send_buf->count = 0;

    sock->recv_buf = (recv_buf_t*)malloc(sizeof(recv_buf_t));
    pthread_mutex_init(&(sock->recv_buf->lock), NULL);
    pthread_cond_init(&(sock->recv_buf->cond), NULL);
    sock->recv_buf->mem = (char*)(malloc(RECV_BUF_SIZE * sizeof(char)));
    sock->recv_buf->capacity = RECV_BUF_SIZE;
    sock->recv_buf->head = 0;
    sock->recv_buf->expect = 0;
    sock->recv_buf->count = 0;

    return sock;
}

/*
绑定监听的地址 包括ip和端口
*/
int tju_bind(tju_tcp_t* sock, tju_sock_addr bind_addr){
    sock->bind_addr = bind_addr;
    return 0;
}

/*
被动打开 监听bind的地址和端口
设置socket的状态为LISTEN
注册该socket到内核的监听socket哈希表
*/
int tju_listen(tju_tcp_t* sock){
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
tju_tcp_t* tju_accept(tju_tcp_t* listen_sock){
    int hashval = cal_hash(listen_sock->bind_addr.ip, listen_sock->bind_addr.port, 0, 0);
    while(empty_full_conn_queue(hashval)){
        // 阻塞等待全连接队列不为空
        if(retry[SYNACK_RETRY] && !empty_half_conn_queue(hashval)){
            // 重发SYN|ACK
            pthread_mutex_lock(half_conn_queue[hashval]->lock);
            conn_node front_info = *(half_conn_queue[hashval]->front);
            pthread_mutex_unlock(half_conn_queue[hashval]->lock);
            send_flags(front_info.sock, (SYN_FLAG_MASK | ACK_FLAG_MASK),
                        front_info.sock->local_isn, front_info.sock->remote_isn + 1);

            // 重新设置 SYN + ACK 超时监测
            front_info.sock->send_wnd->timeout.tv_sec = 3;
            front_info.sock->send_wnd->timeout.tv_usec = 0;
            set_timeout(front_info.sock, SYNACK_RETRY);
        }
    }
    // 从全连接队列中出队
    conn_node* new_conn_node = pop_full_conn_queue(hashval);
    tju_tcp_t* new_conn = new_conn_node->sock;

    // 连接已建立，启动发送和重传线程
    pthread_t pid_send, pid_retry;
    int rst = pthread_create(&pid_send, NULL, send_thread, (void*)(new_conn));
    if (rst<0){
        printf("ERROR create send_thread!\n");
        exit(-1);
    }
    new_conn->send_thread_id = pid_send;
    rst = pthread_create(&pid_retry, NULL, retry_thread, (void*)(new_conn));
    if (rst<0){
        printf("ERROR create retry_thread!\n");
        exit(-1);
    }
    new_conn->retry_thread_id = pid_retry;

    return new_conn;
}


/*
连接到服务端
该函数以一个socket为参数
调用函数前, 该socket还未建立连接
函数正常返回后, 该socket一定是已经完成了3次握手, 建立了连接
因为只要该函数返回, 用户就可以马上使用该socket进行send和recv
*/
int tju_connect(tju_tcp_t* sock, tju_sock_addr target_addr){
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
    init_buf_wnd(sock);

    // 放入listen_socks哈希表，用于发送SYN后接收SYN+ACK
    int hashval = cal_hash(local_addr.ip, local_addr.port, 0, 0);
    listen_socks[hashval] = sock;
    
    // 发送 SYN
    send_flags(sock, SYN_FLAG_MASK, sock->local_isn, sock->remote_isn);
    pthread_mutex_lock(&sock->state_lock);
    sock->state = SYN_SENT;
    pthread_mutex_unlock(&sock->state_lock);

    // 设置 SYN 超时检测
    sock->send_wnd->timeout.tv_sec = 1;
    sock->send_wnd->timeout.tv_usec = 0;
    set_timeout(sock, SYN_RETRY);

    // 接收 SYN + ACK
    while (sock->state != ESTABLISHED){
        // 阻塞
        if(retry[SYN_RETRY]){
            // 将连接建立后的重传超时设置为3s
            sock->send_wnd->timeout.tv_sec = 3;
            sock->send_wnd->timeout.tv_usec = 0;

            // 重传 SYN
            send_flags(sock, SYN_FLAG_MASK, sock->local_isn, sock->remote_isn);
            sock->send_wnd->timeout.tv_sec = 3;
            sock->send_wnd->timeout.tv_usec = 0;
            set_timeout(sock, SYN_RETRY);
        }
    }

    // 连接已建立，启动发送和重传线程
    pthread_t pid_send, pid_retry;
    int rst = pthread_create(&pid_send, NULL, send_thread, (void*)(sock));
    if (rst<0){
        printf("ERROR create send_thread!\n");
        exit(-1);
    }
    sock->send_thread_id = pid_send;
    rst = pthread_create(&pid_retry, NULL, retry_thread, (void*)(sock));
    if (rst<0){
        printf("ERROR create retry_thread!\n");
        exit(-1);
    }
    sock->retry_thread_id = pid_retry;

    return 0;
}

// 采用阻塞式发送
int tju_send(tju_tcp_t* sock, const void *buffer, int len){
    if(len <= 0) return len;

    int buf_base, buf_next, buf_size, buf_store, buf_free;

    // 判断状态
    pthread_mutex_lock(&(sock->state_lock));
    if(sock->state != ESTABLISHED){
        printf("Now the connect is break! [SEND]\n");
        pthread_mutex_unlock(&(sock->state_lock));
        return 0;  // 不接受连接断开之后的发送请求
    }
    pthread_mutex_unlock(&(sock->state_lock));

    pthread_mutex_lock(&(sock->send_buf->lock));  // 与发送线程抢占缓冲区
    buf_size = sock->send_buf->capacity;
    if(buf_size < len) {
        printf("Send_buf doesn't have enough space!\n");
        pthread_mutex_unlock(&(sock->send_buf->lock));
        return -1;
    }
    pthread_mutex_unlock(&(sock->send_buf->lock));

    pthread_mutex_lock(&(sock->send_buf->lock));
    // pthread_cond_wait returns with the mutex reacquired.
    while(1){
        buf_base = sock->send_buf->base;
        buf_next = sock->send_buf->next;
        buf_store = sock->send_buf->store;
        buf_free = buf_size - sock->send_buf->count;
        if(buf_free >= len){
            break;
        }
        pthread_cond_wait(&(sock->send_buf->cond), &(sock->send_buf->lock));
    }

    // 处理环形缓冲区
    if(buf_store + len <= buf_size){
        memcpy(sock->send_buf->mem + buf_store, buffer, len);
    }
    else{
        int len_1 = buf_size - buf_store;
        int len_2 = len - len_1;
        memcpy(sock->send_buf->mem + buf_store, buffer, len_1);
        memcpy(sock->send_buf->mem, buffer + len_1, len_2);
    }

    sock->send_buf->store = (buf_store + len) % buf_size;
    sock->send_buf->count += len;
    pthread_cond_broadcast(&(sock->send_buf->cond));

    pthread_mutex_unlock(&(sock->send_buf->lock));
    return len;
}

// 采用阻塞式接收
int tju_recv(tju_tcp_t* sock, void *buffer, int len){
    int head_index, buf_size, read_len;
    int send_window_update = 0;
    uint16_t old_advertised, new_advertised;
    uint32_t update_seq, update_ack;
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
    pthread_mutex_lock(&(sock->recv_buf->lock));
    // pthread_cond_wait returns with the mutex reacquired.
    while(sock->recv_buf->count == 0){
        pthread_cond_wait(&(sock->recv_buf->cond), &(sock->recv_buf->lock));
    }
    head_index = sock->recv_buf->head;
    buf_size = sock->recv_buf->capacity;
    read_len = (sock->recv_buf->count < len)? sock->recv_buf->count: len;
    old_advertised = advertised_window_locked(sock);

    if(head_index + read_len <= buf_size){
        memcpy(dst, sock->recv_buf->mem + head_index, read_len);
    }
    else{
        int len1 = buf_size - head_index;
        int len2 = read_len - len1;
        memcpy(dst, sock->recv_buf->mem + head_index, len1);
        memcpy(dst + len1, sock->recv_buf->mem, len2);
    }
    // 更新 head
    sock->recv_buf->head = (sock->recv_buf->head + read_len) % buf_size;
    sock->recv_buf->count -= read_len;
    pthread_cond_broadcast(&(sock->recv_buf->cond));
    new_advertised = advertised_window_locked(sock);
    if(old_advertised < UINT16_MAX / 2 && new_advertised >= UINT16_MAX / 2){
        send_window_update = 1;
        update_seq = sock->send_wnd->next_seq;
        update_ack = sock->recv_wnd->expect_seq;
    }

    pthread_mutex_unlock(&(sock->recv_buf->lock)); // 解锁

    if(send_window_update){
        send_flags(sock, ACK_FLAG_MASK, update_seq, update_ack);
    }

    return read_len;
}

// 把内核收到的数据放到sock->receive_buf，内核可使用该函数传递内核缓冲区收到的packet
int tju_handle_packet(tju_tcp_t* sock, char* pkt){
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

    pthread_mutex_lock(&sock->state_lock);
    int st = sock->state;

    /*##################################### 连接管理 ###########################################*/
    // Receiver: [server], Flags: [SYN]
    if(st == LISTEN && data_len == 0 && flags == SYN_FLAG_MASK){
        pthread_mutex_unlock(&sock->state_lock);

        // 新建socket: new_conn
        tju_tcp_t* new_conn = (tju_tcp_t*)malloc(sizeof(tju_tcp_t));
        memcpy(new_conn, sock, sizeof(tju_tcp_t));
        new_conn->established_local_addr.port = local_addr.port; // 随机分配端口
        new_conn->established_local_addr.ip = local_addr.ip;
        new_conn->established_remote_addr = remote_addr;

        // 设置 server_isn
        struct timespec sp;
        clock_gettime(CLOCK_MONOTONIC, &sp);
        uint32_t server_isn = sp.tv_sec;
        uint32_t client_isn = seq_num;

        // 初始化 window -> 用于用户线程重传 SYNACK
        new_conn->local_isn = server_isn;
        new_conn->remote_isn = seq_num;
        init_buf_wnd(new_conn);

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
        send_flags(new_conn, (SYN_FLAG_MASK | ACK_FLAG_MASK), new_conn->local_isn, new_conn->remote_isn + 1);

        // 设置状态
        new_conn->state = SYN_RECV;
        
        // 设置 SYN + ACK 超时检测
        new_conn->send_wnd->timeout.tv_sec = 1;
        new_conn->send_wnd->timeout.tv_usec = 0;
        set_timeout(new_conn, SYNACK_RETRY);

        return 0;
    }
    // Receiver: [client], Flags: [SYN + ACK]
    else if(st == SYN_SENT && data_len == 0 && flags == (SYN_FLAG_MASK | ACK_FLAG_MASK)){
        pthread_mutex_unlock(&sock->state_lock);

        // 取消 SYN 超时检测
        stop_timeout(SYN_RETRY);

        // 更换连接的远程地址为new_conn，而不是bind_addr
        sock->established_remote_addr = remote_addr;

        // 设置 window
        sock->remote_isn = seq_num;
        sock->recv_wnd->expect_seq = seq_num + 1;  // 更新
        sock->send_wnd->window_size = get_advertised_window(pkt);

        // 发送ACK
        send_flags(sock, ACK_FLAG_MASK, sock->local_isn + 1, sock->remote_isn + 1);
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
    else if(st == ESTABLISHED && data_len == 0 && flags == (SYN_FLAG_MASK | ACK_FLAG_MASK)){
        pthread_mutex_unlock(&sock->state_lock);

        // 对SYNACK重复响应
        send_flags(sock, ACK_FLAG_MASK, sock->local_isn + 1, sock->remote_isn + 1);

        return 0;
    }
    // Receiver: [server], Flags: [ACK]
    else if(st == SYN_RECV && data_len == 0 && flags == ACK_FLAG_MASK){
        pthread_mutex_unlock(&sock->state_lock);

        // 取消 SYN + ACK 超时检测
        stop_timeout(SYNACK_RETRY);

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
        new_conn->send_wnd->window_size = get_advertised_window(pkt);

        return 0;
    /* ################################# 三次握手状态机结束 #######################################*/
    }
    /* ################################# 四次挥手状态机开始 #######################################*/
    // Receiver: [Passive], Flags: [ACK | FIN]
    else if((st == ESTABLISHED) && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){
        // 设置 window
        sock->local_fin_seq = ack_num;
        sock->remote_fin_seq = seq_num;

        send_flags(sock, (ACK_FLAG_MASK), sock->local_fin_seq, sock->remote_fin_seq + 1);

        sock->state = CLOSE_WAIT;
        pthread_mutex_unlock(&sock->state_lock);
        return 0;
    }
    // Receiver: [Active], Flags: [ACK]
    else if((st == FIN_WAIT_1) && data_len == 0 && flags == (ACK_FLAG_MASK)){
        if(ack_num != sock->local_fin_seq + 1){  // 校验，有可能是对发送数据的ACK
            pthread_mutex_unlock(&(sock->state_lock));
            return 0;
        }
        
        stop_timeout(FINACK_RETRY);
        
        // 状态转移->FIN_WAIT_2
        sock->state = FIN_WAIT_2;

        pthread_mutex_unlock(&sock->state_lock);

        return 0;
    }
    // Receiver: [Active], Flags: [ACK | FIN]
    else if((st == FIN_WAIT_2) && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){

        // 取消FINACK超时监控
        stop_timeout(FINACK_RETRY);

        // 发送ACK
        send_flags(sock, (ACK_FLAG_MASK), sock->local_fin_seq + 1, sock->remote_fin_seq + 1);

        sock->state = TIME_WAIT;

        pthread_mutex_unlock(&sock->state_lock);

        return 0;
    }
    // Receiver: [Passive], Flags: [ACK]
    else if((st == LAST_ACK) && data_len == 0 && flags == ACK_FLAG_MASK){

        // 最终 ACK 到达，被动关闭结束，停掉 FINACK 重传计时后再释放连接。
        stop_timeout(FINACK_RETRY);

        sock->state = CLOSED;
        pthread_mutex_unlock(&sock->state_lock);
        return 0;
    }
    // Receiver: [Active], Flags: [FINACK]   /* TIME_WAIT 期间收到重复 FINACK */
    else if((st == TIME_WAIT) && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){
        pthread_mutex_unlock(&sock->state_lock);

        // 最终 ACK 可能已经丢失，对端仍停留在 LAST_ACK 并重传 FINACK。
        // 本端在 TIME_WAIT 中应再次发送 ACK，否则对端会一直重传。
        send_flags(sock, (ACK_FLAG_MASK), sock->local_fin_seq + 1, sock->remote_fin_seq + 1);

        return 0;
    }
    /*################################### 同时关闭连接 #######################################*/
    // Receiver: [peer], Flags: [FINACK]
    else if((st == FIN_WAIT_1) && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){
        sock->remote_fin_seq = seq_num;

        if(ack_num == sock->local_fin_seq + 1){
            /* 对端的FIN|ACK已同时确认了我方的FIN（被动方发FIN时ack=remote_fin_seq+1），
             * 一次交互即完成同时关闭：回最终ACK后直接进入TIME_WAIT。
             * 若仍走CLOSING，将等待一个永远不会到来的独立ACK而阻塞。 */
            send_flags(sock, (ACK_FLAG_MASK), sock->local_fin_seq + 1, sock->remote_fin_seq + 1);
            sock->state = TIME_WAIT;
        }
        else{
            /* 对端只确认了数据，我方FIN尚未被确认：进入CLOSING等待独立ACK */
            send_flags(sock, (ACK_FLAG_MASK), sock->local_fin_seq + 1, sock->remote_fin_seq + 1);
            sock->state = CLOSING;
        }

        pthread_mutex_unlock(&sock->state_lock);

        return 0;
    }
    // Receiver: [peer], Flags: [ACK]
    else if((st == CLOSING) && data_len == 0 && flags == ACK_FLAG_MASK){

        // 取消 FINACK 计时
        stop_timeout(FINACK_RETRY);

        sock->state = TIME_WAIT;
        pthread_mutex_unlock(&sock->state_lock);

        return 0;
    /*################################### 连接管理结束 #######################################*/
    }
    /*################################# 数据发送端处理ACK ####################################*/
    else if((st == ESTABLISHED || st == CLOSE_WAIT) && data_len == 0 && flags == ACK_FLAG_MASK){
        pthread_mutex_unlock(&sock->state_lock);
        
        pthread_mutex_lock(&(sock->send_buf->lock));
        uint32_t base_seq = sock->send_wnd->base_seq;
        uint32_t next_seq = sock->send_wnd->next_seq;
        int buf_size = sock->send_buf->capacity;
        int ack_quantity = 0;
        sock->send_wnd->window_size = get_advertised_window(pkt);
        /* 校验是否为正确范围内ACK */
        if(ack_num > base_seq && ack_num <= next_seq){
            ack_quantity = ack_num - base_seq;
        }
        else if(next_seq < base_seq && ack_num <= next_seq){
            ack_quantity = (SEQ_MAX - base_seq) + ack_num;
        }

        // 更新 base, base_seq
        if(ack_quantity > 0){
            sock->send_wnd->ack_cnt = 0;
            sock->send_buf->base = (sock->send_buf->base + ack_quantity) % buf_size;
            sock->send_buf->count -= ack_quantity;
            pthread_cond_broadcast(&(sock->send_buf->cond));
            sock->send_wnd->base_seq = ack_num;  // 累计确认
            if(sock->send_wnd->base_seq == sock->send_wnd->next_seq){
                stop_timeout(DATA_RETRY);  // 说明当前这个把 no_ack 的包全部给确认了
            }
            else{
                /* 仍有未ACK数据：以最后一个ACK时刻重置超时，
                 * 避免RTO从突发起点计算导致大RTT下整窗误重传 */
                sock->send_wnd->timeout.tv_sec = 3;
                sock->send_wnd->timeout.tv_usec = 0;
                set_timeout(sock, DATA_RETRY);
            }
            // 计算RTO
            
        }
        else if(ack_num == base_seq){
            if(base_seq != next_seq){
                sock->send_wnd->ack_cnt++;
            }
            if(sock->send_wnd->ack_cnt >= 3){
                // 快速重传
                resend_data_locked(sock);
                set_timeout(sock, DATA_RETRY);
                sock->send_wnd->ack_cnt = 0;
            }
        }

        pthread_mutex_unlock(&(sock->send_buf->lock));

        return 0;
    }
    pthread_mutex_unlock(&sock->state_lock);
    /*#################################### 接收真实数据 #####################################*/

    // 返回值非0，说明需要加锁失败，要一直尝试加锁
    while(pthread_mutex_lock(&(sock->recv_buf->lock)) != 0); // 加锁

    // 缓冲区仅仅复制接收窗口大小的数据
    uint32_t buf_size = sock->recv_buf->capacity;
    int buf_expect = sock->recv_buf->expect;
    uint32_t expect_num = sock->recv_wnd->expect_seq;
    uint32_t available = buf_size - sock->recv_buf->count;
    uint16_t adv_window = advertised_window_locked(sock);
    uint32_t putlen = data_len;

    if(putlen == 0){
        pthread_mutex_unlock(&(sock->recv_buf->lock)); // 解锁
        return 0;
    }
    
    if(seq_num != expect_num){  // 乱序或者丢包
        pthread_mutex_unlock(&(sock->recv_buf->lock)); // 解锁
        // 发送Duplicate-SACK
        send_flags_with_window(sock, ACK_FLAG_MASK, sock->send_wnd->next_seq,
                               expect_num, adv_window);
        return 0;
    }

    if(putlen > available){
        pthread_mutex_unlock(&(sock->recv_buf->lock));
        send_flags_with_window(sock, ACK_FLAG_MASK, sock->send_wnd->next_seq,
                               expect_num, adv_window);
        return 0;
    }

    if(buf_expect + putlen <= buf_size){
        memcpy(sock->recv_buf->mem + buf_expect, pkt + DEFAULT_HEADER_LEN, putlen);
    }
    else{
        int len1 = buf_size - buf_expect;
        int len2 = putlen - len1;
        memcpy(sock->recv_buf->mem + buf_expect, pkt + DEFAULT_HEADER_LEN, len1);
        memcpy(sock->recv_buf->mem, pkt + DEFAULT_HEADER_LEN + len1, len2);
    }

    // 发送 ACK seq
    sock->recv_wnd->expect_seq = seq_num + putlen;
    sock->recv_buf->expect = (buf_expect + putlen) % buf_size;  // 将来接收数据在缓冲区中的下标
    sock->recv_buf->count += putlen;
    pthread_cond_broadcast(&(sock->recv_buf->cond));
    adv_window = advertised_window_locked(sock);
    uint32_t ack_to_send = sock->recv_wnd->expect_seq;
    // 注意：此处不能 stop_timeout(DATA_RETRY) —— 全局itimer被所有通道共用，
    // 双向传输时收到数据会误杀本端发送方向的重传定时器
    pthread_mutex_unlock(&(sock->recv_buf->lock)); // 解锁
    send_flags_with_window(sock, ACK_FLAG_MASK, sock->send_wnd->next_seq,
                           ack_to_send, adv_window);
    return 0;
}

int tju_close (tju_tcp_t* sock){
    uint32_t seq_num, ack_num;
    
    pthread_mutex_lock(&(sock->state_lock));  // 与后台线程抢锁
    int active_close = (sock->state == ESTABLISHED);  // 确定主动被动
    if(active_close){
    }
    else{
    }

    if(active_close){
        pthread_mutex_unlock(&(sock->state_lock));
        // 此时还取着状态锁
        pthread_mutex_lock(&(sock->send_buf->lock));
        while((uint32_t)(sock->send_wnd->next_seq - sock->send_wnd->base_seq) <
              (uint32_t)sock->send_buf->count){
            pthread_cond_wait(&(sock->send_buf->cond), &(sock->send_buf->lock));
        }
        pthread_mutex_unlock(&(sock->send_buf->lock));

        /* 关键：等待数据刷出期间，对端的FIN可能已被收包线程处理(state->CLOSE_WAIT)。
         * 若不复查就按主动关闭继续发FIN，两个关闭流程会互相踩踏：
         * 对端FIN已在ESTABLISHED被被动分支消费并回了ACK，本端却又置FIN_WAIT_1，
         * 最终停在FIN_WAIT_2等待一个永远不会到来的FIN，close永久阻塞。
         * 复查到CLOSE_WAIT说明本端实为被动关闭，切换到被动路径。 */
        pthread_mutex_lock(&(sock->state_lock));
        if(sock->state == CLOSE_WAIT){
            active_close = 0;  // 保持持锁状态，交给下方被动路径开头的unlock
        }
        else{
            /* 决策、发FIN、置FIN_WAIT_1必须在同一把state_lock内原子完成，
             * 保证收包线程不可能在此间隙把对端FIN按被动分支消费掉 */
            seq_num = sock->send_wnd->next_seq;
            ack_num = sock->recv_wnd->expect_seq;
            sock->local_fin_seq = seq_num;
            sock->remote_fin_seq = ack_num;

            send_flags(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK), seq_num, ack_num);
            sock->state = FIN_WAIT_1;
            pthread_mutex_unlock(&(sock->state_lock));  // 必须更新状态后再放锁
            sock->send_wnd->timeout.tv_sec = 3;
            sock->send_wnd->timeout.tv_usec = 0;
            set_timeout(sock, FINACK_RETRY);

            pthread_mutex_lock(&(sock->send_buf->lock));
            pthread_cond_broadcast(&(sock->send_buf->cond));
            pthread_mutex_unlock(&(sock->send_buf->lock));
        }
    }

    if(active_close){
        while(1){
            pthread_mutex_lock(&(sock->state_lock));
            if(sock->state == TIME_WAIT){
                pthread_mutex_unlock(&(sock->state_lock));
                break;
            }
            if(retry[FINACK_RETRY]){
                // 重传 FINACK
                send_flags(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK), sock->local_fin_seq, sock->remote_fin_seq);
                sock->send_wnd->timeout.tv_sec = 3;
                sock->send_wnd->timeout.tv_usec = 0;
                set_timeout(sock, FINACK_RETRY);
                if(sock->state != CLOSING){
                    // 主动关闭
                }
                else{
                    // 同时关闭
                }
            }
            pthread_mutex_unlock(&(sock->state_lock));
        }
        // 主动方进入 TIME_WAIT 后先清掉关闭相关计时器，再等待 2 * MSL
        stop_timeout(FINACK_RETRY);
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
        pthread_mutex_lock(&(sock->recv_buf->lock));
        while(sock->recv_buf->count > 0){
            pthread_cond_wait(&(sock->recv_buf->cond), &(sock->recv_buf->lock));
        }
        pthread_mutex_unlock(&(sock->recv_buf->lock));

        send_flags(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK), sock->local_fin_seq, sock->remote_fin_seq + 1);
        sock->send_wnd->timeout.tv_sec = 3;
        sock->send_wnd->timeout.tv_usec = 0;
        set_timeout(sock, FINACK_RETRY);

        pthread_mutex_lock(&(sock->state_lock));
        sock->state = LAST_ACK;
        pthread_mutex_unlock(&(sock->state_lock));
        pthread_mutex_lock(&(sock->send_buf->lock));
        pthread_cond_broadcast(&(sock->send_buf->cond));
        pthread_mutex_unlock(&(sock->send_buf->lock));

        while(1){
            pthread_mutex_lock(&(sock->state_lock));
            if(sock->state == CLOSED){
                pthread_mutex_unlock(&(sock->state_lock));
                break;
            }
            if(retry[FINACK_RETRY]){
                // 被动方已发送 FINACK，仍未收到最终 ACK 时重传 FINACK。
                send_flags(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK), sock->local_fin_seq, sock->remote_fin_seq + 1);
                sock->send_wnd->timeout.tv_sec = 3;
                sock->send_wnd->timeout.tv_usec = 0;
                set_timeout(sock, FINACK_RETRY);
            }
            pthread_mutex_unlock(&(sock->state_lock));
        }
        stop_timeout(FINACK_RETRY);
    }
    /* 回收线程 */
    if(sock->send_thread_id != 0){
        pthread_join(sock->send_thread_id, NULL);
    }
    if(sock->retry_thread_id != 0){
        pthread_join(sock->retry_thread_id, NULL);
    }

    return 0;
}

// 发送线程，前置条件是必须连接建立后调用
static void* send_thread(void* arg){
    tju_tcp_t* sock = (tju_tcp_t*)(arg);
    uint32_t seq_num, ack_num;
    int win_size, noack_size, nosent_size, total_size, send_size, plen;

    int buf_base, buf_next, buf_size, buf_store, buf_free;
    char* data, *msg;

    uint32_t cur_seq, buf_off;
    int remain;

    while(1){
        // 确认状态
        pthread_mutex_lock(&(sock->state_lock));
        if(sock->state != ESTABLISHED && sock->state != CLOSE_WAIT){
            pthread_mutex_unlock(&(sock->state_lock));
            break;
        }
        pthread_mutex_unlock(&(sock->state_lock));

        /* 占用缓冲区 */
        pthread_mutex_lock(&(sock->send_buf->lock));
        ack_num = sock->recv_wnd->expect_seq;
        seq_num = sock->send_wnd->next_seq;
        win_size = sock->send_wnd->window_size;
        buf_base = sock->send_buf->base;
        buf_next = sock->send_buf->next;
        buf_store = sock->send_buf->store;
        buf_size = sock->send_buf->capacity;
        noack_size = (buf_next - buf_base + buf_size) % buf_size;  // 发送但是没有ACK的数据大小
        nosent_size =  (buf_store - buf_next + buf_size) % buf_size;  // 缓冲区中所有没有发送的数据大小
        total_size = ((win_size - noack_size) < nosent_size)? (win_size - noack_size): nosent_size;  // 窗口约束

        /* 将窗口内的包都发出去 */
        noack_size = (uint32_t)(seq_num - sock->send_wnd->base_seq);
        if(noack_size > sock->send_buf->count){
            noack_size = sock->send_buf->count;
        }
        nosent_size = sock->send_buf->count - noack_size;
        total_size = ((win_size - noack_size) < nosent_size)?
                     (win_size - noack_size): nosent_size;
        if(total_size < 0){
            total_size = 0;
        }
        remain = total_size;
        cur_seq = seq_num;
        buf_off = buf_next;

        while(remain > 0){
            /* SMSS约束 */
            send_size = (remain < SMSS)? remain: SMSS;

            /* 从缓冲区中取数据 */
            if(send_size != 0 && buf_off + send_size <= buf_size){
                data = (char*)malloc(send_size * sizeof(char));
                memcpy(data, sock->send_buf->mem + buf_off, send_size);
            }
            else{
                int len1 = buf_size - buf_off;
                int len2 = send_size - len1;
                data = (char*)malloc(send_size * sizeof(char));
                memcpy(data, sock->send_buf->mem + buf_off, len1);
                memcpy(data + len1, sock->send_buf->mem, len2);
            }

            /* 发包 */
            plen = DEFAULT_HEADER_LEN + send_size;
            msg = create_packet_buf(sock->established_local_addr.port,
                    sock->established_remote_addr.port, cur_seq, ack_num,
                    DEFAULT_HEADER_LEN, plen,
                    NO_FLAG, 0, 0, data, send_size);
            sendToLayer3(msg, plen);
            free(data);

            free(msg);

            /* 更新指针 */
            cur_seq = (uint32_t)(cur_seq + send_size);
            buf_off = (buf_off + send_size) % buf_size;
            remain -= send_size;
        }
        /* 仅在新突发的第一包时启动数据重传监控：
         * base==next 说明当前无任何已发送未ACK的数据，本次发送是新突发的起点。
         * 此前每轮无条件 set_timeout 会把全局itimer不断重置，SIGALRM永远无法触发，
         * 导致超时重传成为死代码（rdt测试尾部丢包即卡死）。 */
        if(total_size > 0 && sock->send_wnd->base_seq == seq_num){
            sock->send_wnd->timeout.tv_sec = 3;
            sock->send_wnd->timeout.tv_usec = 0;
            set_timeout(sock, DATA_RETRY);
        }
        else if(total_size == 0){
            pthread_cond_wait(&(sock->send_buf->cond), &(sock->send_buf->lock));
        }

        /* 更新窗口 */
        sock->send_wnd->next_seq = cur_seq;
        sock->send_buf->next = buf_off;
        pthread_cond_broadcast(&(sock->send_buf->cond));

        /* 解锁 */
        pthread_mutex_unlock(&(sock->send_buf->lock));
    }

    return NULL;
}

// 重传线程
static void* retry_thread(void* arg){
    tju_tcp_t* sock = (tju_tcp_t*)(arg);
    uint32_t seq_num, ack_num;
    int win_size, noack_size, nosent_size, total_size, send_size, plen;

    int buf_base, buf_next, buf_size, buf_store, buf_free;
    char* data, *msg;

    uint32_t cur_seq, buf_off;
    int remain;

    while(1){
        // 确认状态是否为连接中
        pthread_mutex_lock(&(sock->state_lock));
        if(sock->state != ESTABLISHED){
            pthread_mutex_unlock(&(sock->state_lock));
            break;
        }
        pthread_mutex_unlock(&(sock->state_lock));

        // 判断当前是否数据超时
        if(retry[DATA_RETRY]){  // 重发 no_ack 数据 [base, next)
            pthread_mutex_lock(&(sock->send_buf->lock));
            int has_outstanding =
                (sock->send_wnd->base_seq != sock->send_wnd->next_seq);
            if(has_outstanding){
                resend_data_locked(sock);
            }
            if(has_outstanding){
                set_timeout(sock, DATA_RETRY);  // 仍有未确认数据才重设超时
            }
            else{
                stop_timeout(DATA_RETRY);  // 清除迟到的 SIGALRM，避免空窗口重传
            }
            pthread_mutex_unlock(&(sock->send_buf->lock));
        }
        usleep(100);  // 避免纯自旋抢占CPU
    }
    return NULL;
}

/*
* ################################# 辅助函数,用户不应该调用 #################################
*/
static int send_flags_with_window(tju_tcp_t* sock, uint8_t flags,
                                  uint32_t seq_num, uint32_t ack_num,
                                  uint16_t advertised_window){
    char* msg;
    uint16_t plen = DEFAULT_HEADER_LEN;  // 无 Payload

    tju_sock_addr local_addr = sock->established_local_addr;
    tju_sock_addr remote_addr = sock->established_remote_addr;

    msg = create_packet_buf(local_addr.port, remote_addr.port, seq_num, ack_num, 
              DEFAULT_HEADER_LEN, plen, flags, advertised_window, 0, NULL, 0);
    sendToLayer3(msg, plen);
    free(msg);

    return 0;
}

// 释放sock对应的连接资源
static void free_sock(tju_tcp_t* sock){
    int hashval = cal_hash(sock->established_local_addr.ip, sock->established_local_addr.port,
                           sock->established_remote_addr.ip, sock->established_remote_addr.port);
    established_socks[hashval] = NULL;

    free(sock->send_buf->mem);
    free(sock->send_wnd);
    pthread_cond_destroy(&(sock->send_buf->cond));
    pthread_mutex_destroy(&(sock->send_buf->lock));

    free(sock->recv_buf->mem);
    free(sock->recv_wnd);
    pthread_cond_destroy(&(sock->recv_buf->cond));
    pthread_mutex_destroy(&(sock->recv_buf->lock));

    pthread_mutex_destroy(&(sock->state_lock));

    free(sock);
    
    destroy_half_conn_queue(hashval);
    destroy_full_conn_queue(hashval);
}

// 初始化缓冲区窗口
static void init_buf_wnd(tju_tcp_t* sock){
    sock->send_wnd = (sender_window_t *)(malloc(sizeof(sender_window_t)));
    sock->send_wnd->window_size = UINT16_MAX;
    sock->send_wnd->base_seq = sock->local_isn + 1;
    sock->send_wnd->ack_cnt = 0;
    sock->send_buf->base = 0;  // 没发出任何数据
    sock->send_wnd->next_seq = sock->local_isn + 1;
    sock->send_buf->count = 0;
    sock->send_buf->next = 0;
    sock->send_buf->store = 0; // 从0开始存放数据

    sock->recv_wnd = (receiver_window_t *)(malloc(sizeof(receiver_window_t)));
    sock->recv_wnd->window_size = UINT16_MAX;
    sock->recv_wnd->expect_seq = sock->remote_isn + 1;
    sock->recv_buf->expect = 0;
    sock->recv_buf->head = 0;
    sock->recv_buf->count = 0;
}

/*
* #################################### 队列函数 ############################################
*/
/*
################################### 全连接队列辅助函数 #######################################
*/
// 初始化队列
int init_full_conn_queue(int listen_hashval){
    if(listen_hashval < 0){
        printf("[init_full_conn_queue]: listen_hashval is %d < 0 error!\n", listen_hashval);
        exit(1);
    }
    // printf("start full init.\n");
    full_conn_queue[listen_hashval] = (conn_queue*)(malloc(sizeof(conn_queue)));

    full_conn_queue[listen_hashval]->lock = (pthread_mutex_t*)(malloc(sizeof(pthread_mutex_t)));
    full_conn_queue[listen_hashval]->front = NULL;
    full_conn_queue[listen_hashval]->rear = NULL;

    // printf("before full lock.\n");
    pthread_mutex_init(full_conn_queue[listen_hashval]->lock, NULL);
    
    // printf("finish full init.\n");
    return 0;
}

// 创建节点
conn_node* create_conn_node(tju_tcp_t *new_conn){
    conn_node* new_conn_node = (conn_node*)(malloc(sizeof(conn_node)));
    new_conn_node->sock = new_conn;
    new_conn_node->next = NULL;
    return new_conn_node;
}

// 入队
int push_full_conn_queue(int listen_hashval, conn_node* new_conn_node){
    if(listen_hashval < 0){
        printf("[push_conn_queue]: listen_hashval < 0 error!\n");
        exit(1);
    }
    conn_node* queue_front = full_conn_queue[listen_hashval]->front;
    conn_node* queue_rear = full_conn_queue[listen_hashval]->rear;
    if(queue_front == NULL && queue_rear == NULL){
        full_conn_queue[listen_hashval]->front = new_conn_node;
        full_conn_queue[listen_hashval]->rear = new_conn_node;
    }
    else{
        queue_rear->next = new_conn_node;
        full_conn_queue[listen_hashval]->rear = new_conn_node;
    }
    return 0;
}

// 出队
conn_node* pop_full_conn_queue(int listen_hashval){
    if(listen_hashval < 0){
        printf("[pop_conn_queue]: listen_hashval < 0 error!\n");
        exit(1);
    }
    conn_node* queue_front = full_conn_queue[listen_hashval]->front;
    conn_node* queue_rear = full_conn_queue[listen_hashval]->rear;    
    conn_node* pop_conn_node = queue_front;
    if(queue_front == NULL && queue_rear == NULL){
        printf("[pop_conn_queue]: no node but need pop a node, error!\n");
        exit(1);
    }
    else if(queue_front == queue_rear){
        full_conn_queue[listen_hashval]->front = NULL;
        full_conn_queue[listen_hashval]->rear = NULL;
    }
    else{
        full_conn_queue[listen_hashval]->front = queue_front->next;
    }
    return pop_conn_node;
}

// 销毁队列
int destroy_full_conn_queue(int listen_hashval){
    if(listen_hashval < 0){
        printf("[destroy_conn_queue]: listen_hashval < 0 error!\n");
        exit(1);
    }
    conn_node* it = full_conn_queue[listen_hashval]->front;
    conn_node* killed;
    while(it != NULL){
        killed = it;
        it = it->next;
        free(killed);
    }
    free(full_conn_queue[listen_hashval]);
    return 0;
}

// 判断非空
int empty_full_conn_queue(int listen_hashval){
    if(listen_hashval < 0){
        printf("[empty_conn_queue]: listen_hashval < 0 error!\n");
        exit(1);
    }
    conn_node* head = full_conn_queue[listen_hashval]->front;
    return head ==  NULL;
}
/*
################################## 半连接队列处理函数 #######################################
*/
// 初始化队列
int init_half_conn_queue(int listen_hashval){
    if(listen_hashval < 0){
        printf("[init_conn_queue]: listen_hashval < 0 error!\n");
        exit(1);
    }
    half_conn_queue[listen_hashval] = (conn_queue*)(malloc(sizeof(conn_queue)));
    half_conn_queue[listen_hashval]->lock = (pthread_mutex_t*)(malloc(sizeof(pthread_mutex_t)));
    half_conn_queue[listen_hashval]->front =  NULL;
    half_conn_queue[listen_hashval]->rear = NULL;
    pthread_mutex_init(half_conn_queue[listen_hashval]->lock, NULL);
    return 0;
}

// 入队
int push_half_conn_queue(int listen_hashval, conn_node* new_conn_node){
    if(listen_hashval < 0){
        printf("[push_conn_queue]: listen_hashval < 0 error!\n");
        exit(1);
    }
    conn_node* queue_front = half_conn_queue[listen_hashval]->front;
    conn_node* queue_rear = half_conn_queue[listen_hashval]->rear;
    if(queue_front == NULL && queue_rear == NULL){
        half_conn_queue[listen_hashval]->front = new_conn_node;
        half_conn_queue[listen_hashval]->rear = new_conn_node;
    }
    else{
        queue_rear->next = new_conn_node;
        half_conn_queue[listen_hashval]->rear = new_conn_node;
    }
    return 0;
}

// 出队
conn_node* pop_half_conn_queue(int listen_hashval){
    if(listen_hashval < 0){
        printf("[pop_conn_queue]: listen_hashval < 0 error!\n");
        exit(1);
    }
    
    conn_node* queue_front = half_conn_queue[listen_hashval]->front;
    conn_node* queue_rear = half_conn_queue[listen_hashval]->rear;    
    conn_node* pop_conn_node = queue_front;
    if(queue_front == NULL && queue_rear == NULL){
        printf("[pop_conn_queue]: no node but need pop a node, error!\n");
        exit(1);
    }
    else if(queue_front == queue_rear){
        half_conn_queue[listen_hashval]->front = NULL;
        half_conn_queue[listen_hashval]->rear = NULL;
    }
    else{
        half_conn_queue[listen_hashval]->front = queue_front->next;
    }
    return pop_conn_node;
}

// 销毁队列
int destroy_half_conn_queue(int listen_hashval){
    if(listen_hashval < 0){
        printf("[destroy_conn_queue]: listen_hashval < 0 error!\n");
        exit(1);
    }
    conn_node* it = half_conn_queue[listen_hashval]->front;
    conn_node* killed;
    while(it != NULL){
        killed = it;
        it = it->next;
        free(killed);
    }
    free(half_conn_queue[listen_hashval]);
    return 0;
}

// 判断非空
int empty_half_conn_queue(int listen_hashval){
    if(listen_hashval < 0){
        printf("[empty_conn_queue]: listen_hashval < 0 error!\n");
        exit(1);
    }
    conn_node* head = half_conn_queue[listen_hashval]->front;
    return head ==  NULL;
}

// 移除指定节点
conn_node* remove_half_conn_queue(int listen_hashval, int conn_hashval){
    if(listen_hashval < 0){
        printf("[remove_half_conn_queue]: listen_hashval < 0 error!\n");
        exit(1);
    }
    conn_node* queue_front = half_conn_queue[listen_hashval]->front;
    conn_node* queue_rear = half_conn_queue[listen_hashval]->rear;
    conn_node* it = half_conn_queue[listen_hashval]->front;
    conn_node* it_prev = NULL;
    int hashval;
    if(empty_half_conn_queue(listen_hashval)){
        printf("[remove_half_conn_queue]: queue no any node but will remove.\n");
        exit(1);
    }
    while(it != NULL){
        hashval = cal_hash(it->sock->established_local_addr.port, it->sock->established_local_addr.ip, 
                    it->sock->established_remote_addr.port, it->sock->established_remote_addr.ip);
        // printf("node_hash: %d, conn_hash: %d.\n", hashval, conn_hashval);
        if(hashval == conn_hashval){
            /*
            ###################如果是队头就需要设置新的队头####################
            */
            if(queue_front == it){
                // 对头
                half_conn_queue[listen_hashval]->front = queue_front->next;
            }
            else{
                it_prev->next = it->next;
            }
            /*
            ##################如果是队尾需要设置新的队尾部#####################
            */
            if(queue_rear == it){
                // 队尾
                half_conn_queue[listen_hashval]->rear = it_prev;
            }
            it->next = NULL;
            return it;
        }
        it_prev = it;
        it = it->next;
    }
    return NULL;  // 未找到
}

/*
* ############################### 重传函数 #######################################
*/
static void (*timeout_check[RETRY_FLAG_NUM])(int) = {
    NULL, 
    syn_timeout_check,
    synack_timeout_check,
    fin_timeout_check,
    finack_timeout_check,
    data_timeout_check,
};

void set_timeout(tju_tcp_t* sock, int re_num){
    signal(SIGALRM, timeout_check[re_num]);
    retry[re_num] = 0;
    struct timeval timeout = sock->send_wnd->timeout;
    struct itimerval timer = {0};
    timer.it_value = timeout;
    setitimer(ITIMER_REAL, &timer, NULL);
}

void stop_timeout(int re_num){
    retry[re_num] = 0;
    struct itimerval timer = {0};
    setitimer(ITIMER_REAL, &timer, NULL);
}

void syn_timeout_check(int signal){
    retry[SYN_RETRY] = 1;
}

void synack_timeout_check(int signal){
    retry[SYNACK_RETRY] = 1;
}

void fin_timeout_check(int signal){
    retry[FIN_RETRY] = 1;
}

void finack_timeout_check(int signal){
    retry[FINACK_RETRY] = 1;
}

void data_timeout_check(int signal){
    retry[DATA_RETRY] = 1;
}

/* 重传当前窗口内的未ACK的数据 */
void resend_data(tju_tcp_t* sock){
    pthread_mutex_lock(&(sock->send_buf->lock));
    resend_data_locked(sock);
    pthread_mutex_unlock(&(sock->send_buf->lock));
}

static void resend_data_locked(tju_tcp_t* sock){
    uint32_t seq_num, ack_num;
    int win_size, noack_size, nosent_size, total_send_size, send_size, plen;

    int buf_base, buf_next, buf_size, buf_store, buf_free;
    char* data, *msg;

    uint32_t cur_seq, buf_off;
    int remain;

    /* 占用缓冲区 */
    ack_num = sock->recv_wnd->expect_seq;
    seq_num = sock->send_wnd->base_seq;  // 从base开始发
    win_size = sock->send_wnd->window_size;
    buf_base = sock->send_buf->base;
    buf_next = sock->send_buf->next;
    buf_store = sock->send_buf->store;
    buf_size = sock->send_buf->capacity;
    noack_size = (buf_next - buf_base + buf_size) % buf_size;  // 发送但是没有ACK的数据大小

    /* 将窗口内的包都发出去 */
    noack_size = (uint32_t)(sock->send_wnd->next_seq - sock->send_wnd->base_seq);
    if(noack_size > sock->send_buf->count){
        noack_size = sock->send_buf->count;
    }
    if(noack_size > win_size){
        noack_size = win_size;
    }
    remain = noack_size;
    cur_seq = seq_num;
    buf_off = buf_base;  // 基地址是base

    while(remain > 0){
        /* SMSS约束 */
        send_size = (remain < SMSS)? remain: SMSS;

        /* 从缓冲区中取数据 */
        if(send_size != 0 && buf_off + send_size <= buf_size){
            data = (char*)malloc(send_size * sizeof(char));
            memcpy(data, sock->send_buf->mem + buf_off, send_size);
        }
        else{
            int len1 = buf_size - buf_off;
            int len2 = send_size - len1;
            data = (char*)malloc(send_size * sizeof(char));
            memcpy(data, sock->send_buf->mem + buf_off, len1);
            memcpy(data + len1, sock->send_buf->mem, len2);
        }

        /* 发包 */
        plen = DEFAULT_HEADER_LEN + send_size;
        msg = create_packet_buf(sock->established_local_addr.port,
                sock->established_remote_addr.port, cur_seq, ack_num,
                DEFAULT_HEADER_LEN, plen,
                NO_FLAG, 0, 0, data, send_size);
        sendToLayer3(msg, plen);
        free(data);

        /* 更新指针 */
        cur_seq = (uint32_t)(cur_seq + send_size);
        buf_off = (buf_off + send_size) % buf_size;
        remain -= send_size;
    }
    /* 解锁 */
}

void update_rto(tju_tcp_t* sock){
    
}
