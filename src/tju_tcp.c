#include "tju_tcp.h"

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
    sock->send_buf->mem = (char*)(malloc(SEND_BUF_SIZE * sizeof(char)));
    sock->send_buf->capacity = SEND_BUF_SIZE;

    sock->recv_buf = (recv_buf_t*)malloc(sizeof(recv_buf_t));
    pthread_mutex_init(&(sock->recv_buf->lock), NULL);
    sock->recv_buf->mem = (char*)(malloc(RECV_BUF_SIZE * sizeof(char)));
    sock->recv_buf->capacity = RECV_BUF_SIZE;

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
            /*################## SYNACK Timeout ##################
            * new_conn 已经进入半连接队列并且当前状态处于 SYN_RECV
            * 需要提前在 sender_window 中设置当前连接的seq和ack_num
            */
            pthread_mutex_lock(half_conn_queue[hashval]->lock);
            conn_node front_info = *(half_conn_queue[hashval]->front);
            pthread_mutex_unlock(half_conn_queue[hashval]->lock);
            send_flags(front_info.sock, (SYN_FLAG_MASK | ACK_FLAG_MASK),
                        front_info.sock->local_isn, front_info.sock->remote_isn + 1);

            // 重新设置 SYN + ACK 超时监测
            set_timeout(3, SYNACK_RETRY);
            printf("Server retransmit SYNACK!\n");
        }
    }
    // 从全连接队列中出队
    conn_node* new_conn_node = pop_full_conn_queue(hashval);
    tju_tcp_t* new_conn = new_conn_node->sock;

    // 连接已建立，启动发送和重传线程
    pthread_t pid_send = 1895;
    pthread_t pid_retry = 1896;
    int rst = pthread_create(&pid_send, NULL, send_thread, (void*)(new_conn));
    if (rst<0){
        printf("ERROR create send_thread!\n");
        exit(-1);
    }
    new_conn->send_thread_id = pid_send;
    // rst = pthread_create(&pid_retry, NULL, retry_thread, (void*)(new_conn));
    // if (rst<0){
    //     printf("ERROR create retry_thread!\n");
    //     exit(-1);
    // }

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
    printf("Client sent SYN, clint_isn = %d!\n", seq_num);

    // 设置 SYN 超时检测
    set_timeout(1, SYN_RETRY);

    // 接收 SYN + ACK
    while (sock->state != ESTABLISHED){
        // 阻塞
        if(retry[SYN_RETRY]){
            // 将连接建立后的重传超时设置为3s
            sock->send_wnd->timeout.tv_sec = 3;
            sock->send_wnd->timeout.tv_usec = 0;

            // 重传 SYN
            send_flags(sock, SYN_FLAG_MASK, sock->local_isn, sock->remote_isn);
            set_timeout(1, SYN_RETRY);
            printf("Client retransmit SYN!\n");
        }
    }

    // 连接已建立，启动发送和重传线程
    pthread_t pid_send = 1895;
    pthread_t pid_retry = 1896;
    int rst = pthread_create(&pid_send, NULL, send_thread, (void*)(sock));
    if (rst<0){
        printf("ERROR create send_thread!\n");
        exit(-1);
    }
    sock->send_thread_id = pid_send;
    // rst = pthread_create(&pid_retry, NULL, retry_thread, (void*)(sock));
    // if (rst<0){
    //     printf("ERROR create retry_thread!\n");
    //     exit(-1);
    // }
    printf("I have conncect.\n");

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

    while(1){
        pthread_mutex_lock(&(sock->send_buf->lock));
        buf_base = sock->send_buf->base;
        buf_next = sock->send_buf->next;
        buf_store = sock->send_buf->store;
        buf_free = buf_size - ((buf_size + buf_store - buf_base) % buf_size);
        // 忙等待能够放入缓冲区当中
        if(buf_free >= len){
            // 现在占据发送缓冲区的锁
            printf("I will wrote data.\n");
            break;
        }
        pthread_mutex_unlock(&(sock->send_buf->lock));
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

    pthread_mutex_unlock(&(sock->send_buf->lock));
    printf("[User]: Send function wrote %d byte data in buf[%d].\n",len, buf_store);
    
    return len;
}

// 采用阻塞式接收
int tju_recv(tju_tcp_t* sock, void *buffer, int len){
    int head_index, end_index, buf_size, noread_len;

    pthread_mutex_lock(&(sock->state_lock));
    if(sock->state != ESTABLISHED && sock->state != CLOSE_WAIT){
        printf("Now the connect is break! [RECV]\n");
        pthread_mutex_unlock(&(sock->state_lock));
        return 0;  // 不接受连接断开之后的发送请求
    }
    pthread_mutex_unlock(&(sock->state_lock));

    // 等待有len字节数据可读
    while(1){
        pthread_mutex_lock(&(sock->recv_buf->lock));
        head_index = sock->recv_buf->head;
        end_index = sock->recv_buf->expect;
        buf_size = sock->recv_buf->capacity;
        noread_len = (end_index - head_index + buf_size) % buf_size;
        if(noread_len >= len){
            printf("[User]: recv %d byte data!\n", len);
            break;
        }
        pthread_mutex_unlock(&(sock->recv_buf->lock));
    }

    // 内核缓冲区复制到用户接收缓冲区buffer
    int win_size = sock->recv_wnd->window_size;
    int win_read_len = (noread_len < win_size)? noread_len: win_size;  // 窗口约束
    int read_len = (win_read_len < len)? win_read_len: len;  // len约束
    if(read_len == 0){
        printf("[User] Recv_buf has no data.\n");
        pthread_mutex_unlock(&(sock->recv_buf->lock)); // 解锁
        return 0;
    }

    if(head_index + read_len <= buf_size){
        memcpy(buffer, sock->recv_buf->mem + head_index, read_len);
    }
    else{
        int len1 = buf_size - head_index;
        int len2 = read_len - len1;
        memcpy(buffer, sock->recv_buf->mem + head_index, len1);
        memcpy(buffer + len1, sock->recv_buf->mem, len2);
    }
    printf("[User]: Recv function have memcpyed into user_memory.\n");
    
    // 更新 head
    sock->recv_buf->head = (sock->recv_buf->head + read_len) % buf_size;

    pthread_mutex_unlock(&(sock->recv_buf->lock)); // 解锁

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

    // 获取主机名，便于日志记录
    char hostname[8];
    gethostname(hostname, 8);

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
        printf("Server sent SYNACK, server_isn = %d!\n", server_isn);
        
        // 设置 SYN + ACK 超时检测
        set_timeout(3, SYNACK_RETRY);

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

        // 发送ACK
        send_flags(sock, ACK_FLAG_MASK, sock->local_isn + 1, sock->remote_isn + 1);
        sock->state = ESTABLISHED;
        printf("Client sent ACK!\n");

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
        printf("Client retransmit ACK!\n");

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

        return 0;
    /* ################################# 三次握手状态机结束 #######################################*/
    }
    /* ################################# 四次挥手状态机开始 #######################################*/
    // Receiver: [Passive], Flags: [ACK | FIN]
    else if((st == ESTABLISHED) && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){
        pthread_mutex_unlock(&sock->state_lock);

        // 设置 window
        sock->local_fin_seq = ack_num;
        sock->remote_fin_seq = seq_num;

        send_flags(sock, (ACK_FLAG_MASK), sock->local_fin_seq, sock->remote_fin_seq + 1);
        printf("%s(passive) sent ACK!\n", hostname);
        sock->state = CLOSE_WAIT;

        return 0;
    }
    // Receiver: [Active], Flags: [ACK]
    else if((st == FIN_WAIT_1) && data_len == 0 && flags == (ACK_FLAG_MASK)){
        pthread_mutex_unlock(&sock->state_lock);

        stop_timeout(FINACK_RETRY);

        // 状态转移->FIN_WAIT_2
        sock->state = FIN_WAIT_2;

        return 0;
    }
    // Receiver: [Active], Flags: [ACK | FIN]
    else if((st == FIN_WAIT_2) && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){
        pthread_mutex_unlock(&sock->state_lock);

        // 取消FINACK超时监控
        stop_timeout(FINACK_RETRY);

        // 发送ACK
        send_flags(sock, (ACK_FLAG_MASK), sock->local_fin_seq + 1, sock->remote_fin_seq + 1);
        printf("%s(active) sent ACK!\n", hostname);
        sock->state = TIME_WAIT;

        return 0;
    }
    // Receiver: [Passive], Flags: [ACK]
    else if((st == LAST_ACK) && data_len == 0 && flags == ACK_FLAG_MASK){
        pthread_mutex_unlock(&sock->state_lock);

        // 最终 ACK 到达，被动关闭结束，停掉 FINACK 重传计时后再释放连接。
        stop_timeout(FINACK_RETRY);

        sock->state = CLOSED;
        printf("LAST_ACK --> CLOSED\n");
        return 0;
    }
    // Receiver: [Active], Flags: [FINACK]   /* TIME_WAIT 期间收到重复 FINACK */
    else if((st == TIME_WAIT) && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){
        pthread_mutex_unlock(&sock->state_lock);

        // 最终 ACK 可能已经丢失，对端仍停留在 LAST_ACK 并重传 FINACK。
        // 本端在 TIME_WAIT 中应再次发送 ACK，否则对端会一直重传。
        send_flags(sock, (ACK_FLAG_MASK), sock->local_fin_seq + 1, sock->remote_fin_seq + 1);
        printf("%s(active) retransmit ACK in TIME_WAIT!\n", hostname);

        return 0;
    }
    /*################################### 同时关闭连接 #######################################*/
    // Receiver: [peer], Flags: [FINACK]
    else if((st == FIN_WAIT_1) && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){
        pthread_mutex_unlock(&sock->state_lock);

        // 发送ACK
        send_flags(sock, (ACK_FLAG_MASK), sock->local_fin_seq, sock->remote_fin_seq + 1);
        printf("%s(peer) sent ACK!\n", hostname);

        // CLOSING 特殊状态
        sock->state = CLOSING;

        return 0;
    }
    // Receiver: [peer], Flags: [ACK]
    else if((st == CLOSING) && data_len == 0 && flags == ACK_FLAG_MASK){
        pthread_mutex_unlock(&sock->state_lock);

        // 取消 FINACK 计时
        stop_timeout(FINACK_RETRY);

        sock->state = TIME_WAIT;
        return 0;
    /*################################### 连接管理结束 #######################################*/
    }
    /*################################# 数据发送端处理ACK ####################################*/
    else if((st == ESTABLISHED) && data_len == 0 && flags == ACK_FLAG_MASK){
        pthread_mutex_unlock(&sock->state_lock);
        
        uint32_t base_seq = sock->send_wnd->base_seq;
        uint32_t next_seq = sock->send_wnd->next_seq;
        int buf_size = sock->send_buf->capacity;
        int ack_quantity = 0;
        /* 校验是否为正确范围内ACK */
        if(ack_num > base_seq && ack_num <= next_seq){
            ack_quantity = ack_num - base_seq;
        }
        else if(next_seq < base_seq && ack_num <= next_seq){
            ack_quantity = (SEQ_MAX - base_seq) + ack_num;
        }
        
        // 更新 base, base_seq
        if(ack_quantity > 0){
            sock->send_buf->base = (sock->send_buf->base + ack_quantity) % buf_size;
            sock->send_wnd->base_seq = ack_num;  // 累计确认
            printf("[Kernel]: Received the ACK to the %d.\n", ack_num);
        }

        return 0;
    }
    pthread_mutex_unlock(&sock->state_lock);
    /*#################################### 接收真实数据 #####################################*/

    // 返回值非0，说明需要加锁失败，要一直尝试加锁
    while(pthread_mutex_lock(&(sock->recv_buf->lock)) != 0); // 加锁

    // 缓冲区仅仅复制接收窗口大小的数据
    uint16_t win_size = sock->recv_wnd->window_size;
    uint32_t buf_size = sock->recv_buf->capacity;
    int buf_expect = sock->recv_buf->expect;
    uint32_t expect_num = sock->recv_wnd->expect_seq;
    uint32_t putlen = (win_size < data_len)? win_size: data_len;

    if(putlen == 0){
        pthread_mutex_unlock(&(sock->recv_buf->lock)); // 解锁
        return 0;
    }
    
    if(seq_num != expect_num){
        printf("seq_num(%d) != expect_num(%d)!\n", seq_num, expect_num);
        pthread_mutex_unlock(&(sock->recv_buf->lock)); // 解锁
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
    send_flags(sock, ACK_FLAG_MASK, sock->send_wnd->next_seq, sock->recv_wnd->expect_seq);
    pthread_mutex_unlock(&(sock->recv_buf->lock)); // 解锁
    printf("[Kernel] Sent Ack to the %d.\n", seq_num + putlen);

    return 0;
}

int tju_close (tju_tcp_t* sock){
    // 获取主机名，便于日志记录
    char hostname[8];
    gethostname(hostname, 8);
    uint32_t seq_num, ack_num;
    
    pthread_mutex_lock(&(sock->state_lock));  // 与后台线程抢锁
    int active_close = (sock->state == ESTABLISHED);  // 确定主动被动
    if(active_close){
        printf("#####################ACTIVE######################\n");
        
    }
    else{
        printf("#####################PASSIVE######################\n");
    }

    if(active_close){
        pthread_mutex_unlock(&(sock->state_lock));
        while(1){
            pthread_mutex_lock(&(sock->send_buf->lock));
            if((sock->send_buf->next == sock->send_buf->store)){
                // 确保所有提交的数据都被送出缓冲区
                pthread_mutex_unlock(&(sock->send_buf->lock));
                break;
            }
            pthread_mutex_unlock(&(sock->send_buf->lock));
        }

        pthread_mutex_lock(&(sock->state_lock));
        sock->state = FIN_WAIT_1;
        pthread_mutex_unlock(&(sock->state_lock));  // 解决了Close和后台线程同时FIN的竞态条件

        seq_num = sock->send_wnd->next_seq;
        ack_num = sock->recv_wnd->expect_seq;
        sock->local_fin_seq = seq_num;
        sock->remote_fin_seq = ack_num;
        
        send_flags(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK), seq_num, ack_num);

        printf("%s(active) sent FINACK!\n", hostname);
        set_timeout(3, FINACK_RETRY);

        while(1){
            pthread_mutex_lock(&(sock->state_lock));
            if(sock->state == TIME_WAIT){
                pthread_mutex_unlock(&(sock->state_lock));
                break;
            }
            if(retry[FINACK_RETRY]){
                // 重传 FINACK
                send_flags(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK), sock->local_fin_seq, sock->remote_fin_seq);
                set_timeout(3, FINACK_RETRY);
                if(sock->state != CLOSING){
                    // 主动关闭
                    printf("%s(active) retransimit FINACK!\n", hostname);
                }
                else{
                    // 同时关闭
                    printf("%s(peer) retransimit FINACK!\n", hostname);
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

        printf("%s(active) closed!\n", hostname);
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
        }

        // 等待接收缓冲区处理完毕
        while(1){
            pthread_mutex_lock(&(sock->recv_buf->lock));
            uint32_t head_index = sock->recv_buf->head;
            uint32_t end_index = sock->recv_buf->expect;
            if(head_index == end_index){
                pthread_mutex_unlock(&(sock->recv_buf->lock));
                break;
            }
            pthread_mutex_unlock(&(sock->recv_buf->lock));
        }

        send_flags(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK), sock->local_fin_seq, sock->remote_fin_seq + 1);
        printf("%s(passive) sent FINACK!\n", hostname);
        set_timeout(3, FINACK_RETRY);

        pthread_mutex_lock(&(sock->state_lock));
        sock->state = LAST_ACK;
        pthread_mutex_unlock(&(sock->state_lock));

        while(1){
            pthread_mutex_lock(&(sock->state_lock));
            if(sock->state == CLOSED){
                pthread_mutex_unlock(&(sock->state_lock));
                break;
            }
            if(retry[FINACK_RETRY]){
                // 被动方已发送 FINACK，仍未收到最终 ACK 时重传 FINACK。
                send_flags(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK), sock->local_fin_seq, sock->remote_fin_seq + 1);
                set_timeout(3, FINACK_RETRY);
                printf("%s(passive) retransimit FINACK!\n", hostname);
            }
            pthread_mutex_unlock(&(sock->state_lock));
        }
        stop_timeout(FINACK_RETRY);
        printf("%s(passive) closed!\n", hostname);
    }
    // 收回发送线程
    if(sock->send_thread_id != 0){
        pthread_join(sock->send_thread_id, NULL);
    }

    return 0;
}

// 发送线程，前置条件是必须连接建立后调用
static void* send_thread(void* arg){
    tju_tcp_t* sock = (tju_tcp_t*)(arg);
    uint32_t seq_num, ack_num;
    int win_size, noack_size, nosent_size, win_send_size, send_size, plen;

    int buf_base, buf_next, buf_size, buf_store, buf_free;
    char* data, *msg;

    while(1){
        // 确认状态
        pthread_mutex_lock(&(sock->state_lock));
        if(sock->state != ESTABLISHED){
            pthread_mutex_unlock(&(sock->state_lock));
            break;
        }
        pthread_mutex_unlock(&(sock->state_lock));

        // 占用缓冲区
        ack_num = sock->recv_wnd->expect_seq;
        seq_num = sock->send_wnd->next_seq;
        win_size = sock->send_wnd->window_size;
        pthread_mutex_lock(&(sock->send_buf->lock));
        buf_base = sock->send_buf->base;
        buf_next = sock->send_buf->next;
        buf_store = sock->send_buf->store;
        buf_size = sock->send_buf->capacity;
        noack_size = (buf_next - buf_base + buf_size) % buf_size;  // 发送但是没有ACK的数据大小
        nosent_size =  (buf_store - buf_next + buf_size) % buf_size;  // 缓冲区中所有没有发送的数据大小
        win_send_size = ((win_size - noack_size) < nosent_size)? (win_size - noack_size): nosent_size;  // 窗口约束
        send_size = (win_send_size < SMSS)? win_send_size: SMSS;  // SMSS约束

        if(send_size == 0){
            // 没有可发数据
            pthread_mutex_unlock(&(sock->send_buf->lock));
            continue;
        }

        /* 从缓冲区中取数据 */
        if(send_size != 0 && buf_next + send_size < buf_size){
            data = (char*)malloc(send_size * sizeof(char));
            memcpy(data, sock->send_buf->mem + buf_next, send_size);
        }
        else if(send_size != 0 && buf_next + send_size >= buf_size){
            int len1 = buf_size - buf_next;
            int len2 = buf_next + send_size - buf_size;
            data = (char*)malloc(send_size * sizeof(char));
            memcpy(data, sock->send_buf->mem + buf_next, len1);
            memcpy(data + len1, sock->send_buf->mem, len2);
        }
        else{
            pthread_mutex_unlock(&(sock->send_buf->lock));
            continue;
        }

        /* 发包 */
        plen = DEFAULT_HEADER_LEN + send_size;
        msg = create_packet_buf(sock->established_local_addr.port,
                sock->established_remote_addr.port, seq_num, ack_num,
                DEFAULT_HEADER_LEN, plen,
                NO_FLAG, 0, 0, data, send_size);
        sendToLayer3(msg, plen);
        free(data);

        /* 更新窗口 */
        sock->send_wnd->next_seq = (uint32_t)(seq_num + send_size);
        sock->send_buf->next = (buf_next + send_size) % buf_size;

        /* 日志 */
        printf("[User] Sent pkt src=%d dst=%d flags=0x%x seq=%d ack=%d datalen=%d.\n",
            sock->established_local_addr.port, sock->established_remote_addr.port,
            get_flags(msg), get_seq(msg), get_ack(msg), send_size);

        /* 解锁 */
        pthread_mutex_unlock(&(sock->send_buf->lock));
    }
    printf("Send_thread exit.\n");
    return NULL;
}


/*
* ############################## 辅助函数,用户不应该调用 ##############################
*/

static int send_flags(tju_tcp_t* sock, uint8_t flags, uint32_t seq_num, uint32_t ack_num){
    char* msg;
    uint16_t plen = DEFAULT_HEADER_LEN;  // 无 Payload

    tju_sock_addr local_addr = sock->established_local_addr;
    tju_sock_addr remote_addr = sock->established_remote_addr;

    msg = create_packet_buf(local_addr.port, remote_addr.port, seq_num, ack_num, 
              DEFAULT_HEADER_LEN, plen, flags, 0, 0, NULL, 0);
    sendToLayer3(msg, plen);

    return 0;
}

// 释放sock对应的连接资源
static void free_sock(tju_tcp_t* sock){
    int hashval = cal_hash(sock->established_local_addr.ip, sock->established_local_addr.port,
                           sock->established_remote_addr.ip, sock->established_remote_addr.port);
    established_socks[hashval] = NULL;

    free(sock->send_buf->mem);
    free(sock->send_wnd);
    pthread_mutex_destroy(&(sock->send_buf->lock));

    free(sock->recv_buf->mem);
    free(sock->recv_wnd);
    pthread_mutex_destroy(&(sock->recv_buf->lock));

    pthread_mutex_destroy(&(sock->state_lock));

    free(sock);
    
    destroy_half_conn_queue(hashval);
    destroy_full_conn_queue(hashval);
}

// 初始化缓冲区窗口
static void init_buf_wnd(tju_tcp_t* sock){
    sock->send_wnd = (sender_window_t *)(malloc(sizeof(sender_window_t)));
    sock->send_wnd->window_size = TCP_SENDWN_SIZE;
    sock->send_wnd->base_seq = sock->local_isn + 1;
    sock->send_buf->base = 0;  // 没发出任何数据
    sock->send_wnd->next_seq = sock->local_isn + 1;
    sock->send_buf->next = 0;
    sock->send_buf->store = 0; // 从0开始存放数据

    sock->recv_wnd = (receiver_window_t *)(malloc(sizeof(receiver_window_t)));
    sock->recv_wnd->window_size = TCP_RECVWN_SIZE;
    sock->recv_wnd->expect_seq = sock->remote_isn + 1;
    sock->recv_buf->expect = 0;
    sock->recv_buf->head = 0;
}