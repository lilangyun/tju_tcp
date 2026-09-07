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
    
    pthread_mutex_init(&(sock->send_lock), NULL);
    sock->sending_buf = (char*)(malloc(SEND_BUF_SIZE * sizeof(char)));
    sock->sending_len = SEND_BUF_SIZE;

    pthread_mutex_init(&(sock->recv_lock), NULL);
    sock->received_buf = (char*)(malloc(RECV_BUF_SIZE * sizeof(char)));
    sock->received_len = RECV_BUF_SIZE;
    
    if(pthread_cond_init(&sock->wait_cond, NULL) != 0){
        perror("ERROR condition variable not set\n");
        exit(-1);
    }

    sock->window.wnd_send = NULL;
    sock->window.wnd_recv = NULL;

    // 初始化定时模块
    init_retry();

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
            printf("[tju_accept]\n");
            pthread_mutex_lock(half_conn_queue[hashval]->lock);
            conn_node front_info = *(half_conn_queue[hashval]->front);
            send_flags(front_info.sock, SYN_FLAG_MASK | ACK_FLAG_MASK);
            pthread_mutex_unlock(half_conn_queue[hashval]->lock);

            // 重新设置 SYN + ACK 超时监测
            set_timeout(3, SYNACK_RETRY);
            printf("Server retransmit SYNACK!\n");
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
    uint32_t seq = sp.tv_sec % (SEQ_MAX);  // client_isn

    // 初始化 window
    sock->window.wnd_send = (sender_window_t *)(malloc(sizeof(sender_window_t)));
    sock->window.wnd_recv = (receiver_window_t *)(malloc(sizeof(receiver_window_t)));
    pthread_mutex_init(&(sock->window.wnd_recv->cond_lock), NULL);
    sock->window.wnd_recv->has_new_data = 0;
    sock->window.wnd_send->window_size = TCP_SENDWN_SIZE;
    sock->window.wnd_recv->window_size = TCP_RECVWN_SIZE;
    sock->window.wnd_send->baseseq = seq;
    sock->window.wnd_send->base = -1;  // 表示当前还没有发送任何包
    sock->window.wnd_send->nextseq = seq;
    sock->window.wnd_send->next = 0;
    sock->window.wnd_recv->expect_seq = 0;  // ack_num = 0
    sock->window.wnd_recv->expect = 0;
    sock->window.wnd_recv->head = 0;

    // 放入listen_socks哈希表，用于发送SYN后接收SYN+ACK
    int hashval = cal_hash(local_addr.ip, local_addr.port, 0, 0);
    listen_socks[hashval] = sock;
    
    pthread_mutex_lock(&sock->state_lock);
    // 发送 SYN
    send_flags(sock, SYN_FLAG_MASK);
    sock->state = SYN_SENT;
    pthread_mutex_unlock(&sock->state_lock);
    printf("Client sent SYN!\n");

    // 设置 SYN 超时检测
    set_timeout(1, SYN_RETRY);

    // 接收 SYN + ACK
    while (sock->state != ESTABLISHED){
        // 阻塞
        if(retry[SYN_RETRY]){
            // 将连接建立后的重传超时设置为3s
            sock->window.wnd_send->timeout.tv_sec = 3;
            sock->window.wnd_send->timeout.tv_usec = 0;

            // 重传 SYN
            send_flags(sock, SYN_FLAG_MASK);
            set_timeout(3, SYN_RETRY);
            printf("Client retransmit SYN!\n");
        }
    }

    // 连接已建立，启动发送和重传线程
    pthread_t pid_send, pid_retry;
    int rst = pthread_create(&pid_send, NULL, send_thread, (void*)(sock));
    if (rst<0){
        printf("ERROR create send_thread!\n");
        exit(-1);
    }
    // rst = pthread_create(&pid_retry, NULL, retry_thread, (void*)(sock));
    // if (rst<0){
    //     printf("ERROR create retry_thread!\n");
    //     exit(-1);
    // }

    return 0;
}

int tju_send(tju_tcp_t* sock, const void *buffer, int len){
    if(len <= 0) return len;

    pthread_mutex_lock(&(sock->send_lock));
    int buf_base = sock->window.wnd_send->base;
    int buf_next = sock->window.wnd_send->next;
    int buf_used = sock->window.wnd_send->used;
    int buf_size = sock->sending_len;
    int buf_avail = buf_size - buf_used;  // 缓冲区可用空间
    int write_pos = (buf_base < 0)? 0: (buf_base + buf_used) % buf_size;
    printf("[SEND]: I write %d byte data in %d.\n",len,  write_pos);

    if(buf_avail < len){
        printf("Now the sending_buf only has %d bytes!\n", buf_avail);
        return -1;
    }

    if(write_pos + len <= buf_size){
        memcpy(sock->sending_buf + write_pos, buffer, len);
    }
    else{
        int len_1 = buf_size - write_pos;
        int len_2 = len - len_1;
        memcpy(sock->sending_buf + write_pos, buffer, len_1);
        memcpy(sock->sending_buf, buffer + len_1, len_2);
    }

    sock->window.wnd_send->used += len;

    pthread_mutex_unlock(&(sock->send_lock));
    
    return 0;
}
int tju_recv(tju_tcp_t* sock, void *buffer, int len){

    // 等待被唤醒
    pthread_mutex_lock(&(sock->window.wnd_recv->cond_lock));
    while(!(sock->window.wnd_recv->has_new_data)){
        pthread_cond_wait(&(sock->wait_cond), &(sock->window.wnd_recv->cond_lock));
    }
    sock->window.wnd_recv->has_new_data = 0;
    pthread_mutex_unlock(&(sock->window.wnd_recv->cond_lock));

    // 用户处理缓冲区数据
    while(pthread_mutex_lock(&(sock->recv_lock)) != 0); // 加锁

    // 内核缓冲区复制到用户接收缓冲区buffer
    int head_index = sock->window.wnd_recv->head;
    int end_index = sock->window.wnd_recv->expect;
    int buf_size = sock->received_len;
    int noread_len = (end_index - head_index + buf_size) % buf_size;
    int read_len = (noread_len < len)? noread_len: len;
    if(head_index + read_len <= buf_size){
        memcpy(buffer, sock->received_buf, read_len);
    }
    else{
        int len1 = buf_size - head_index;
        int len2 = read_len - len1;
        memcpy(buffer, sock->received_buf + head_index, len1);
        memcpy(buffer + len1, sock->received_buf, len2);
    }
    
    // 更新 head
    sock->window.wnd_recv->head = (sock->window.wnd_recv->head + read_len) % buf_size;

    pthread_mutex_unlock(&(sock->recv_lock)); // 解锁

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
    uint32_t seq = get_seq(pkt);
    uint32_t ack_num = get_ack(pkt);

    // 获取主机名，便于日志记录
    char hostname[8];
    gethostname(hostname, 8);
    
    /*##################################### 连接管理 ###########################################*/
    // Receiver: [server], Flags: [SYN]
    if(sock->state == LISTEN && data_len == 0 && flags == SYN_FLAG_MASK){

        // 新建socket: new_conn
        tju_tcp_t* new_conn = (tju_tcp_t*)malloc(sizeof(tju_tcp_t));
        memcpy(new_conn, sock, sizeof(tju_tcp_t));
        new_conn->established_local_addr.port = local_addr.port;
        new_conn->established_local_addr.ip = local_addr.ip;
        new_conn->established_remote_addr = remote_addr;

        // 设置 server_isn
        struct timespec sp;
        clock_gettime(CLOCK_MONOTONIC, &sp);
        uint32_t server_isn = sp.tv_sec % SEQ_MAX;
        uint32_t client_isn = seq;

        // 初始化 window -> 用于用户线程重传 SYNACK
        new_conn->window.wnd_send = (sender_window_t*)(malloc(sizeof(sender_window_t)));
        new_conn->window.wnd_recv = (receiver_window_t*)(malloc(sizeof(receiver_window_t)));
        pthread_mutex_init(&(new_conn->window.wnd_recv->cond_lock), NULL);
        new_conn->window.wnd_recv->has_new_data = 0;
        new_conn->window.wnd_send->window_size = TCP_SENDWN_SIZE;
        new_conn->window.wnd_recv->window_size = TCP_RECVWN_SIZE;
        new_conn->window.wnd_send->baseseq = server_isn;
	    new_conn->window.wnd_send->base = -1;  // 无有效数据发送
        new_conn->window.wnd_send->nextseq = server_isn;
	    new_conn->window.wnd_send->next = 0;
        new_conn->window.wnd_recv->expect_seq = client_isn + 1;
	    new_conn->window.wnd_recv->expect = 0;  // 未收到任何数据
        new_conn->window.wnd_recv->head = 0;

        // 将new_conn加入listen _socket half_conn_queue
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
        send_flags(new_conn, SYN_FLAG_MASK | ACK_FLAG_MASK);

        // 设置状态
        pthread_mutex_lock(&new_conn->state_lock);
        new_conn->state = SYN_RECV;
        pthread_mutex_unlock(&new_conn->state_lock);
        printf("Server sent SYNACK!\n");
        
        // 设置 SYN + ACK 超时检测
        set_timeout(3, SYNACK_RETRY);

        return 0;
    }
    // Receiver: [client], Flags: [SYN + ACK]
    else if(sock->state == SYN_SENT && data_len == 0 && flags == (SYN_FLAG_MASK | ACK_FLAG_MASK)){

        // 取消 SYN 超时检测
        stop_timeout(SYN_RETRY);

        // 更换连接的远程地址为new_conn，而不是bind_addr
        sock->established_remote_addr = remote_addr;

        // 设置 window
        sock->window.wnd_send->window_size = TCP_SENDWN_SIZE;
        sock->window.wnd_recv->window_size = TCP_RECVWN_SIZE;
        sock->window.wnd_send->baseseq = ack_num;
	    sock->window.wnd_send->base = -1;
        sock->window.wnd_send->nextseq = ack_num;
	    sock->window.wnd_send->next = 0;
        sock->window.wnd_recv->expect_seq = seq + 1;
        sock->window.wnd_recv->expect = 0;
        sock->window.wnd_send->used = 0;
        sock->window.wnd_recv->head = -1;  // 无有效数据

        // 发送ACK
        send_flags(sock, ACK_FLAG_MASK);
        pthread_mutex_lock(&sock->state_lock);
        sock->state = ESTABLISHED;
        pthread_mutex_unlock(&sock->state_lock);
        printf("Client sent ACK!\n");

        // 设置 ACK 超时检测
        set_timeout(3, ACK_RETRY);

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
    else if(sock->state == ESTABLISHED && data_len == 0 && flags == (SYN_FLAG_MASK | ACK_FLAG_MASK)){
        
        // 取消 ACK 超时检测
        stop_timeout(ACK_RETRY);

        // 发送ACK
        send_flags(sock, ACK_FLAG_MASK);
        printf("Client retransmit ACK!\n");

        // 设置 ACK 超时检测
        set_timeout(3, ACK_RETRY);

        return 0;
    }
    // Receiver: [server], Flags: [ACK]
    else if(sock->state == SYN_RECV && data_len == 0 && flags == ACK_FLAG_MASK){
        
        // 设置窗口
        sock->window.wnd_send->used = 0;
        sock->window.wnd_recv->head = -1;

        // 取消 SYN + ACK 超时检测
        stop_timeout(SYNACK_RETRY);

        // 出半连接队列
        int hashval = cal_hash(sock->bind_addr.ip, sock->bind_addr.port, 0, 0);
        int conn_hashval = cal_hash(local_addr.ip, local_addr.port,
                                    remote_addr.ip, remote_addr.port);
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
        pthread_mutex_lock(&new_conn->state_lock);
        new_conn->state = ESTABLISHED;
        pthread_mutex_unlock(&new_conn->state_lock);

        return 0;
    /* ################################# 三次握手状态机结束 #######################################*/
    }
    /* ################################# 四次挥手状态机开始 #######################################*/
    // Receiver: [Passive], Flags: [ACK | FIN]
    else if((sock->state == ESTABLISHED) && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){

        // 设置 window
        sock->window.wnd_send->window_size = 1;
        sock->window.wnd_send->baseseq = ack_num;
        sock->window.wnd_send->nextseq = ack_num;
        sock->window.wnd_recv->expect_seq = seq + 1;

        // 发送 ACK
        send_flags(sock, (ACK_FLAG_MASK));
        printf("%s(passive) sent ACK!\n", hostname);
        pthread_mutex_lock(&sock->state_lock);
        sock->state = CLOSE_WAIT;
        pthread_mutex_unlock(&sock->state_lock);

        // 避免状态转移过快接收方只能处理一个packet
        // sleep(2);

        // 发送 FINACK
        send_flags(sock, (ACK_FLAG_MASK | FIN_FLAG_MASK));
        printf("%s(passive) sent FINACK!\n", hostname);
        pthread_mutex_lock(&sock->state_lock);
        sock->state = LAST_ACK;
        pthread_mutex_unlock(&sock->state_lock);

        return 0;
    }
    // Receiver: [Active], Flags: [ACK]
    else if((sock->state == FIN_WAIT_1) && data_len == 0 && flags == (ACK_FLAG_MASK)){

        // 取消FINACK超时监控
        stop_timeout(FINACK_RETRY);
        
        // 状态转移->FIN_WAIT_2
        pthread_mutex_lock(&sock->state_lock);
        sock->state = FIN_WAIT_2;
        pthread_mutex_unlock(&sock->state_lock);

        return 0;
    }
    // Receiver: [Active], Flags: [ACK | FIN]
    else if(sock->state == FIN_WAIT_2 && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){

        // 取消FINACK超时监控
        stop_timeout(FINACK_RETRY);

        // 设置窗口
        sock->window.wnd_send->window_size = 1;
        sock->window.wnd_send->baseseq = ack_num;
        sock->window.wnd_send->nextseq = ack_num;
        sock->window.wnd_recv->expect_seq = seq + 1;

        // 发送ACK
        send_flags(sock, (ACK_FLAG_MASK));
        printf("%s(active) sent ACK!\n", hostname);
        pthread_mutex_lock(&sock->state_lock);
        sock->state = TIME_WAIT;
        pthread_mutex_unlock(&sock->state_lock);

        return 0;
    }
    // Receiver: [Passive], Flags: [ACK]
    else if(sock->state == LAST_ACK && data_len == 0 && flags == ACK_FLAG_MASK){

        // 关闭
        pthread_mutex_lock(&sock->state_lock);
        sock->state = CLOSED;
        pthread_mutex_unlock(&sock->state_lock);

        return 0;
    }
    /*################################### 同时关闭连接 #######################################*/
    // Receiver: [peer], Flags: [FINACK]
    else if(sock->state == FIN_WAIT_1 && data_len == 0 && flags == (FIN_FLAG_MASK | ACK_FLAG_MASK)){

        // 取消 FINACK 计时
        stop_timeout(FINACK_RETRY);

        // 设置窗口
        sock->window.wnd_send->window_size = 1;
        sock->window.wnd_send->baseseq = ack_num;
        sock->window.wnd_send->nextseq = ack_num;
        sock->window.wnd_recv->expect_seq = seq + 1;

        // 发送ACK
        send_flags(sock, (ACK_FLAG_MASK));
        printf("%s(peer) sent ACK!\n", hostname);
        set_timeout(3, ACK_RETRY);

        // CLOSING 特殊状态
        pthread_mutex_lock(&sock->state_lock);
        sock->state = CLOSING;
        pthread_mutex_unlock(&sock->state_lock);

        return 0;
    }
    // Receiver: [peer], Flags: [ACK]
    else if(sock->state == CLOSING && data_len == 0 && flags == ACK_FLAG_MASK){
        stop_timeout(ACK_RETRY);
        pthread_mutex_lock(&sock->state_lock);
        sock->state = TIME_WAIT;
        pthread_mutex_unlock(&sock->state_lock);
        return 0;
    /*################################### 连接管理结束 #######################################*/
    }
    /*################################# 数据发送端处理ACK #####################################*/
    else if(sock->state == ESTABLISHED && data_len == 0 && flags == ACK_FLAG_MASK){
        int ack_quantity = ack_num - sock->window.wnd_send->baseseq;
        int buf_size = sock->sending_len;
        // 更新 base, baseseq
        if(sock->window.wnd_send->baseseq < ack_num){
            sock->window.wnd_send->used -= ack_quantity;
            sock->window.wnd_send->base = (sock->window.wnd_send->base + ack_quantity) % buf_size;
            sock->window.wnd_send->baseseq = ack_num;  // 累计确认
        }
        return 0;
    }

    // 返回值非0，说明需要加锁失败，要一直尝试加锁
    while(pthread_mutex_lock(&(sock->recv_lock)) != 0); // 加锁

    // 缓冲区仅仅复制接收窗口大小的数据
    uint16_t win_size = sock->window.wnd_recv->window_size;
    uint32_t buf_size = sock->received_len;
    int expect_index = sock->window.wnd_recv->expect;
    uint32_t putlen = (win_size < data_len)? win_size: data_len;
    
    if(expect_index + putlen <= buf_size){
        memcpy(sock->received_buf + expect_index, pkt + DEFAULT_HEADER_LEN, putlen);
    }
    else{
        int len1 = buf_size - expect_index;
        int len2 = putlen - len1;
        memcpy(sock->received_buf + expect_index, pkt + DEFAULT_HEADER_LEN, len1);
        memcpy(sock->received_buf, pkt + DEFAULT_HEADER_LEN + len1, len2);
    }
    pthread_mutex_unlock(&(sock->recv_lock)); // 解锁

    // 发送 ACK seq
    sock->window.wnd_recv->expect_seq = seq + putlen;
    sock->window.wnd_recv->expect += putlen;  // 将来接收数据在缓冲区中的下标
    send_flags(sock, ACK_FLAG_MASK);
    
    // 通知 recv 用户函数
    pthread_mutex_lock(&(sock->window.wnd_recv->cond_lock));
    sock->window.wnd_recv->has_new_data = 1;
    pthread_cond_broadcast(&(sock->wait_cond));
    pthread_mutex_unlock(&(sock->window.wnd_recv->cond_lock));

    return 0;
}

int tju_close (tju_tcp_t* sock){
    // 获取主机名，便于日志记录
    char hostname[8];
    gethostname(hostname, 8);

    // 主动关闭
    if(sock->state == ESTABLISHED){
        // 发送FIN
        send_flags(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK));
        printf("%s(active) sent FINACK!\n", hostname);
        pthread_mutex_lock(&sock->state_lock);
        sock->state = FIN_WAIT_1;
        pthread_mutex_unlock(&sock->state_lock);
        // 设置 FINACK 超时监测
        set_timeout(3, FINACK_RETRY);

        // 等待接收到 FINACK
        while(sock->state != TIME_WAIT){
            if(retry[FINACK_RETRY]){
                // 重传 FINACK
                send_flags(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK));
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
            if(sock->state == CLOSING && retry[ACK_RETRY]){
                // 同时关闭连接可能需要重传ACK
                send_flags(sock, ACK_FLAG_MASK);
                set_timeout(3, ACK_RETRY);
                printf("%s(peer) retransimit ACK!\n", hostname);
            }
        }

        // 等待 2 * MSL
        sleep(2 * MSL);
        
        // 关闭
        pthread_mutex_lock(&sock->state_lock);
        sock->state = CLOSED;
        pthread_mutex_unlock(&sock->state_lock);
        printf("%s(active) closed!\n", hostname);
    }
    // 被动关闭
    else{
        while(sock->state != CLOSED){
            if(retry[ACK_RETRY]){
                // 重传 ACK
                send_flags(sock, ACK_FLAG_MASK);
                set_timeout(3, ACK_RETRY);
                printf("%s(passive) retransimit ACK!\n", hostname);
            }
            if(retry[FINACK_RETRY]){
                // 重传 FINACK
                send_flags(sock, (FIN_FLAG_MASK | ACK_FLAG_MASK));
                set_timeout(3, FINACK_RETRY);
                printf("%s(passive) retransimit FINACK!\n", hostname);
            }
        }
        printf("%s(passive) closed!\n", hostname);
    }
    
    return 0;
}

/*
* ############################## 辅助函数,用户不应该调用 ##############################
*/

// 用于单独发送三次握手的flags
static int send_flags(tju_tcp_t* sock, uint8_t flags){
    char* msg;
    uint16_t plen = DEFAULT_HEADER_LEN;  // 无 Payload
    // 通过窗口获取seq和ack_num，需要提前配置
    uint32_t seq = sock->window.wnd_send->nextseq;
    uint32_t ack_num = sock->window.wnd_recv->expect_seq;
    tju_sock_addr local_addr = sock->established_local_addr;
    tju_sock_addr remote_addr = sock->established_remote_addr;

    // 组成包
    msg = create_packet_buf(local_addr.port, remote_addr.port, seq, ack_num, 
              DEFAULT_HEADER_LEN, plen, flags, 1, 0, NULL, 0);

    // UDP发送
    sendToLayer3(msg, plen);
    return 0;
}

// 释放sock对应的连接资源
static void free_sock(tju_tcp_t* sock){
    int hashval = cal_hash(sock->established_local_addr.ip, sock->established_local_addr.port,
                           sock->established_remote_addr.ip, sock->established_remote_addr.port);
    established_socks[hashval] = NULL;
    free(sock->received_buf);
    free(sock->sending_buf);
    free(sock->window.wnd_recv);
    free(sock->window.wnd_send);
    pthread_mutex_destroy(&(sock->state_lock));
    free(sock);
    destroy_full_conn_queue(hashval);
    destroy_half_conn_queue(hashval);
}

// 发送线程
static void* send_thread(void* arg){
    tju_tcp_t* sock = (tju_tcp_t*)(arg);
    uint32_t seq, ack_num;
    int send_len, used, base_index, next_index;
    int win_size, buf_size, noack_size, nosent_size, max_send_size, send_size;
    char* data, *msg;

    while(1){
        pthread_mutex_lock(&(sock->state_lock));
        if(sock->state != ESTABLISHED){
            pthread_mutex_unlock(&(sock->state_lock));
            break;
        }
        pthread_mutex_unlock(&(sock->state_lock));

        pthread_mutex_lock(&(sock->send_lock));
        seq = sock->window.wnd_send->nextseq;
        ack_num = sock->window.wnd_recv->expect_seq;
        used = sock->window.wnd_send->used;
        base_index = sock->window.wnd_send->base;
        next_index = sock->window.wnd_send->next;
        win_size = sock->window.wnd_send->window_size;
        buf_size = sock->sending_len;

        if(used == 0){
            pthread_mutex_unlock(&(sock->send_lock));
            usleep(100000);
            continue;
        }

        noack_size = (base_index == -1)? 0: (next_index - base_index + buf_size) % buf_size;
        nosent_size = used - noack_size;  // 在缓冲区但是没有发送的数据大小
        max_send_size = ((win_size - noack_size) < nosent_size)? (win_size - noack_size): nosent_size;  // 单次发送最大
        send_size = (max_send_size < SMSS)? max_send_size: SMSS;  // 限制在SMSS范围内

        /* 从缓冲区中取数据 */
        if(send_size != 0 && next_index + send_size < buf_size){
            data = (char*)malloc(send_size * sizeof(char));
            memcpy(data, sock->sending_buf + next_index, send_size);
        }
        else if(send_size != 0 && next_index + send_size >= buf_size){
            int len1 = buf_size - next_index;
            int len2 = next_index + send_size - buf_size;
            data = (char*)malloc(send_size * sizeof(char));
            memcpy(data, sock->sending_buf + next_index, len1);
            memcpy(data + len1, sock->sending_buf, len2);
        }
        else{
            printf("send_size = %d, next_index = %d.\n", send_size, next_index);
            continue;
        }

        /* 发包 */
        msg = create_packet_buf(sock->established_local_addr.port,
                sock->established_remote_addr.port, seq, ack_num,
                DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN + send_size,
                NO_FLAG, 0, 0, data, send_size);
        sendToLayer3(msg, send_size);

        free(data);

        /* 更新窗口 */
        if(sock->window.wnd_send->base == -1){
            sock->window.wnd_send->base = 0;  // 代表已经发出过包
        }
        sock->window.wnd_send->nextseq = seq + send_size;
        sock->window.wnd_send->next = (next_index + send_size) % buf_size;

        pthread_mutex_unlock(&(sock->send_lock));
    }
}

// // 重传线程
// static int retry_thread(void* arg){

// }
