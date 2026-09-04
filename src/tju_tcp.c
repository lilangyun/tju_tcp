#include "tju_tcp.h"

/*
创建 TCP socket 
初始化对应的结构体
设置初始状态为 CLOSED
*/
tju_tcp_t* tju_socket(){
    tju_tcp_t* sock = (tju_tcp_t*)malloc(sizeof(tju_tcp_t));
    sock->state = CLOSED;
    
    pthread_mutex_init(&(sock->send_lock), NULL);
    sock->sending_buf = NULL;
    sock->sending_len = 0;

    pthread_mutex_init(&(sock->recv_lock), NULL);
    sock->received_buf = NULL;
    sock->received_len = 0;
    
    if(pthread_cond_init(&sock->wait_cond, NULL) != 0){
        perror("ERROR condition variable not set\n");
        exit(-1);
    }

    sock->window.wnd_recv = NULL;
    sock->window.wnd_recv = NULL;

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
    init_full_conn_queue(hashval);
    init_half_conn_queue(hashval);
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
        // 阻塞等待全连接队列非空
    }
    // 从全连接队列中出队
    conn_node* new_conn_node = pop_full_conn_queue(hashval);
    tju_tcp_t* new_conn = new_conn_node->sock;

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

    sock->established_remote_addr = target_addr;

    tju_sock_addr local_addr;
    local_addr.ip = inet_network(CLIENT_IP);
    local_addr.port = 5678; // 连接方进行connect连接的时候 内核中是随机分配一个可用的端口
    sock->established_local_addr = local_addr;

    // 三次握手
    // 发送SYN
    char* msg;
    uint32_t seq = 464;  // 应改为随机值
    uint16_t plen = DEFAULT_HEADER_LEN;  // 无payload
    msg = create_packet_buf(local_addr.port, target_addr.port, seq, 0, 
              DEFAULT_HEADER_LEN, plen, SYN_FLAG_MASK, 1, 0, NULL, 0);
    // 放入listen_socks，用于发送SYN后接收flags
    int hashval = cal_hash(local_addr.ip, local_addr.port, 0, 0);
    listen_socks[hashval] = sock;
    sendToLayer3(msg, plen);
    sock->state = SYN_SENT;
    printf("Client sent SYN!\n");

    // 接收SYN + ACK
    while (sock->state != ESTABLISHED){
        // 阻塞
    }

    /* 
    这里也不能直接建立连接 需要经过三次握手
    实际在linux中 connect调用后 会进入一个while循环
    循环跳出的条件是socket的状态变为ESTABLISHED 表面看上去就是 正在连接中 阻塞
    而状态的改变在别的地方进行 在我们这就是tju_handle_packet
    sock->state = ESTABLISHED;
    */

    return 0;
}

int tju_send(tju_tcp_t* sock, const void *buffer, int len){
    // 这里当然不能直接简单地调用sendToLayer3
    char* data = malloc(len);
    memcpy(data, buffer, len);

    char* msg;
    uint32_t seq = 464;
    uint16_t plen = DEFAULT_HEADER_LEN + len;

    msg = create_packet_buf(sock->established_local_addr.port, sock->established_remote_addr.port, seq, 0, 
              DEFAULT_HEADER_LEN, plen, NO_FLAG, 1, 0, data, len);

    sendToLayer3(msg, plen);
    
    return 0;
}
int tju_recv(tju_tcp_t* sock, void *buffer, int len){
    while(sock->received_len<=0){
        // 阻塞
    }

    while(pthread_mutex_lock(&(sock->recv_lock)) != 0); // 加锁

    // read_len = min(received_len, len)
    int read_len = 0;
    if (sock->received_len >= len){ // 从中读取len长度的数据
        read_len = len;
    }else{
        read_len = sock->received_len; // 读取sock->received_len长度的数据(全读出来)
    }

    memcpy(buffer, sock->received_buf, read_len);

    if(read_len < sock->received_len) { // 还剩下一些
        char* new_buf = malloc(sock->received_len - read_len);
        memcpy(new_buf, sock->received_buf + read_len, sock->received_len - read_len);
        free(sock->received_buf);
        sock->received_len -= read_len;
        sock->received_buf = new_buf;
    }else{
        free(sock->received_buf);
        sock->received_buf = NULL;
        sock->received_len = 0;
    }
    pthread_mutex_unlock(&(sock->recv_lock)); // 解锁

    return 0;
}

// 把内核收到的数据放到sock->receive_buf，内核可使用该函数传递内核缓冲区收到的packet
int tju_handle_packet(tju_tcp_t* sock, char* pkt){
    // 四元组
    tju_sock_addr local_addr, remote_addr;
    remote_addr.ip = inet_network(CLIENT_IP);
    remote_addr.port = get_src(pkt);
    local_addr.ip = sock->bind_addr.ip;
    local_addr.port = get_dst(pkt);

    uint32_t data_len = get_plen(pkt) - DEFAULT_HEADER_LEN;
    uint8_t flags = get_flags(pkt);
    uint32_t seq = get_seq(pkt);
    uint32_t ack_num = get_ack(pkt);

    /*
    ##########################三次握手状态机##################################
    */ 
    // Receiver: [server], Flags: [SYN]
    if(data_len == 0 && flags == SYN_FLAG_MASK){
        // 新建socket: new_conn
        tju_tcp_t* new_conn = (tju_tcp_t*)malloc(sizeof(tju_tcp_t));
        memcpy(new_conn, sock, sizeof(tju_tcp_t));
        new_conn->established_local_addr.port = local_addr.port + 1;  // 这里也应该改为随机选取
        new_conn->established_local_addr.ip = local_addr.ip;
        new_conn->established_remote_addr = remote_addr;
        uint32_t server_isn = 464;  // 后续应随机化
        uint32_t client_isn = seq;

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
        send_flags(new_conn, server_isn, client_isn + 1,
                   new_conn->established_local_addr,
                   new_conn->established_remote_addr,
                   SYN_FLAG_MASK | ACK_FLAG_MASK);
        new_conn->state = SYN_RECV;
        printf("Server sent SYNACK!\n");
    }
    // Receiver: [client], Flags: [SYN + ACK]
    if(data_len == 0 && flags == (SYN_FLAG_MASK | ACK_FLAG_MASK)){
        uint32_t server_isn = seq;
        uint32_t client_isn = ack_num;
        // 更换连接的远程地址
        sock->established_remote_addr = remote_addr;
        
        // 发送ACK
        send_flags(sock, client_isn, server_isn + 1, local_addr, remote_addr, ACK_FLAG_MASK);
        sock->state = ESTABLISHED;
        printf("Client sent ACK!\n");

        // 从listen_socks哈希表中取消
        int hashval = cal_hash(local_addr.ip, local_addr.port, 0, 0);
        listen_socks[hashval] = NULL;

        // 将建立了连接的socket放入内核 已建立连接哈希表中
        int conn_hashval = cal_hash(local_addr.ip, 
                                    local_addr.port, 
                                    remote_addr.ip, 
                                    remote_addr.port);
        established_socks[conn_hashval] = sock;
    }
    // Receiver: [server], Flags: [ACK]
    if(data_len == 0 && flags == ACK_FLAG_MASK){
        // 3. 接收 ACK
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
        new_conn->state = ESTABLISHED;
    }
    /*
    ##########################################################################
    */ 

    // 返回值非0，说明需要加锁失败，要一直尝试加锁
    while(pthread_mutex_lock(&(sock->recv_lock)) != 0); // 加锁

    if(sock->received_buf == NULL){
        sock->received_buf = malloc(data_len);
    }else {
        sock->received_buf = realloc(sock->received_buf, sock->received_len + data_len);
    }
    // 这里脱下头部，只复制数据
    memcpy(sock->received_buf + sock->received_len, pkt + DEFAULT_HEADER_LEN, data_len);
    sock->received_len += data_len;

    pthread_mutex_unlock(&(sock->recv_lock)); // 解锁

    return 0;
}

int tju_close (tju_tcp_t* sock){
    return 0;
}


// ### 辅助函数,用户不应该调用 ### //
static int send_flags(tju_tcp_t* sock,
                      uint32_t seq,
                      uint32_t ack_num,
                      tju_sock_addr local_addr,
                      tju_sock_addr remote_addr,
                      uint8_t flags){
    char* msg;
    uint16_t plen = DEFAULT_HEADER_LEN;  // 无payload
    msg = create_packet_buf(local_addr.port, remote_addr.port, seq, ack_num, 
              DEFAULT_HEADER_LEN, plen, flags, 1, 0, NULL, 0);
    sendToLayer3(msg, plen);
    return 0;
}

/*
######################## 全连接队列辅助函数 ############################## //
*/
// 初始化队列
static int init_full_conn_queue(int listen_hashval){
    if(listen_hashval < 0){
        printf("[init_full_conn_queue]: listen_hashval is %d < 0 error!\n", listen_hashval);
        exit(1);
    }
    printf("start full init.\n");
    full_conn_queue[listen_hashval] = (conn_queue*)(malloc(sizeof(conn_queue)));
    // full_conn_queue[listen_hashval]->front =  (conn_node*)(malloc(sizeof(conn_node)));
    // full_conn_queue[listen_hashval]->rear = (conn_node*)(malloc(sizeof(conn_node)));
    full_conn_queue[listen_hashval]->lock = (pthread_mutex_t*)(malloc(sizeof(pthread_mutex_t)));
    full_conn_queue[listen_hashval]->front = NULL;
    full_conn_queue[listen_hashval]->front = NULL;
    // printf("before full lock.\n");
    pthread_mutex_init(full_conn_queue[listen_hashval]->lock, NULL);
    printf("finish full init.\n");
    return 0;
}

// 创建节点
static conn_node* create_conn_node(tju_tcp_t *new_conn){
    conn_node* new_conn_node = (conn_node*)(malloc(sizeof(conn_node)));
    new_conn_node->sock = new_conn;
    new_conn_node->next = NULL;
    return new_conn_node;
}

// 入队
static int push_full_conn_queue(int listen_hashval, conn_node* new_conn_node){
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
static conn_node* pop_full_conn_queue(int listen_hashval){
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
static int destroy_full_conn_queue(int listen_hashval){
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
    return 0;
}

// 判断非空
static int empty_full_conn_queue(int listen_hashval){
    if(listen_hashval < 0){
        printf("[empty_conn_queue]: listen_hashval < 0 error!\n");
        exit(1);
    }
    conn_node* head = full_conn_queue[listen_hashval]->front;
    return head ==  NULL;
}
/*
########################半连接队列处理函数#######################################
*/
// 初始化队列
static int init_half_conn_queue(int listen_hashval){
    if(listen_hashval < 0){
        printf("[init_conn_queue]: listen_hashval < 0 error!\n");
        exit(1);
    }
    half_conn_queue[listen_hashval] = (conn_queue*)(malloc(sizeof(conn_queue)));
    // half_conn_queue[listen_hashval]->front =  (conn_node*)(malloc(sizeof(conn_node)));
    // half_conn_queue[listen_hashval]->rear = (conn_node*)(malloc(sizeof(conn_node)));
    half_conn_queue[listen_hashval]->lock = (pthread_mutex_t*)(malloc(sizeof(pthread_mutex_t)));
    half_conn_queue[listen_hashval]->front =  NULL;
    half_conn_queue[listen_hashval]->rear = NULL;
    pthread_mutex_init(half_conn_queue[listen_hashval]->lock, NULL);
    return 0;
}

// 入队
static int push_half_conn_queue(int listen_hashval, conn_node* new_conn_node){
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
static conn_node* pop_half_conn_queue(int listen_hashval){
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
static int destroy_half_conn_queue(int listen_hashval){
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
    return 0;
}

// 判断非空
static int empty_half_conn_queue(int listen_hashval){
    if(listen_hashval < 0){
        printf("[empty_conn_queue]: listen_hashval < 0 error!\n");
        exit(1);
    }
    conn_node* head = half_conn_queue[listen_hashval]->front;
    return head ==  NULL;
}

// 移除指定节点
static conn_node* remove_half_conn_queue(int listen_hashval, int conn_hashval){
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
        printf("node_hash: %d, conn_hash: %d.\n", hashval, conn_hashval);
        if(hashval == conn_hashval){
            // match

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
