#include "queue.h"

conn_queue* half_conn_queue[MAX_SOCK] = {NULL};  // 半连接队列
conn_queue* full_conn_queue[MAX_SOCK] = {NULL};  // 全连接队列

/*
############################## 全连接队列辅助函数 ######################################
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
########################半连接队列处理函数#######################################
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
