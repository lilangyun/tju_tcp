#include "kernel.h"
#include "trace.h"

tju_tcp_t* listen_socks[MAX_SOCK];
tju_tcp_t* established_socks[MAX_SOCK];
int BACKEND_UDPSOCKET_ID;
/*
模拟Linux内核收到一份TCP报文的处理函数
*/
void onTCPPocket(char* pkt){
    // 当我们收到TCP包时 包中 源IP 源端口 是发送方的 也就是我们眼里的 远程(remote) IP和端口
    uint16_t remote_port = get_src(pkt);
    uint16_t local_port = get_dst(pkt);
    // remote/local IP 从数据包解析；仿真环境直接根据 hostname 判断。

    char hostname[8];
    gethostname(hostname, 8);
    uint32_t remote_ip, local_ip;  // 选择 ip
    if(strcmp(hostname,"server")==0){ // 自己是服务端 远端就是客户端
        // printf("Server receive the packet.\n");
        local_ip = inet_network(SERVER_IP);
        remote_ip = inet_network(CLIENT_IP);
    }else if(strcmp(hostname,"client")==0){ // 自己是客户端 远端就是服务端
        local_ip = inet_network(CLIENT_IP);
        remote_ip = inet_network(SERVER_IP);
    }

    int hash_index;
    // 根据4个ip port 组成四元组 查找有没有已经建立连接的socket
    hash_index = cal_hash(local_ip, local_port, remote_ip, remote_port);

    // 首先查找已经建立连接的socket哈希表
    if (established_socks[hash_index]!=NULL){
        tju_handle_packet(established_socks[hash_index], pkt);
        return;
    }

    // 没有的话再查找监听中的socket哈希表
    hash_index = cal_hash(local_ip, local_port, 0, 0); //监听的socket只有本地监听ip和端口 没有远端
    if (listen_socks[hash_index]!=NULL){
        tju_handle_packet(listen_socks[hash_index], pkt);
        return;
    }

    // 都没找到 丢掉数据包
    printf("找不到能够处理该TCP数据包的socket, 丢弃该数据包\n");
    return;
}



/*
以用户填写的TCP报文为参数
根据用户填写的TCP的目的IP和目的端口,向该地址发送数据报
不可以修改此函数实现
*/
void sendToLayer3(char* packet, int packet_length){
    if (packet_length>MAX_LEN){
        printf("ERROR: 不能发送超过 MAX_LEN 长度的packet, 防止IP层进行分片\n");
        return;
    }

    // 获取hostname 根据hostname 判断是客户端还是服务端
    char hostname[8];
    gethostname(hostname, 8);

    struct sockaddr_in remote_address;
    remote_address.sin_family      = AF_INET;
    remote_address.sin_port        = htons(20218);
    int result;
    if(strcmp(hostname,"server")==0){
        remote_address.sin_addr.s_addr = inet_addr(CLIENT_IP);
        result = sendto(BACKEND_UDPSOCKET_ID, packet, packet_length, 0, (struct sockaddr*)&remote_address, sizeof(remote_address));
    }else if(strcmp(hostname,"client")==0){
        remote_address.sin_addr.s_addr = inet_addr(SERVER_IP);
        result = sendto(BACKEND_UDPSOCKET_ID, packet, packet_length, 0, (struct sockaddr*)&remote_address, sizeof(remote_address));
    }else{
        printf("请不要改动hostname...\n");
        exit(-1);
    }
}

/*
 仿真接收数据线程。
 持续读取监听在 20218 端口的 UDP socket；
 收到完整 TJU_TCP 报文后调用 onTCPPocket()。
*/
void* receive_thread(void* arg){

    char packet_header[DEFAULT_HEADER_LEN];
    char* packet;

    uint32_t packet_length = 0, assembled_bytes = 0, received_bytes = 0;
    int header_bytes;

    struct sockaddr_in source_address;
    int source_address_size = sizeof(source_address);

    while(1) {
        // MSG_PEEK 表示看一眼 不会把数据从缓冲区删除
        header_bytes = recvfrom(BACKEND_UDPSOCKET_ID, packet_header, DEFAULT_HEADER_LEN, MSG_PEEK, (struct sockaddr *)&source_address, &source_address_size);
        // 一旦收到了大于header长度的数据 则接受整个TCP包
        if(header_bytes >= DEFAULT_HEADER_LEN){
            packet_length = get_plen(packet_header);
            packet = malloc(packet_length);
            assembled_bytes = 0;
            while(assembled_bytes < packet_length){ // 接收完整报文后再交给协议处理。
                received_bytes = recvfrom(BACKEND_UDPSOCKET_ID, packet + assembled_bytes, packet_length - assembled_bytes, NO_FLAG, (struct sockaddr *)&source_address, &source_address_size);
                assembled_bytes = assembled_bytes + received_bytes;
            }
            // 通知内核收到一个完整的TCP报文
            onTCPPocket(packet);
            free(packet);
        }
    }
}

/*
 开启仿真, 运行起后台线程

 不论是server还是client
 都创建一个UDP socket 监听在20218端口
 然后创建新线程 不断调用该socket的recvfrom
*/
void startSimulation(){
    trace_init();

    // 对于内核 初始化监听socket哈希表和建立连接socket哈希表
    int index;
    for(index=0;index<MAX_SOCK;index++){
        listen_socks[index] = NULL;
        established_socks[index] = NULL;
    }

    // 获取hostname
    char hostname[8];
    gethostname(hostname, 8);
    // printf("startSimulation on hostname: %s\n", hostname);

    BACKEND_UDPSOCKET_ID = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (BACKEND_UDPSOCKET_ID < 0){
        printf("ERROR opening socket");
        exit(-1);
    }

    // 设置socket选项 SO_REUSEADDR = 1 允许端口复用
    // 意思是 允许绑定本地地址冲突 和 改变了系统对处于TIME_WAIT状态的socket的看待方式
    int option_value = 1;
    setsockopt(BACKEND_UDPSOCKET_ID, SOL_SOCKET, SO_REUSEADDR, (const void *)&option_value , sizeof(int));

    struct sockaddr_in local_address;
    memset(&local_address, 0, sizeof(local_address));
    local_address.sin_family = AF_INET;
    // 监听本机上任何IP端口
    local_address.sin_addr.s_addr = htonl(INADDR_ANY); // INADDR_ANY = 0.0.0.0
    local_address.sin_port = htons((unsigned short)20218);

    if (bind(BACKEND_UDPSOCKET_ID, (struct sockaddr *) &local_address, sizeof(local_address)) < 0){
        printf("ERROR on binding");
        exit(-1);
    }

    pthread_t receiver_thread_id = 1001;
    int result = pthread_create(&receiver_thread_id, NULL, receive_thread, (void*)(&BACKEND_UDPSOCKET_ID));
    if (result<0){
        printf("ERROR open thread");
        exit(-1);
    }
    // printf("successfully created bankend thread\n");
    return;
}

int cal_hash(uint32_t local_ip, uint16_t local_port, uint32_t remote_ip, uint16_t remote_port){
    // 实际上肯定不是这么算的
    return (local_ip + local_port + remote_ip + remote_port)%MAX_SOCK;
}
