#include "trace.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#define TRACE_PATH_SIZE 128
#define TRACE_INFO_SIZE 256

static FILE* trace_file = NULL;
static pthread_mutex_t trace_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_once_t trace_once = PTHREAD_ONCE_INIT;

static int64_t trace_timestamp_us(void){
    struct timeval now;
    gettimeofday(&now, NULL);
    return (int64_t)now.tv_sec * 1000000LL + (int64_t)now.tv_usec;
}

static FILE* trace_open(const char* filename){
    char path[TRACE_PATH_SIZE];
    FILE* file;

    snprintf(path, sizeof(path), "/vagrant/tju_tcp/test/%s", filename);
    file = fopen(path, "w");
    if(file != NULL){
        return file;
    }

    snprintf(path, sizeof(path), "test/%s", filename);
    file = fopen(path, "w");
    if(file != NULL){
        return file;
    }

    return fopen(filename, "w");
}

static void trace_init_once(void){
    char hostname[64];
    const char* filename;

    if(gethostname(hostname, sizeof(hostname)) != 0){
        return;
    }
    hostname[sizeof(hostname) - 1] = '\0';

    if(strcmp(hostname, "server") == 0){
        filename = "server.event.trace";
    }
    else if(strcmp(hostname, "client") == 0){
        filename = "client.event.trace";
    }
    else{
        return;
    }

    trace_file = trace_open(filename);
    if(trace_file != NULL){
        atexit(trace_close);
    }
}

void trace_init(void){
    pthread_once(&trace_once, trace_init_once);
}

void trace_close(void){
    pthread_mutex_lock(&trace_lock);
    if(trace_file != NULL){
        fclose(trace_file);
        trace_file = NULL;
    }
    pthread_mutex_unlock(&trace_lock);
}

static void trace_write(const char* event, const char* info){
    pthread_mutex_lock(&trace_lock);
    if(trace_file != NULL){
        fprintf(trace_file, "[%lld] [%s] [%s]\n",
                (long long)trace_timestamp_us(), event, info);
        fflush(trace_file);
    }
    pthread_mutex_unlock(&trace_lock);
}

void trace_send(uint32_t seq, uint32_t ack, uint8_t flags, uint32_t length){
    char info[TRACE_INFO_SIZE];
    snprintf(info, sizeof(info), "seq:%u ack:%u flag:%u length:%u",
             seq, ack, flags, length);
    trace_write("SEND", info);
}

void trace_recv(uint32_t seq, uint32_t ack, uint8_t flags, uint32_t length){
    char info[TRACE_INFO_SIZE];
    snprintf(info, sizeof(info), "seq:%u ack:%u flag:%u length:%u",
             seq, ack, flags, length);
    trace_write("RECV", info);
}

void trace_cwnd(int type, uint32_t size){
    char info[TRACE_INFO_SIZE];
    snprintf(info, sizeof(info), "type:%d size:%u", type, size);
    trace_write("CWND", info);
}

void trace_rwnd(uint32_t size){
    char info[TRACE_INFO_SIZE];
    snprintf(info, sizeof(info), "size:%u", size);
    trace_write("RWND", info);
}

void trace_swnd(uint32_t size){
    char info[TRACE_INFO_SIZE];
    snprintf(info, sizeof(info), "size:%u", size);
    trace_write("SWND", info);
}

void trace_deliver(uint32_t seq, uint32_t size){
    char info[TRACE_INFO_SIZE];
    snprintf(info, sizeof(info), "seq:%u size:%u", seq, size);
    trace_write("DELV", info);
}

void trace_rtt(uint64_t sample_us,
               uint64_t estimated_us,
               uint64_t deviation_us,
               uint64_t timeout_us){
    char info[TRACE_INFO_SIZE];
    snprintf(info, sizeof(info),
             "SampleRTT:%f EstimatedRTT:%f DeviationRTT:%f TimeoutInterval:%f",
             (double)sample_us / 1000.0,
             (double)estimated_us / 1000.0,
             (double)deviation_us / 1000.0,
             (double)timeout_us / 1000.0);
    trace_write("RTTS", info);
}
