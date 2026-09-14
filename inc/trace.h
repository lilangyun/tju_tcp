#ifndef _TRACE_H_
#define _TRACE_H_

#include <stdint.h>

/* 根据主机名初始化 client/server trace 文件；进程启动时命令行覆盖。 */
void trace_init(void);
void trace_close(void);

void trace_send(uint32_t seq, uint32_t ack, uint8_t flags, uint32_t length);
void trace_recv(uint32_t seq, uint32_t ack, uint8_t flags, uint32_t length);
void trace_cwnd(int type, uint32_t size);
void trace_rwnd(uint32_t size);
void trace_swnd(uint32_t size);
void trace_deliver(uint32_t seq, uint32_t size);
void trace_rtt(uint64_t sample_us,
               uint64_t estimated_us,
               uint64_t deviation_us,
               uint64_t timeout_us);

#endif
