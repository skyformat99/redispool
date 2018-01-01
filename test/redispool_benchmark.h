#pragma once
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif
	void	redisPoolBenchmark(char* host, uint16_t port, size_t requests);

#ifdef __cplusplus
}
#endif
