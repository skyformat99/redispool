#include <stdio.h>
#include "gtest.h"
#include "gmock.h"
#include "redispool_benchmark.h"

GTEST_API_ int main(int argc, char **argv)
{
#if ENABLE_UT
	testing::InitGoogleTest(&argc, argv);
	testing::InitGoogleMock(&argc, argv);
	//TODO:test under
	return RUN_ALL_TESTS();
#else
	redisPoolBenchmark("localhost", 6379, 1000000);
#endif
}
