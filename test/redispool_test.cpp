#include "gtest.h"
#include "gmock.h"
#include "redispool.h"
TEST(RedisPool, FunctionTest)
{
	auto pool = RedisPool::Singleton();
	pool->Clear();
	auto c = pool->CreateConnection("localhost");
	ASSERT_EQ(pool->Size(), 1);
	ASSERT_TRUE(c->Ping());
	ASSERT_TRUE(c->KeepAlive());
	ASSERT_TRUE(c->SetTimeout(300));
	ASSERT_TRUE(c->Ping());
	auto r = c->Command("SET abc %s", "hello");
	ASSERT_STREQ(r->str, "OK");
	r = c->Command("GET abc");
	ASSERT_STREQ(r->str, "hello");
	r = c->Command("DEL abc");
	ASSERT_EQ(r->integer, 1);
	auto e = pool->CreateMutexConnection("localhost");
	ASSERT_EQ(pool->Size(), 2);
	r = e->Command("SET abc %s", "hello");
	ASSERT_STREQ(r->str, "OK");
	r = e->Command("GET abc");
	ASSERT_STREQ(r->str, "hello");
	r = e->Command("DEL abc");
	ASSERT_EQ(r->integer, 1);
	ASSERT_EQ(pool->UsingConnectionAliveSize(), 2);
	ASSERT_TRUE(pool->TryKeepUsingConnectionAlive());
	ASSERT_EQ(pool->BackupSize(), 0);
	pool->ReturnConnection(e);
	ASSERT_FALSE(e);
	ASSERT_EQ(pool->BackupSize(), 1);
	ASSERT_EQ(pool->UsingConnectionAliveSize(), 1);
	//测试超时
	auto start = std::chrono::high_resolution_clock::now();
	pool->CreateConnection("192.168.1.1");
	auto elapse = std::chrono::duration_cast<std::chrono::milliseconds>
		(std::chrono::high_resolution_clock::now() - start);
	ASSERT_TRUE(elapse.count() >= 500);
}
