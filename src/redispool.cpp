#include "redispool.h"
//初始化单例变量以及本地变量
std::shared_ptr<RedisPool>	RedisPool::singleton_{ nullptr };
std::atomic_int				RedisPool::singleton_build_counter_{ 0 };
std::atomic<size_t>			redis_connection_fail_times{ 0 };
//RedisPool连接池保活
//为降低开销，仅对在用的连接保活
void RedisPoolKeepUsingConnectionAlive()
{
	if (!RedisPool::Singleton())
	{
		return;
	}
	//1. 没有连接问题？
	if (redis_connection_fail_times == 0)
	{
		return;
	}
	//2. 有连接问题，尝试修复
	redis_connection_fail_times = 0;
	RedisPool::Singleton()->TryKeepUsingConnectionAlive();
	//3. 仍然存在连接问题？
	if (redis_connection_fail_times != 0)
	{
		//TODO: warnning
		std::cerr << "Warnning: RedisPool alive:"
			<< RedisPool::Singleton()->UsingConnectionAliveSize()
			<< std::endl;
	}
	else
	{
		//通知其它持久化层，链路已经修复；如SQLite
	}
}

std::shared_ptr<RedisConnection> AllocateRedisContext(std::string host, uint16_t port)
{
	if (!RedisPool::Singleton())
	{
		return std::shared_ptr<RedisConnection>();
	}
	//借用连接
	auto p = RedisPool::Singleton()->LoanConnection(host, port);

	if (!p)
	{
		//创建连接
		p = RedisPool::Singleton()->CreateConnection(host, port);
	}
	
	if (p)
	{
		//返回实际连接的智能指针，离开作用域后，没有引用则FreeRedisContext
		return std::shared_ptr<RedisConnection>(p.get(), FreeRedisContext);
	}
	else
	{
		//返回空连接的引用
		return std::shared_ptr<RedisConnection>();
	}
}

void FreeRedisContext(RedisConnection *&p)
{
	if (RedisPool::Singleton())
	{
		RedisPool::Singleton()->ReturnConnection(p);
	}
	else
	{
		//单例已经析构，直接销毁RedisConnection
		if (p != nullptr)
		{
			delete p;
			p = nullptr;
		}
	}
}
