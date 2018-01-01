#pragma once
// facade of hredis connection, impl. to C++xx
// JStatham 2017/12/30
#include <memory>
#include <mutex>
#include <chrono>
#include <map>
#include <stdint.h>
#include <string.h>
#include <string>
#include <vector>
#include <assert.h>
#include <atomic>
#include <iostream>
#ifdef WIN32
#include <winsock.h>
#endif
#ifdef LINUX
#include <sys/time.h>
#endif
#include "hiredis.h"
//模块内宏定义
#define	REDIS_DEFAULT_PORT	(6379)
//故障统计变量
//	连接故障次数
extern std::atomic<size_t>	redis_connection_fail_times;
//用智能指针管理redisReply资源
inline std::shared_ptr<redisReply> make_redis_reply_ptr(redisReply* r)
{
	return std::shared_ptr<redisReply>(r, freeReplyObject);
}

class RedisConnection
{
public:
	const int retry_connect = 2;
	virtual ~RedisConnection()
	{
		Close();
	}
	//执行用户命令，返回redisReply智能指针
	//执行命令失败，redis建议重连；用户重连2次，简化保活设计，增强实时性
	virtual std::shared_ptr<redisReply> Command(const char* fmt, ...)
	{
		va_list ap;
		va_start(ap, fmt);
		auto r = CommandVaList(fmt, ap);
		va_end(ap);
		return r;
	}
	virtual std::shared_ptr<redisReply> CommandVaList(const char* fmt, va_list ap)
	{
		if (!context_)
		{
			return make_redis_reply_ptr(nullptr);
		}

		redisReply* r = (redisReply*)redisvCommand(context_.get(),
			fmt, ap);
		//执行命令失败，redis建议重连；使用调用者的时间重连
		for (int i = 0; r == nullptr && i < retry_connect; ++i)
		{
			++link_fail_times;
			Close();
			Connect();

			if (context_)
			{
				r = (redisReply*)redisvCommand(context_.get(),
					fmt, ap);
			}
		}

		return make_redis_reply_ptr(r);
	}
	virtual std::shared_ptr<redisReply> CommandArgv(int argc, const char **argv, const size_t *argvlen)
	{
		if (!context_)
		{
			return make_redis_reply_ptr(nullptr);
		}

		redisReply* r = (redisReply*)redisCommandArgv(context_.get(),
			argc, argv, argvlen);
		//执行命令失败，redis建议重连；使用调用者的时间重连
		for (int i = 0; r == nullptr && i < retry_connect; ++i)
		{
			++link_fail_times;
			Close();
			Connect();

			if (context_)
			{
				r = (redisReply*)redisCommandArgv(context_.get(),
					argc, argv, argvlen);
			}
		}

		return make_redis_reply_ptr(r);
	}
	virtual bool SetTimeout(int timeout)
	{
		if (!context_)
		{
			return false;
		}

		struct timeval tv { 0, 0 };
		tv.tv_sec = timeout_ / 1000;
		tv.tv_usec = timeout_ * 1000;
		//非经常性操作，重连1次
		if (REDIS_OK != redisSetTimeout(context_.get(), tv))
		{
			Close();
			Connect();

			if (context_)
			{
				if (REDIS_OK == redisSetTimeout(context_.get(), tv))
				{
					timeout_ = timeout;
					return true;
				}
			}
		}

		timeout_ = timeout;
		return true;
	}
	virtual bool KeepAlive()
	{
		if (!context_)
		{
			return false;
		}
		//非经常性操作，重连1次
		if (REDIS_OK != redisEnableKeepAlive(context_.get()))
		{
			Close();
			Connect();

			if (context_)
			{
				return REDIS_OK == redisEnableKeepAlive(context_.get());
			}
		}

		return true;
	}
	/*
	Returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk. This command is often used to test if a connection is still alive, or to measure latency.
	If the client is subscribed to a channel or a pattern, it will instead return a multi-bulk with a "pong" in the first position and an empty bulk in the second position, unless an argument is provided in which case it returns a copy of the argument.

	Return value
	Simple string reply

	Examples
	redis> 
	PING
	"PONG"
	redis> 
	PING "hello world"
	"hello world"
	redis> 
	*/
	virtual bool Ping()
	{
		auto hello = "hello, u there";
		auto r = CommandWithoutRetry("PING %s", hello);

		if (r)
		{
			if (r->type == REDIS_REPLY_STRING)
			{
				return strcmp(hello, r->str) == 0;
			}
		}

		return false;
	}
	virtual void Close()
	{
		context_.reset();
	}
	size_t LinkFails()
	{
		return link_fail_times;
	}
protected:
	//用户不能显式构造，必须用RedisPool创建；
	RedisConnection(std::string host, uint16_t port = REDIS_DEFAULT_PORT,
		std::string password = "", int db = 0, int timeout = 500/*ms*/)
		: host_(host), port_(port), password_(password), db_(db), timeout_(timeout)
	{
		Connect();
	}
	bool Select()
	{
		auto r = CommandWithoutRetry("select %d", db_);

		if (!r)
		{
			return false;
		}

		if (REDIS_REPLY_STATUS != r->type ||
			strcmp("OK", r->str) != 0)
		{
			std::cerr << "select db:" << db_ << " error!" << std::endl;
			return false;
		}

		return true;
	}
	void Connect()
	{
		if (context_)
		{
			return;
		}

		if (timeout_ == -1)
		{
			context_ = std::shared_ptr<redisContext>(
				redisConnect(host_.c_str(), port_), 
				redisFree);
		}
		else
		{
			struct timeval tv{0, 0};
			tv.tv_sec = timeout_ / 1000;
			tv.tv_usec = timeout_ * 1000;
			context_ = std::shared_ptr<redisContext>(
				redisConnectWithTimeout(host_.c_str(), port_, tv), 
				redisFree);
		}

		if (!context_)
		{
			char	err_msg[128] = { 0 };

			sprintf(err_msg, "RedisConnection %s:%d cannot connected!",
				host_.c_str(), port_);
			std::cerr << err_msg << std::endl;
			++redis_connection_fail_times;
			++link_fail_times;
			return;
		}

		if (context_->err != 0)
		{
			std::cerr << "RedisConnection " << host_ << "@" << port_ 
				<<" connected failed!" << std::endl;
			++redis_connection_fail_times;
			++link_fail_times;
			return;
		}

		if (!Auth())
		{
			Close();
			throw std::invalid_argument("auth failed");
			return;
		}

		if (!Select())
		{
			Close();
			return;
		}

		link_fail_times = 0;
	}
	//执行用户命令，返回redisReply智能指针
	std::shared_ptr<redisReply> CommandWithoutRetry(const char* fmt, ...)
	{
		if (!context_)
		{
			return make_redis_reply_ptr(nullptr);
		}

		va_list ap;
		va_start(ap, fmt);
		redisReply* r = (redisReply*)redisvCommand(context_.get(),
			fmt, ap);
		va_end(ap);

		if (r == nullptr)
		{
			++link_fail_times;
		}

		return make_redis_reply_ptr(r);
	}
	/*
	AUTH password
	Available since 1.0.0.
	Request for authentication in a password-protected Redis server. Redis can be instructed to require a password before allowing clients to execute commands. This is done using the requirepass directive in the configuration file.
	If password matches the password in the configuration file, the server replies with the OK status code and starts accepting commands. Otherwise, an error is returned and the clients needs to try a new password.
	Note: because of the high performance nature of Redis, it is possible to try a lot of passwords in parallel in very short time, so make sure to generate a strong and very long password so that this attack is infeasible.

	Return value
	Simple string reply
	*/
	bool Auth()
	{
		if (password_.empty())
		{
			return true;
		}

		auto r = CommandWithoutRetry("AUTH %s", password_.c_str());

		if (r)
		{
			if (r->type == REDIS_REPLY_STRING)
			{
				return strcmp("+OK\r\n", r->str) == 0;
			}
		}

		return false;
	}
	std::string HashName()
	{
		return host_ + "@" + std::to_string(port_);
	}
	friend class RedisPool;
private:
	std::string host_;
	uint16_t	port_{ REDIS_DEFAULT_PORT };
	std::string	password_;
	int			db_{ 0 };
	int			timeout_{ -1 };
	std::shared_ptr<redisContext> context_;
	//连接错误次数；连接正常后清零
	size_t		link_fail_times{ 0 };
};

class RedisMutexConnection : public RedisConnection
{
public:
	void Close() override
	{
		std::lock_guard<std::recursive_mutex>	guard(mutex_);
		RedisConnection::Close();
	}
	bool Ping() override
	{
		std::lock_guard<std::recursive_mutex>	guard(mutex_);
		return RedisConnection::Ping();
	}
	bool KeepAlive() override
	{
		std::lock_guard<std::recursive_mutex>	guard(mutex_);
		return RedisConnection::KeepAlive();
	}
	bool SetTimeout(int timeout) override
	{
		std::lock_guard<std::recursive_mutex>	guard(mutex_);
		return RedisConnection::SetTimeout(timeout);
	}
	std::shared_ptr<redisReply> CommandVaList(const char* fmt, va_list ap) override
	{
		std::lock_guard<std::recursive_mutex>	guard(mutex_);
		return RedisConnection::CommandVaList(fmt, ap);
	}
	std::shared_ptr<redisReply> CommandArgv(int argc, const char **argv, const size_t *argvlen) override
	{
		std::lock_guard<std::recursive_mutex>	guard(mutex_);
		return RedisConnection::CommandArgv(argc, argv, argvlen);
	}
protected:
	friend class RedisPool;
	//用户不能显式构造，必须用RedisPool创建；
	RedisMutexConnection(std::string host, uint16_t port = REDIS_DEFAULT_PORT,
		std::string password = "", int db = 0, int timeout = 500/*ms*/)
		: RedisConnection(host, port, password, db, timeout) {}
private:
	std::recursive_mutex	mutex_;
};
//redis链接池
//工厂模式
class RedisPool
{
public:
	static std::shared_ptr<RedisPool> Singleton()
	{
		//防止程序退出过程中，单例已经析构后；
		//Singleton多次无意义构造
		if (singleton_build_counter_ == 0)
		{
			++singleton_build_counter_;
			singleton_.reset(new RedisPool());
		}

		return singleton_;
	}
	std::shared_ptr<RedisConnection> CreateConnection(std::string host, uint16_t port = REDIS_DEFAULT_PORT,
		std::string password = "", int db = 0, int timeout = 500/*ms*/)
	{
		std::lock_guard<std::mutex>	guard(mutex_);		

		if (SizeUnlock() == max_size_)
		{
			std::cerr << "Warnning: redisPool full" << std::endl;
			return std::shared_ptr<RedisConnection>();
		}

		auto p =
			std::shared_ptr<RedisConnection>(new
				RedisConnection(host, port, password, db, timeout));
		//用户请求链路，同时借出
		using_pool_.push_back(p);
		return p;
	}
	std::shared_ptr<RedisConnection> CreateMutexConnection(std::string host, uint16_t port = REDIS_DEFAULT_PORT,
		std::string password = "", int db = 0, int timeout = 500/*ms*/)
	{
		std::lock_guard<std::mutex>	guard(mutex_);

		if (SizeUnlock() == max_size_)
		{
			std::cerr << "Warnning: redisPool full" << std::endl;
			return std::shared_ptr<RedisConnection>();
		}

		auto p =
			std::shared_ptr<RedisConnection>(new
				RedisMutexConnection(host, port, password, db, timeout));
		//用户请求链路，同时借出
		using_pool_.push_back(p);
		return p;
	}
	void CloseConnection(std::shared_ptr<RedisConnection> connection)
	{
		std::lock_guard<std::mutex>	guard(mutex_);

		for (auto n = backup_pool_.begin(); n != backup_pool_.end(); ++ n)
		{
			if (*n == connection)
			{
				connection->Close();
				backup_pool_.erase(n);
				return;
			}
		}

		for (auto n = using_pool_.begin(); n != using_pool_.end(); ++n)
		{
			if (*n == connection)
			{
				connection->Close();
				using_pool_.erase(n);
				return;
			}
		}
	}
	std::shared_ptr<RedisConnection> LoanConnection(std::string host, uint16_t port = REDIS_DEFAULT_PORT)
	{
		std::lock_guard<std::mutex>	guard(mutex_);

		for (auto n = backup_pool_.begin(); n != backup_pool_.end(); ++ n)
		{
			if ((*n)->HashName() == HashName(host, port))
			{
				auto p = *n;
				backup_pool_.erase(n);
				using_pool_.push_back(p);
				return p;
			}
		}

		return std::shared_ptr<RedisConnection>();
	}
	std::shared_ptr<RedisConnection> LoanConnection()
	{
		std::lock_guard<std::mutex>	guard(mutex_);

		if (backup_pool_.size() > 0)
		{
			auto p = backup_pool_.back();
			backup_pool_.pop_back();
			using_pool_.push_back(p);
			return p;
		}

		return std::shared_ptr<RedisConnection>();
	}
	//归还后，入参失效
	void ReturnConnection(std::shared_ptr<RedisConnection> &c)
	{
		std::lock_guard<std::mutex>	guard(mutex_);
		//用户重复归还连接
		if (IsBackup(c))
		{
			c.reset();
			return;
		}

		for (auto n = using_pool_.begin(); n != using_pool_.end(); ++n)
		{
			if ((*n) == c)
			{
				using_pool_.erase(n);
				backup_pool_.push_back(c);
				c.reset();
				break;
			}
		}		
	}
	//归还后，入参失效
	void ReturnConnection(RedisConnection *&c)
	{
		std::lock_guard<std::mutex>	guard(mutex_);
		//用户重复归还连接
		if (IsBackup(c))
		{
			c = nullptr;
			return;
		}

		for (auto n = using_pool_.begin(); n != using_pool_.end(); ++n)
		{
			if ((*n).get() == c)
			{
				backup_pool_.push_back(*n);
				using_pool_.erase(n);
				c = nullptr;
				break;
			}
		}
	}
	size_t Size()
	{
		std::lock_guard<std::mutex>	guard(mutex_);
		return SizeUnlock();
	}
	size_t BackupSize()
	{
		std::lock_guard<std::mutex>	guard(mutex_);
		return backup_pool_.size();
	}
	size_t UsingSize()
	{
		std::lock_guard<std::mutex>	guard(mutex_);
		return using_pool_.size();
	}
	void Clear()
	{
		std::lock_guard<std::mutex>	guard(mutex_);
		using_pool_.clear();
		backup_pool_.clear();
	}
	~RedisPool()
	{
		Clear();
	}
	//用户可以自定义保活措施；比如定时轮询，恢复链路
	//如果存在失效链路，则返回false；如全部链路正常，返回true
	//注意：链路产生link fails才认为需要保活，降低开销
	bool TryKeepUsingConnectionAlive()
	{
		std::lock_guard<std::mutex>	guard(mutex_);
		bool is_alive = true;

		for (auto c : using_pool_)
		{
			//链路产生link fails才认为需要保活；降低开销
			if (c->LinkFails() > 0)
			{
				is_alive = false;
				c->Close();
				c->Connect();
			}
		}

		return is_alive;
	}
	//用户获取链接池有效链接数目
	//认为link fails是链路异常，降低开销，不用实时的Ping
	size_t UsingConnectionAliveSize()
	{
		std::lock_guard<std::mutex>	guard(mutex_);
		size_t alive = 0;

		for (auto c : using_pool_)
		{
			if (c->LinkFails() == 0)
			{
				++ alive;
			}
		}

		return alive;
	}
protected:
	inline size_t SizeUnlock()
	{
		return backup_pool_.size() + using_pool_.size();
	}
	RedisPool(size_t max_size = 10) : max_size_(max_size) {}
	std::string HashName(std::string host, uint16_t port)
	{
		return host + "@" + std::to_string(port);
	}
	bool IsBackup(std::shared_ptr<RedisConnection> c)
	{
		for (auto &n : backup_pool_)
		{
			if (n == c)
			{
				return true;
			}
		}

		return false;
	}
	bool IsBackup(RedisConnection *c)
	{
		for (auto &n : backup_pool_)
		{
			if (n.get() == c)
			{
				return true;
			}
		}

		return false;
	}
private:
	//创建的链路空闲情况（归还）下，存备份池
	std::vector<std::shared_ptr<RedisConnection> >	backup_pool_;
	//用户申请的（创建、借用）情况下，存'在用'池
	std::vector<std::shared_ptr<RedisConnection> >	using_pool_;
	//连接池最大规模
	size_t								max_size_;
	//单例智能指针
	static std::shared_ptr<RedisPool>	singleton_;
	//连接池互斥锁
	std::mutex							mutex_;
	//单例构造次数，程序内仅允许构造一次
	static	std::atomic_int				singleton_build_counter_;
};

std::shared_ptr<RedisConnection> AllocateRedisContext(std::string host, uint16_t port);
void FreeRedisContext(RedisConnection *&);
extern "C" void RedisPoolKeepUsingConnectionAlive();
