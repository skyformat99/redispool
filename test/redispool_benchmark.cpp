#include "UiConsole.h"
#include "redispool_benchmark.h"
#include "redispool.h"
#include <sstream>
#include <thread>
#include <chrono>
#include <iostream>
#include <vector>

static	UI_TBL	tbl;
static	std::mutex	s_mutex;

static void client_entry(char* host, uint16_t port, size_t requests)
{
	std::ostringstream os;
	os << std::this_thread::get_id();

	try
	{
		//SET
		auto start = std::chrono::high_resolution_clock::now();

		for (size_t n = 0; n < requests; ++n)
		{
			auto c = AllocateRedisContext(host, port);
			auto r = c->Command("SET %s %s", os.str().c_str(), "hello");
			assert(strcmp(r->str, "OK") == 0);
		}

		auto elapse = std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::high_resolution_clock::now() - start);
		double delta = (double)requests / (elapse.count() / 1000);
		//print
		s_mutex.lock();
		InsertUITblEntry(&tbl,
			"%s%s%l%l%d",
			os.str().c_str(),
			"SET",
			(long)requests,
			(long)elapse.count(),
			(int)delta);
		s_mutex.unlock();
		//GET
		start = std::chrono::high_resolution_clock::now();

		for (size_t n = 0; n < requests; ++n)
		{
			auto c = AllocateRedisContext(host, port);
			auto r = c->Command("GET %s", os.str().c_str());
			assert(strcmp(r->str, "hello") == 0);
		}

		elapse = std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::high_resolution_clock::now() - start);
		delta = (double)requests / (elapse.count() / 1000);
		//print
		s_mutex.lock();
		InsertUITblEntry(&tbl,
			"%s%s%l%l%d",
			os.str().c_str(),
			"GET",
			(long)requests,
			(long)elapse.count(),
			(int)delta);
		s_mutex.unlock();
	}
	catch (std::exception &e)
	{
		std::cerr << e.what() << std::endl;
	}
	catch (...)
	{
		std::cerr << "unknow exception" << std::endl;
	}
}

void	redisPoolBenchmark(char* host, uint16_t port, size_t requests)
{
	StartUITbl(&tbl, UI_DEF_SET3, "redis pool benchmark",
		"thread|mode|requests|time(ms)|REQ/s");
	auto t = std::shared_ptr<std::thread>(
		new std::thread(client_entry, host, port, requests));
	t->join();
	EndUITbl(&tbl);
}
