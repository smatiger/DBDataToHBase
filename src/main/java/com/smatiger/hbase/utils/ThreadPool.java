package com.smatiger.hbase.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.smatiger.hbase.contants.HBaseContants;

/**
 * @ClassName: ThreadPool
 * @Description: 线程池
 * @author wangxiaohu wsmalltiger@163.com
 * @date 2015年4月22日 下午2:16:41
 */
public final class ThreadPool extends ThreadPoolExecutor {
	private static BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
	private static ThreadPool threadPool = new ThreadPool(
			HBaseContants.LOAD_DBDATA_TO_HBASE_THREAD_POOL_MAX, 100, 10000,
			TimeUnit.MILLISECONDS, queue);

	private ThreadPool(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				new ThreadPoolExecutor.CallerRunsPolicy());
	}

	public static ThreadPool getInstance() {
		return threadPool;
	}

	@Override
	public void execute(Runnable command) {
		super.execute(command);
	}
}
