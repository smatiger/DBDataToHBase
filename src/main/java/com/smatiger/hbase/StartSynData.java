package com.smatiger.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smatiger.hbase.handler.HBaseHandler;

/**
 * @ClassName: StartSynData
 * @Description: 开始同步数据
 * @author wangxiaohu wsmalltiger@163.com
 * @date 2015年4月23日 上午10:45:44
 */
public class StartSynData {
	private static Logger logger = LoggerFactory.getLogger(StartSynData.class);

	public static void main(String[] args) {
		StartSynData.start(args);
	}

	public static void start(String[] args) {
		String errMsg = "参数格式错误：[hbase.table.yaml, 如果表存在是否先删除表：默认false]";
		if (args == null || args.length < 1) {
			System.out.println(errMsg);
			return;
		}
		try {
			HBaseHandler hbaseHandler = new HBaseHandler(args[0]);
			hbaseHandler.createTable(args[0]);
		} catch (Exception e) {
			logger.error(null, e);
		}
		logger.error("执行结束!");
	}
}
