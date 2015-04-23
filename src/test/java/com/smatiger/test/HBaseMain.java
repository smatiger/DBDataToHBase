package com.smatiger.test;

import com.smatiger.hbase.StartSynData;

/**
 * @ClassName: HBaseMain
 * @Description: hbase工具
 * @author wangxiaohu wsmalltiger@163.com
 * @date 2015年4月21日 下午8:25:54
 */
public class HBaseMain {

	public static void main(String[] args) {
		//执行前请配置 HBASE_HOME和HADOOP_HOME环境变量
		System.setProperty("hadoop.home.dir", "E:/hadoop/hadoop-2.6.0");
		//		try {
		//			//删除hbase中的表
		//			HBaseUtils.getInstance("hbase.table.yaml").dropTable(
		//					"propertydomain");
		//			HBaseUtils.getInstance("hbase.table.yaml").dropTable(
		//					"scgrid:organizations");
		//		} catch (Exception e) {
		//			e.printStackTrace();
		//		}
		args = new String[] { "hbase.table.yaml" };
		StartSynData.start(args);
		//		try {
		//			//count统计表中的数据量（性能差，待优化）
		//			HBaseUtils.getInstance("hbase.table.yaml").queryAll(
		//					"propertydomain");
		//			HBaseUtils.getInstance("hbase.table.yaml").queryAll(
		//					"scgrid:organizations");
		//		} catch (Exception e) {
		//			e.printStackTrace();
		//		}
	}
}
