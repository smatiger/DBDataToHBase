package com.smatiger.hbase.utils;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smatiger.hbase.contants.HBaseContants;

/**
 * @ClassName: HBaseUtils
 * @Description: 工具类
 * @author wangxiaohu wsmalltiger@163.com
 * @date 2015年4月22日 上午11:01:22
 */
public class HBaseUtils {
	private static Logger logger = LoggerFactory.getLogger(HBaseUtils.class);

	private static HBaseUtils hbaseUtils = null;

	private static Configuration configuration = null;
	public static HBaseAdmin hbaseAdmin = null;
	private static HConnection hconnection = null;

	private HBaseUtils(String confPath) throws Exception {
		configuration = HBaseConfigurationUtils.getConfiguration(confPath);
		hconnection = HConnectionManager.createConnection(configuration);
		hbaseAdmin = new HBaseAdmin(configuration);
	}

	public static HBaseUtils getInstance(String confPath) throws Exception {
		if (hbaseUtils == null) {
			hbaseUtils = new HBaseUtils(confPath);
		}
		return hbaseUtils;
	}

	public void dropTable(String tableName) throws IOException {
		if (tableExists(tableName)) {
			if (hbaseAdmin.isTableEnabled(tableName)) {
				hbaseAdmin.disableTable(tableName);
			}
			hbaseAdmin.deleteTable(tableName);
		}
	}

	public boolean tableExists(String tableName) throws IOException {
		return hbaseAdmin.tableExists(tableName);
	}

	public void createTable(String tableName) throws IOException {
		logger.error("create table [" + tableName + "] running...");
		HTableDescriptor tableDescriptor = new HTableDescriptor(
				TableName.valueOf(tableName));
		// 设置写WAL日志的级别，示例中设置的是同步写WAL，该方式安全性较高，但无疑会一定程度影响性能，请根据具体场景选择使用；
		//		tableDescriptor.setDurability(Durability.SYNC_WAL);
		HColumnDescriptor columnDescriptor = new HColumnDescriptor(
				HBaseContants.DEFAULT_HBASE_TABLE_COLUMN_FAMILY_NAME);
		tableDescriptor.addFamily(columnDescriptor);
		getHbaseAdmin().createTable(tableDescriptor);
		logger.error("create table [" + tableName + "] done!");
	}

	public void queryAll(String tableName) {
		try {
			long start = System.currentTimeMillis();
			System.out.println("统计总数：");
			HTableInterface htableInterface = hconnection.getTable(tableName);
			Scan scan = new Scan();
			ResultScanner scanner = htableInterface.getScanner(scan);
			int count = 0;
			Iterator<Result> iterator = scanner.iterator();
			while (iterator.hasNext()) {
				//				Result row = 
				iterator.next();
				//				for (Cell cell : row.rawCells()) {
				//					String columnName = new String(
				//							CellUtil.cloneQualifier(cell));
				//					Object value = null;
				//					byte[] byteValue = CellUtil.cloneValue(cell);
				//					if ("DOMAINNAME".equals(columnName)) {
				//						value = Bytes.toString(byteValue);
				//					} else {
				//						value = Bytes.toBigDecimal(byteValue);
				//					}
				//					System.out.print(columnName + "=" + value + "\t");
				//				}
				//				System.out.println();
				count++;
			}
			long end = System.currentTimeMillis();
			System.out.println("总记录数：" + count + "   count耗时：" + (end - start)
					/ 1000);
			htableInterface.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public HConnection getHconnection() {
		return hconnection;
	}

	public HBaseAdmin getHbaseAdmin() {
		return hbaseAdmin;
	}
}
