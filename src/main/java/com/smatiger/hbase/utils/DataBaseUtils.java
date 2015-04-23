package com.smatiger.hbase.utils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smatiger.hbase.connection.ConnectionPool;
import com.smatiger.hbase.contants.HBaseContants;

/**
 * @ClassName: DataBaseUtils
 * @Description: 数据库工具类
 * @author wangxiaohu wsmalltiger@163.com
 * @date 2015年4月22日 下午1:56:05
 */
public class DataBaseUtils {
	private static Logger logger = LoggerFactory.getLogger(DataBaseUtils.class);
	private ConnectionPool connectionPool;
	private HBaseUtils hbaseUtils;
	private AtomicInteger threadCount = null;
	private int dataListSumSize = 0;
	private AtomicInteger dataListNum = new AtomicInteger(0);
	private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private DateFormat dateFormatDate = new SimpleDateFormat("yyyy-MM-dd");
	private static DataBaseUtils dataBaseUtils;
	private Timer timer = new Timer();

	private void dealDBDataByIdRange(String tableName, String hbaseTable,
			long minId, long maxId, Map<String, String> columnLabels)
			throws Exception {
		if (minId == 0 || maxId == 0) {
			logger.error("DB table [" + tableName + "] is empty!");
			return;
		}
		long idRange = maxId - minId;
		if (idRange <= HBaseContants.LOAD_DBDATA_TO_HBASE_THREAD_MIN_DATA_SIZE) {
			logger.info("load DB table [" + tableName
					+ "] data to HBase table [" + hbaseTable + "] , no thread.");
			executeLoadDBDataToHBaseByIdRange(tableName, hbaseTable, minId,
					maxId, columnLabels);
		} else {
			int idSplitLength = Integer.valueOf("" + idRange
					/ HBaseContants.LOAD_DBDATA_TO_HBASE_THREAD_MAX_DATA_SIZE);
			int openThreadLength = idSplitLength < HBaseContants.LOAD_DBDATA_TO_HBASE_THREAD_POOL_MIN ? HBaseContants.LOAD_DBDATA_TO_HBASE_THREAD_POOL_MIN
					: idSplitLength;
			openThreadLength = openThreadLength < 1 ? 1 : openThreadLength;
			logger.error("load DB table data[" + tableName
					+ "] to HBase , open " + openThreadLength + " thread.");
			openThread(tableName, hbaseTable, openThreadLength, idRange, minId,
					columnLabels);
		}
	}

	private void openThread(String tableName, String hbaseTable,
			int openThreadLength, long idRange, Long minId,
			Map<String, String> columnLabels) throws Exception {
		threadCount = new AtomicInteger(openThreadLength);
		CountDownLatch syncLock = new CountDownLatch(openThreadLength);
		Long idValue = idRange % openThreadLength == 0 ? idRange
				/ openThreadLength : idRange / openThreadLength + 1;
		for (int i = 0; i < openThreadLength; i++) {
			Thread thread = new DBDataToHBaseThread(tableName, hbaseTable,
					minId + (i * idValue) + (i == 0 ? 0 : 1), minId
							+ ((i + 1) * idValue), openThreadLength, syncLock,
					columnLabels);
			ThreadPool.getInstance().execute(thread);
		}
		syncLock.await();
		ThreadPool.getInstance().shutdown();
	}

	private void executeLoadDBDataToHBaseByIdRange(String tableName,
			String hbaseTableName, long minId, long maxId,
			Map<String, String> columnMap) throws Exception {
		long start = System.currentTimeMillis();
		Connection connection = connectionPool.getConnection();
		HTableInterface htable = hbaseUtils.getHconnection().getTable(
				hbaseTableName);
		ResultSet resultSet = connection.createStatement().executeQuery(
				"select * from " + tableName + " where id <= " + maxId
						+ " and id >= " + minId);
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		while (resultSet.next()) {
			Map<String, Object> map = new HashMap<String, Object>();
			Iterator<String> iterator = columnMap.keySet().iterator();
			while (iterator.hasNext()) {
				String column = iterator.next();
				String type = columnMap.get(column);
				map.put(column, putObjectValueToMap(resultSet, column, type));
			}
			resultList.add(map);
		}
		dataListNum.addAndGet(resultList.size());
		dataListSumSize += resultList.size();
		long query = System.currentTimeMillis();
		logger.info("query DB table " + tableName + "  id range " + maxId + "-"
				+ minId + "=" + (maxId - minId) + "  size=" + resultList.size()
				+ "  useTime：" + (query - start) / 1000 + " seconds.");
		List<Put> putList = new ArrayList<Put>();
		int index = 0;
		for (Map<String, Object> map : resultList) {
			Long id = Long.parseLong(map.get("ID").toString());
			Put put = new Put(Bytes.toBytes(id));
			put.add(Bytes
					.toBytes(HBaseContants.DEFAULT_HBASE_TABLE_COLUMN_FAMILY_NAME),
					Bytes.toBytes(HBaseContants.HBASE_TABLE_ID_COLUMN_NAME),
					Bytes.toBytes(id));
			Iterator<String> iterator = map.keySet().iterator();
			while (iterator.hasNext()) {
				String key = iterator.next();
				Object value = map.get(key);
				byte[] byteValue = null;
				if (value != null) {
					byteValue = getByteValueFromMap(value);
				}
				put.add(Bytes
						.toBytes(HBaseContants.DEFAULT_HBASE_TABLE_COLUMN_FAMILY_NAME),
						Bytes.toBytes(key), byteValue);
			}
			putList.add(put);
			dataListNum.getAndDecrement();
			index++;
			if (index % HBaseContants.DATA_INPORT_SIZE == 0) {
				htable.put(putList);
				putList.clear();
			}
		}
		if (putList.size() != 0) {
			htable.put(putList);
			putList.clear();
		}
		long end = System.currentTimeMillis();
		logger.info("import HBase table " + hbaseTableName + "  useTime ："
				+ (end - query) / 1000 + " seconds.");
		connectionPool.closeConnection(connection);
		resultList.clear();
		resultList = null;
		htable.close();
	}

	private byte[] getByteValueFromMap(Object value) {
		if (value instanceof BigDecimal) {
			return Bytes.toBytes((BigDecimal) value);
		} else if (value instanceof Timestamp) {
			try {
				return Bytes.toBytes(dateFormat.format((Timestamp) value));
			} catch (Exception e) {
				return Bytes.toBytes(dateFormatDate.format((Timestamp) value));
			}
		}
		return Bytes.toBytes(value.toString());
	}

	private Object putObjectValueToMap(ResultSet resultSet, String column,
			String type) throws Exception {
		if (String.class.getName().equals(type)) {
			return resultSet.getString(column);
		} else if (BigDecimal.class.getName().equals(type)) {
			return resultSet.getBigDecimal(column);
		} else if (Timestamp.class.getName().equals(type)) {
			return resultSet.getTimestamp(column);
		}
		return resultSet.getObject(column);
	}

	class DBDataToHBaseThread extends Thread {
		private String tableName;
		private String hbaseTable;
		private long minId;
		private long maxId;
		private CountDownLatch syncLock;
		private int threadNum;
		private Map<String, String> columnMap;

		public DBDataToHBaseThread(String tableName, String hbaseTable,
				long minId, long maxId, int threadNum, CountDownLatch syncLock,
				Map<String, String> columnMap) {
			this.tableName = tableName;
			this.hbaseTable = hbaseTable;
			this.minId = minId;
			this.maxId = maxId;
			this.threadNum = threadNum;
			this.syncLock = syncLock;
			this.columnMap = columnMap;
		}

		@Override
		public void run() {
			try {
				executeLoadDBDataToHBaseByIdRange(tableName, hbaseTable, minId,
						maxId, columnMap);
			} catch (Exception e) {
				logger.error("", e);
			} finally {
				syncLock.countDown();
				threadCount.getAndDecrement();
				logger.error("load DB table data[" + tableName
						+ "] to HBase thread done! thread num " + threadNum
						+ ", surplus thread " + threadCount.get() + "!");
			}
		}
	}

	public void loadDBDataToHBase(String tableName, String hbaseTable,
			List<Object> columns) throws Exception {
		if (!connectionPool.enable) {
			logger.error("DB connectionPool is not enable. load DB data to HBase failure!");
			return;
		}
		Connection connection = connectionPool.getConnection();
		ResultSet resultSet = connection.createStatement().executeQuery(
				"select * from " + tableName + " where 1=2");
		ResultSetMetaData rsMetaData = resultSet.getMetaData();
		Map<String, String> columnMap = getTableColumns(rsMetaData, columns,
				tableName);
		logger.info("load data from DB table [" + tableName
				+ "] to HBase table [" + hbaseTable + "] running...");
		long minId = 0, maxId = 0;
		resultSet = connection.createStatement().executeQuery(
				"select min(id) from " + tableName);
		if (resultSet.next()) {
			minId = resultSet.getLong(1);
		}
		resultSet = connection.createStatement().executeQuery(
				"select max(id) from " + tableName);
		if (resultSet.next()) {
			maxId = resultSet.getLong(1);
		}
		long startTime = System.currentTimeMillis();
		dealDBDataByIdRange(tableName, hbaseTable, minId, maxId, columnMap);
		dataListNum = new AtomicInteger(0);
		dataListSumSize = 0;
		timer.cancel();
		long endTime = System.currentTimeMillis();
		logger.error("load DB table [" + tableName + "] data to HBase table ["
				+ hbaseTable + "] use time: " + (endTime - startTime) / 1000
				+ " seconds. is done!");
	}

	private Map<String, String> getTableColumns(ResultSetMetaData rsMetaData,
			List<Object> columns, String tableName) throws Exception {
		Map<String, String> columnLabels = new HashMap<String, String>();
		List<String> columnNames = new ArrayList<String>();
		for (int i = 0; i < rsMetaData.getColumnCount(); i++) {
			String columnLabel = rsMetaData.getColumnLabel(i + 1).trim()
					.toUpperCase();
			columnNames.add(columnLabel);
			columnLabels.put(columnLabel, rsMetaData.getColumnClassName(i + 1));
		}
		if (columns == null) {
			return columnLabels;
		}
		for (int i = 0; i < columns.size(); i++) {
			String columnName = columns.get(i).toString().trim().toUpperCase();
			if (!columnNames.contains(columnName)) {
				throw new RuntimeException("column: " + columnName
						+ " is not in table " + tableName + "!");
			}
		}
		for (int i = 0; i < columnNames.size(); i++) {
			boolean isExists = false;
			String key = columnNames.get(i);
			for (int j = 0; j < columns.size(); j++) {
				String columnName = columns.get(j).toString().trim()
						.toUpperCase();
				if (columnName.equals(key)) {
					isExists = true;
				}
			}
			if (!isExists) {
				columnLabels.remove(key);
			}
		}
		return columnLabels;
	}

	public static DataBaseUtils getInstance(ConnectionPool connectionPool,
			HBaseUtils hbaseUtils) {
		if (dataBaseUtils == null) {
			dataBaseUtils = new DataBaseUtils(connectionPool, hbaseUtils);
		}
		return dataBaseUtils;
	}

	private DataBaseUtils(ConnectionPool connectionPool, HBaseUtils hbaseUtils) {
		this.connectionPool = connectionPool;
		this.hbaseUtils = hbaseUtils;
		startTimer();
	}

	private void startTimer() {
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				printImportHBaseListSize();
			}
		}, 2000, 1000 * 30);
	}

	private void printImportHBaseListSize() {
		ThreadPool pool = ThreadPool.getInstance();
		BlockingQueue<Runnable> queue = pool.getQueue();
		logger.info("----->-----------------------");
		logger.info("----->当前线程池大小:" + pool.getPoolSize());
		logger.info("----->当前线程池是否关闭:" + pool.isShutdown());
		logger.info("----->当前总共线程:" + pool.getTaskCount());
		logger.info("----->当前活动线程:" + pool.getActiveCount());
		logger.info("----->当前已完线程:" + pool.getCompletedTaskCount());
		logger.info("----->当前等待线程:" + queue.size());
		logger.error("---->total data：" + dataListSumSize + "  The remaining："
				+ dataListNum.get());
		logger.info("----->-----------------------");
	}
}
