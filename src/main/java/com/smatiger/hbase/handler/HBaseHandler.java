package com.smatiger.hbase.handler;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smatiger.hbase.connection.ConnectionPool;
import com.smatiger.hbase.contants.HBaseContants;
import com.smatiger.hbase.utils.DataBaseUtils;
import com.smatiger.hbase.utils.HBaseConfigurationUtils;
import com.smatiger.hbase.utils.HBaseUtils;

/**
 * @ClassName: HBaseHandler
 * @Description: HBase处理器
 * @author wangxiaohu wsmalltiger@163.com
 * @date 2015年4月21日 下午2:35:18
 */
public class HBaseHandler {
	private static Logger logger = LoggerFactory.getLogger(HBaseHandler.class);
	private ConnectionPool connectionPool;
	private HBaseUtils hbaseUtils;

	@SuppressWarnings("unchecked")
	public void createTable(String confPath) throws Exception {
		Map<String, Object> hbaseConf = HBaseConfigurationUtils
				.getHBaseConfYaml(confPath);
		List<Object> hbaseTableMap = (List<Object>) hbaseConf
				.get(HBaseContants.HBASE_CONF_YAML_TABLE);
		for (int i = 0; hbaseTableMap != null && i < hbaseTableMap.size(); i++) {
			Map<String, Object> tableMap = (Map<String, Object>) hbaseTableMap
					.get(i);
			boolean tableEnable = HBaseConfigurationUtils.dealYamlBooleanValue(
					tableMap.get(HBaseContants.HBASE_CONF_YAML_TABLE_ENABLE),
					false);
			if (!tableEnable) {
				continue;
			}
			String nameSpace = (String) tableMap
					.get(HBaseContants.HBASE_CONF_YAML_TABLE_NAMESPACE);
			if (StringUtils.isEmpty(nameSpace)) {
				nameSpace = HBaseContants.HBASE_CONF_YAML_TABLE_NAMESPACE_DEFAULT;
			}
			isCreateNameSpace(nameSpace);
			String tableName = (String) tableMap
					.get(HBaseContants.HBASE_CONF_YAML_TABLE_NAME);
			boolean fromDB = HBaseConfigurationUtils.dealYamlBooleanValue(
					tableMap.get(HBaseContants.HBASE_CONF_YAML_TABLE_FROMDB),
					HBaseContants.DEFAULT_HBASE_TABLE_IS_DATA_FROM_DB);
			List<Object> columns = (List<Object>) tableMap
					.get(HBaseContants.HBASE_CONF_YAML_TABLE_COLUMN);
			boolean ifExistsIsDelete = HBaseConfigurationUtils
					.dealYamlBooleanValue(
							tableMap.get(HBaseContants.HBASE_CONF_YAML_TABLE_EXISTSISDELETE),
							HBaseContants.DEFAULT_HBASE_TABLE_IF_EXISTS_IS_DELETE);
			String dbTableName = (String) tableMap
					.get(HBaseContants.HBASE_CONF_YAML_TABLE_DBTABLENAME);
			dbTableName = StringUtils.isEmpty(dbTableName) ? tableName
					: dbTableName;
			isCreateTable(nameSpace + ":" + tableName, dbTableName, columns,
					ifExistsIsDelete, fromDB);
		}
	}

	private void checkSystemEnv() {
		if (StringUtils.isEmpty(System.getenv("HBASE_HOME"))) {
			throw new RuntimeException("环境变量HBASE_HOME未配置！");
		}
		if (StringUtils.isEmpty(System.getenv("HADOOP_HOME"))) {
			throw new RuntimeException("环境变量HADOOP_HOME未配置！");
		}
	}

	private void isCreateTable(String tableName, String dbTableName,
			List<Object> columns, final boolean ifExistsIsDelete, boolean fromDB)
			throws Exception {
		checkTablesParams(tableName, columns);
		if (hbaseUtils.tableExists(tableName)) {
			if (ifExistsIsDelete) {
				logger.error("table [" + tableName + "] exists, drop it!");
				hbaseUtils.dropTable(tableName);
				hbaseUtils.createTable(tableName);
			}
		} else {
			hbaseUtils.createTable(tableName);
		}
		if (fromDB) {
			DataBaseUtils.getInstance(connectionPool, hbaseUtils)
					.loadDBDataToHBase(dbTableName, tableName, columns);
		}
	}

	private void checkTablesParams(String tableName, List<Object> columns) {
		if (StringUtils.isEmpty(tableName)) {
			throw new RuntimeException("表名不能为空！");
		}
		String[] nameSpaceTableName = tableName.split(":");
		if (nameSpaceTableName.length != 2) {
			throw new RuntimeException("namespace或表名格式错误，请配置为英文字母！");
		}
		if (StringUtils.isEmpty(nameSpaceTableName[0])
				|| StringUtils.isEmpty(nameSpaceTableName[1])) {
			throw new RuntimeException("namespace或表名不能为空！");
		}
		for (int i = 0; columns != null && i < columns.size(); i++) {
			Object obj = columns.get(i);
			if (obj instanceof String) {
				if (StringUtils.isEmpty((String) obj)) {
					columns.remove(i--);
				}
				//			} else if (obj instanceof List) {
				//				List<Object> subColumns = (List<Object>) obj;
				//				for (int j = 0; subColumns != null && j < subColumns.size(); j++) {
				//					Object subColumn = subColumns.get(j);
				//					if (subColumn instanceof String) {
				//						String subColumnName = (String) subColumn;
				//						nameSpaceTableName = subColumnName.split(":");
				//						if (nameSpaceTableName.length != 2) {
				//							throw new RuntimeException("列[" + columns
				//									+ "]参数格式错误，多个列族必须包含[:]!");
				//						}
				//						if (StringUtils.isEmpty(nameSpaceTableName[0])
				//								|| StringUtils.isEmpty(nameSpaceTableName[1])) {
				//							throw new RuntimeException("[" + columns
				//									+ "]namespace或表名不能为空！");
				//						}
				//					} else {
				//						throw new RuntimeException("列[" + columns + "]配置错误！");
				//					}
				//				}
			} else {
				throw new RuntimeException("列[" + columns + "]配置错误， 只能为英文字母组合！");
			}
		}
	}

	private void isCreateNameSpace(String nameSpace) throws Exception {
		HBaseAdmin hbaseAdmin = hbaseUtils.getHbaseAdmin();
		NamespaceDescriptor[] namespaceDescriptors = hbaseAdmin
				.listNamespaceDescriptors();
		boolean isExists = false;
		for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
			if (nameSpace.trim().equals(namespaceDescriptor.getName())) {
				isExists = true;
				break;
			}
		}
		if (!isExists) {
			logger.error("create nameSpace: " + nameSpace);
			hbaseAdmin.createNamespace(NamespaceDescriptor.create(nameSpace)
					.build());
			hbaseAdmin.close();
		}
	}

	public HBaseHandler(String confPath) throws Exception {
		checkSystemEnv();
		hbaseUtils = HBaseUtils.getInstance(confPath);
		connectionPool = ConnectionPool.getInstance(confPath);
	}
}
