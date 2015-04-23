package com.smatiger.hbase.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import com.smatiger.hbase.contants.HBaseContants;
import com.smatiger.hbase.utils.HBaseConfigurationUtils;

/**
 * @ClassName: ConnectionPool
 * @Description: 连接池BoneCP
 * @author wangxiaohu wsmalltiger@163.com
 * @date 2015年4月22日 上午9:25:43
 */
public class ConnectionPool {
	private static Logger logger = LoggerFactory
			.getLogger(ConnectionPool.class);

	public boolean enable = false;

	private static String connection_driver = null;
	private static String connection_url = null;
	private static String connection_userName = null;
	private static String connection_password = null;
	private static int connection_max_poolSize = 30;
	private static int connection_min_poolSize = 5;
	private static BoneCP boneCP = null;
	private static ConnectionPool connectionPool = null;

	private ConnectionPool(String confPath) {
		if (!getConfig(confPath)) {
			return;
		}
		try {
			Class.forName(connection_driver);
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl(connection_url);
			config.setUsername(connection_userName);
			config.setPassword(connection_password);
			config.setMaxConnectionsPerPartition(connection_max_poolSize);
			config.setMinConnectionsPerPartition(connection_min_poolSize);
			//分区数
			config.setPartitionCount(2);
			config.setConnectionTimeoutInMs(3600 * 1000);
			boneCP = new BoneCP(config);
		} catch (Exception e) {
			throw new RuntimeException("初始化连接池出错：", e);
		}
	}

	public static ConnectionPool getInstance(String confPath) {
		if (connectionPool == null) {
			connectionPool = new ConnectionPool(confPath);
		}
		return connectionPool;
	}

	@SuppressWarnings("unchecked")
	private boolean getConfig(String confPath) {
		Map<String, String> hbaseJDBCConf = (Map<String, String>) HBaseConfigurationUtils
				.getHBaseConfYaml(confPath).get(
						HBaseContants.HBASE_CONF_YAML_CONNECTION);
		if (hbaseJDBCConf == null) {
			return false;
		}
		enable = HBaseConfigurationUtils.dealYamlBooleanValue(hbaseJDBCConf
				.get(HBaseContants.HBASE_CONF_YAML_CONNECTION_ENABLE), false);
		if (!enable) {
			return false;
		}
		try {
			connection_driver = hbaseJDBCConf
					.get(HBaseContants.HBASE_CONF_YAML_CONNECTION_DRIVER);
			connection_url = hbaseJDBCConf
					.get(HBaseContants.HBASE_CONF_YAML_CONNECTION_URL);
			connection_userName = hbaseJDBCConf
					.get(HBaseContants.HBASE_CONF_YAML_CONNECTION_USERNAME);
			connection_password = hbaseJDBCConf
					.get(HBaseContants.HBASE_CONF_YAML_CONNECTION_PASSWORD);
			connection_max_poolSize = HBaseConfigurationUtils
					.dealYamlIntegerValue(
							hbaseJDBCConf
									.get(HBaseContants.HBASE_CONF_YAML_CONNECTION_MAX_POOL_SIZE),
							connection_max_poolSize);
			connection_min_poolSize = HBaseConfigurationUtils
					.dealYamlIntegerValue(
							hbaseJDBCConf
									.get(HBaseContants.HBASE_CONF_YAML_CONNECTION_MIN_POOL_SIZE),
							connection_min_poolSize);
			return true;
		} catch (Exception e) {
			throw new RuntimeException("连接池配置错误：", e);
		}
	}

	public Connection getConnection() throws SQLException {
		return boneCP.getConnection();
	}

	public void closeConnection(Connection connection) {
		if (!enable || connection == null) {
			return;
		}
		try {
			connection.close();
		} catch (SQLException e) {
			logger.error("关闭JBDC连接出错：", e);
		}
	}

	public void shutdown() {
		if (enable && boneCP != null) {
			boneCP.shutdown();
		}
	}
}
