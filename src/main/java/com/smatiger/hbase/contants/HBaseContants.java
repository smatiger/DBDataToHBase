package com.smatiger.hbase.contants;

/**
 * @ClassName: HBaseContants
 * @Description: 常量
 * @author wangxiaohu wsmalltiger@163.com
 * @date 2015年4月21日 下午4:52:25
 */
public class HBaseContants {
	public static final boolean DEFAULT_HBASE_TABLE_IF_EXISTS_IS_DELETE = false;
	public static final boolean DEFAULT_HBASE_TABLE_IS_DATA_FROM_DB = false;
	public static final String DEFAULT_HBASE_TABLE_COLUMN_FAMILY_NAME = "columnFamily1";

	public static final long LOAD_DBDATA_TO_HBASE_THREAD_MIN_DATA_SIZE = 10000;
	public static final long LOAD_DBDATA_TO_HBASE_THREAD_MAX_DATA_SIZE = 200000;

	public static final int LOAD_DBDATA_TO_HBASE_THREAD_POOL_MIN = 2;
	public static final int LOAD_DBDATA_TO_HBASE_THREAD_POOL_MAX = 10;

	public static final String HBASE_TABLE_ID_COLUMN_NAME = "ID";
	public static final String HBASE_CONF_YAML_TABLE_NAMESPACE_DEFAULT = "default";

	public static final long DATA_INPORT_SIZE = 2000;

	public static final String HBASE_CONF_SITE_XML_NAME = "hbase-site.xml";

	public static final String HBASE_CONF_YAML_MASTER = "hbase.master";
	public static final String HBASE_CONF_YAML_TABLE = "hbase.table";
	public static final String HBASE_CONF_YAML_TABLE_ENABLE = "enable";
	public static final String HBASE_CONF_YAML_TABLE_NAME = "table.name";
	public static final String HBASE_CONF_YAML_TABLE_NAMESPACE = "table.nameSpace";
	public static final String HBASE_CONF_YAML_TABLE_COLUMN = "table.column";
	public static final String HBASE_CONF_YAML_TABLE_FROMDB = "table.fromDB";
	public static final String HBASE_CONF_YAML_TABLE_EXISTSISDELETE = "table.existsIsDelete";
	public static final String HBASE_CONF_YAML_TABLE_DBTABLENAME = "table.dbTableName";

	public static final String HBASE_CONF_YAML_CONNECTION = "hbase.connection";
	public static final String HBASE_CONF_YAML_CONNECTION_ENABLE = "enable";
	public static final String HBASE_CONF_YAML_CONNECTION_DRIVER = "driver";
	public static final String HBASE_CONF_YAML_CONNECTION_URL = "connUrl";
	public static final String HBASE_CONF_YAML_CONNECTION_USERNAME = "userName";
	public static final String HBASE_CONF_YAML_CONNECTION_PASSWORD = "password";
	public static final String HBASE_CONF_YAML_CONNECTION_MAX_POOL_SIZE = "maxPoolSize";
	public static final String HBASE_CONF_YAML_CONNECTION_MIN_POOL_SIZE = "minPoolSize";
}
