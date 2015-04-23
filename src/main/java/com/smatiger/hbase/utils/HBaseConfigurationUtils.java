package com.smatiger.hbase.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.yaml.snakeyaml.Yaml;

import com.smatiger.hbase.contants.HBaseContants;

/**
 * @ClassName: HBaseConfigurationUtils
 * @Description: 获取HBase配置
 * @author wangxiaohu wsmalltiger@163.com
 * @date 2015年4月21日 下午2:34:11
 */
public class HBaseConfigurationUtils {
	private static Logger logger = LoggerFactory
			.getLogger(HBaseConfigurationUtils.class);
	public static Configuration configuration;

	private static Map<String, Object> confYamlMap = null;

	public static Configuration getConfiguration(String confPath) {
		if (configuration == null) {
			//			confPath = System.getenv("HADOOP_HOME") + File.separator + confPath;
			confPath = HBaseConfigurationUtils.class
					.getResource("/" + confPath).getPath();
			logger.error("加载HBase配置文件[" + confPath + "]...");
			configuration = HBaseConfiguration.create();
			Map<String, String> hbaseSiteXmlMap = getHBaseSiteXmlMap();
			String zkClientPort = hbaseSiteXmlMap
					.get("hbase.zookeeper.property.clientPort");
			String zkQuorum = hbaseSiteXmlMap.get("hbase.zookeeper.quorum");
			logger.info("zkQuorum:" + zkQuorum);
			logger.info("zkClientPort:" + zkClientPort);
			configuration.set("hbase.zookeeper.property.clientPort",
					zkClientPort);
			configuration.set("hbase.zookeeper.quorum", zkQuorum);
			String masterAddress = (String) getHBaseConfYaml(confPath).get(
					HBaseContants.HBASE_CONF_YAML_MASTER);
			if (StringUtils.isEmpty(masterAddress)) {
				throw new RuntimeException("hbase.master不能为空！");
			}
			configuration.set("hbase.master", masterAddress);
			logger.error("HBase配置文件加载完毕!");
		}
		return configuration;
	}

	private static Map<String, String> getHBaseSiteXmlMap() {
		String path = System.getenv("HBASE_HOME") + File.separator + "conf"
				+ File.separator + HBaseContants.HBASE_CONF_SITE_XML_NAME;
		Map<String, String> hbaseSiteXmlMap = new HashMap<String, String>();
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder dbBuilder = dbFactory.newDocumentBuilder();
			Document doc = dbBuilder.parse(new File(path));
			NodeList nList = doc.getElementsByTagName("configuration").item(0)
					.getChildNodes();
			for (int i = 0; i < nList.getLength(); i++) {
				Node node = nList.item(i);
				if ("property".equals(node.getNodeName())) {
					putNodeValueToMap(hbaseSiteXmlMap, node);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("解析文件[" + path + "]发生异常！", e);
		}
		return hbaseSiteXmlMap;
	}

	private static void putNodeValueToMap(Map<String, String> hbaseSiteXmlMap,
			Node parentNode) {
		if (parentNode == null) {
			return;
		}
		NodeList nodeList = parentNode.getChildNodes();
		String name = null, value = null;
		for (int i = 0; nodeList != null && i < nodeList.getLength(); i++) {
			Node node = nodeList.item(i);
			if ("name".equals(node.getNodeName())) {
				name = node.getTextContent();
			}
			if ("value".equals(node.getNodeName())) {
				value = node.getTextContent();
			}
		}
		if (name != null && value != null) {
			hbaseSiteXmlMap.put(name.trim(), value.trim());
		}
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> getHBaseConfYaml(String path) {
		if (confYamlMap == null) {
			if (StringUtils.isEmpty(path)) {
				throw new RuntimeException("HBase配置目录不能为空！");
			}
			Iterator<Object> iterator;
			try {
				iterator = new Yaml().loadAll(new FileInputStream(path))
						.iterator();
				if (iterator.hasNext()) {
					Object yamlObject = iterator.next();
					if (yamlObject instanceof Map) {
						confYamlMap = (Map<String, Object>) yamlObject;
					} else {
						throw new RuntimeException("文件[" + path + "]不存在或格式错误！");
					}
				}
			} catch (FileNotFoundException e) {
				throw new RuntimeException("文件不存在[" + path + "]！", e);
			}
		}
		return confYamlMap;
	}

	public static boolean dealYamlBooleanValue(Object booleanValue,
			boolean defaultValue) {
		boolean returnBoolean = defaultValue;
		if (booleanValue == null) {
			return returnBoolean;
		}
		if (booleanValue instanceof Boolean) {
			returnBoolean = ((Boolean) booleanValue).booleanValue();
		} else {
			returnBoolean = "true".equals(booleanValue.toString().trim()) ? true
					: defaultValue;
		}
		return returnBoolean;
	}

	public static int dealYamlIntegerValue(Object obj, int defaultValue) {
		int returnValue = defaultValue;
		if (obj == null) {
			return returnValue;
		}
		if (obj instanceof Integer) {
			returnValue = (Integer) obj;
		} else {
			try {
				returnValue = Integer.valueOf(obj.toString().trim());
			} catch (Exception e) {
			}
		}
		return returnValue;
	}
}
