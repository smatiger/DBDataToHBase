# DBDataToHBase
关系型数据库中的数据同步至HBase;

### 使用BoneCP作为数据库连接池。测试过Oracle数据库同步至Hbase，理论上应该支持所有关系型数据库（例如：mysql、sqlserver等）。


实现原理
---------------------------------
	1、加载并解析hbase-site.xml和自定义yaml配置文件
	2、创建HBaseConnection、JDBC connectionPool
	3、循环自定义yaml配置hbase.table；验证配置；
	4、获取当前循环表的最大、最小ID
	5、根据配置、表id自动平均分配线程从数据库中拉去数据
	6、线程池管理线程，批量导入hbase
	7、清理集合数据；关闭线程池；导入完成；


测试
------------------------------------------------
13W数据，6个字段20秒内完成。还算可以接受；13W数据，24个字段50秒左右完成。
	  
2000条提交一次至HBase，耗时0.6秒左右；10000条提交一次至HBase，耗时1.6秒左右。

oracle表中6380W数据共30个字段。共开启340个线程，并行执行10个线程，每个线程处理20W数据。

缺点：
-----------------------------------------------
- 暂时不支持HBase多列族。
- 仅支持单节点运行，性能与sqoop相差很多。


yaml配置示例：
-----------------------------------------
``` java
#HBase地址
hbase.master: 192.168.1.248:60000
#JBDC连接配置，使用BoneCP作为连接池
hbase.connection:
  enable: true
  driver: oracle.jdbc.driver.OracleDriver
  connUrl: "jdbc:oracle:thin:@192.168.1.2:1521:oracl"
  userName: demo
  password: demo
  maxPoolSize: 30
  minPoolSize: 5
hbase.table:
  #hbase中的表名，必输项
 - table.name: organizations
  #是否启用该配置，默认false
   enable: true
  #hbase中的namespace，默认：default
   table.nameSpace: scgrid
   #是否从数据库中导入纪录到hbase,默认：false
   table.fromDB: true
   #如果base中存在表，是否删表，默认：false
   table.existsIsDelete: false
   #数据库表名，默认：与hbase中表名相同
#   table.dbTableName: organizations
   #需要导出的列，默认导出全部列
#   table.column: [id, parentfunorgid, parentid, orgtype, orglevel, subcount]
#   table.column: [id, parentfunorgid, parentid, orgtype, orglevel, subcount, seq, maxcode, subcountfun, departmentno, orgname, contactway, orginternalcode, simplepinyin, fullpinyin, remark, createuser, buildingid, centerx, centery, updateuser, updatedate, createdate, functionalorgtype]
 - table.name: propertydomain
   enable: false
   table.fromDB: true
   table.existsIsDelete: false
   table.dbTableName: propertydomains
   table.column: [id, domainname, systemsensitive]
 - table.name: baseinfo
   enable: false
   table.nameSpace: scgrid
   table.existsIsDelete: false
   table.fromDB: true
#   table.column: [id, namefullpinyin, simplepinyin, usedname, idcardno, telephone, mobilenumber, birthday, gender, workunit, imgurl, email, isdeath, nation, politicalbackground, schooling, career, maritalstate, bloodtype, faith, stature, province, city, district, nativeplaceaddress, nativepolicestation, createuser, updateuser, createdate, updatedate]
``` 


