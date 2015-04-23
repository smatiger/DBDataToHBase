# DBDataToHBase
��ϵ�����ݿ��е�����ͬ����HBase;

### ʹ��BoneCP��Ϊ���ݿ����ӳء����Թ�Oracle���ݿ�ͬ����Hbase��������Ӧ��֧�����й�ϵ�����ݿ⣨���磺mysql��sqlserver�ȣ���


ʵ��ԭ��
---------------------------------
	1�����ز�����hbase-site.xml���Զ���yaml�����ļ�
	2������HBaseConnection��JDBC connectionPool
	3��ѭ���Զ���yaml����hbase.table����֤���ã�
	4����ȡ��ǰѭ����������СID
	5���������á���id�Զ�ƽ�������̴߳����ݿ�����ȥ����
	6���̳߳ع����̣߳���������hbase
	7�����������ݣ��ر��̳߳أ�������ɣ�


����
------------------------------------------------
13W���ݣ�6���ֶ�20������ɡ�������Խ��ܣ�13W���ݣ�24���ֶ�50��������ɡ�
	  
2000���ύһ����HBase����ʱ0.6�����ң�10000���ύһ����HBase����ʱ1.6�����ҡ�

oracle����6380W���ݹ�30���ֶΡ�������340���̣߳�����ִ��10���̣߳�ÿ���̴߳���20W���ݡ�

ȱ�㣺
-----------------------------------------------
- ��ʱ��֧��HBase�����塣
- ��֧�ֵ��ڵ����У�������sqoop���ܶࡣ


yaml����ʾ����
-----------------------------------------
``` java
#HBase��ַ
hbase.master: 192.168.1.248:60000
#JBDC�������ã�ʹ��BoneCP��Ϊ���ӳ�
hbase.connection:
  enable: true
  driver: oracle.jdbc.driver.OracleDriver
  connUrl: "jdbc:oracle:thin:@192.168.1.2:1521:oracl"
  userName: demo
  password: demo
  maxPoolSize: 30
  minPoolSize: 5
hbase.table:
  #hbase�еı�����������
 - table.name: organizations
  #�Ƿ����ø����ã�Ĭ��false
   enable: true
  #hbase�е�namespace��Ĭ�ϣ�default
   table.nameSpace: scgrid
   #�Ƿ�����ݿ��е����¼��hbase,Ĭ�ϣ�false
   table.fromDB: true
   #���base�д��ڱ��Ƿ�ɾ��Ĭ�ϣ�false
   table.existsIsDelete: false
   #���ݿ������Ĭ�ϣ���hbase�б�����ͬ
#   table.dbTableName: organizations
   #��Ҫ�������У�Ĭ�ϵ���ȫ����
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
