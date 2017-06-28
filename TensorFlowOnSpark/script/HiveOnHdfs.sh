$ tar -xzvf hive-x.y.z.tar.gz
$ cd hive-x.y.z
$ export HIVE_HOME={{pwd}}
$ export PATH=$HIVE_HOME/bin:$PATH
$HADOOP_HOME/bin/hadoop fs -mkdir       /tmp
$HADOOP_HOME/bin/hadoop fs -mkdir       /user/hive/warehouse
$HADOOP_HOME/bin/hadoop fs -chmod g+w   /tmp
$HADOOP_HOME/bin/hadoop fs -chmod g+w   /user/hive/warehouse


#10.85.125.173   feed-ctr
#10.85.125.174   feed-ctr

ipMaster=10.85.125.173
ipSlaver1=10.85.125.174
mainPath=/home/hadoop/
useradd -d ${mainPath} -m hadoop -p hadoop
echo 'hadoop' | passwd --stdin hadoop
ssh localhost
cd ~/.ssh
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat id_rsa.pub >> authorized_keys
cat ~/.ssh/id_rsa.pub>> authorized_keys

chmod 755 ~
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
scp ~/.ssh/authorized_keys hadoop@${ipSlaver1}:~/.ssh/authorized_keys
scp ~/.ssh/known_hosts hadoop@${ipSlaver1}:~/.ssh/
ssh ${ipSlaver1}
exit
cd ~
mkdir ~/libs
mkdir ~/libs/hadoop2.7
cd ~/installs
if [ $(ls | grep hadoop | wc -l) = 0 ]; then
  wget -c -t 10 http://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz 
  #statements
fi
cp *.*gz ~/libs
cd ~/libs
for tarGz in *.*gz; do tar -xvf $tarGz ; done
rm *.*gz
chown  -R  hadoop:hadoop  ~/libs/
echo 'export JAVA_HOME=~/libs/jdk1.8.0_131' > ~/libs/env.sh
echo 'export CLASSPATH=.:$JAVA_HOME/jre/lib/rt.jar:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar' >> ~/libs/env.sh
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/libs/env.sh
echo 'export HADOOP_HOME=~/libs/hadoop-2.7.3' >> ~/libs/env.sh
echo 'export PATH=$PATH:$HADOOP_HOME/bin;' >> ~/libs/env.sh
echo 'export PATH=$PATH:$HADOOP_HOME/sbin' >> ~/libs/env.sh
echo 'export HADOOP_MAPRED_HOME=$HADOOP_HOME' >> ~/libs/env.sh
echo 'export HADOOP_COMMON_HOME=$HADOOP_HOME' >> ~/libs/env.sh
echo 'export HADOOP_HDFS_HOME=$HADOOP_HOME' >> ~/libs/env.sh
echo 'export YARN_HOME=$HADOOP_HOME' >> ~/libs/env.sh
echo 'export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native' >> ~/libs/env.sh
echo 'export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib:$HADOOP_HOME/lib/native"' >> ~/libs/env.sh
echo 'export JAVA_LIBRARY_PATH=$HADOOP_HOME/lib/native:$JAVA_LIBRARY_PATH' >> ~/libs/env.sh
echo 'export SCALA_HOME=~/libs/scala-2.11.8' >> ~/libs/env.sh
echo 'export SPARK_HOME=~/libs/spark-2.0.2-bin-hadoop2.7' >> ~/libs/env.sh
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SCALA_HOME/bin' >> ~/libs/env.sh
echo 'alias hadoop-stop=${HADOOP_HOME}/sbin/stop-all.sh' >> ~/libs/env.sh
echo 'export HIVE_HOME=~/libs/apache-hive-2.1.1-bin' >> ~/libs/env.sh
echo 'export PATH=$HIVE_HOME/bin:$PATH' >> ~/libs/env.sh

echo 'alias hadoop-start=${HADOOP_HOME}/sbin/start-all.sh' >> ~/libs/env.sh
echo 'alias hadoop-restart="hadoop-stop;hadoop-start"' >> ~/libs/env.sh
echo 'alias spark-example=${SPARK_HOME}/bin/run-example' >> ~/libs/env.sh
source ~/libs/env.sh

mkdir $HADOOP_HOME/tmp
mkdir $HADOOP_HOME/data
mkdir $HADOOP_HOME/data/hdfs
mkdir $HADOOP_HOME/data/hdfs/namenode
mkdir $HADOOP_HOME/data/hdfs/datanode
cd $HADOOP_HOME/etc/hadoop
echo 'export JAVA_HOME='$JAVA_HOME >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
echo 'export HADOOP_SSH_OPTS="-i /home/hadoop/.ssh/id_rsa"' >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh 
# cp $HADOOP_HOME/etc/hadoop/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml.origin
echo '
<configuration>
  <property>
    <name>fs.default.name</name>
    <value>hdfs://'${ipMaster}':9000</value>
  </property>
  <!--默认file system uri-->

  <property>
    <name>hadoop.native.lib</name>
    <value>false</value>
    <description>if loaddown native lib, open warn, so set this</description>
  </property>
  <!--使用本地hadoop库标识-->
    <property>
        <name>io.file.buffer.size</name>
        <value>131072</value>
    </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>'${HADOOP_HOME}'/tmp</value>
    <description>A base for other temporary directories.</description>
  </property>
  <!--用于临时文件夹的基础路径-->
</configuration>
' > $HADOOP_HOME/etc/hadoop/core-site.xml 

# cp $HADOOP_HOME/etc/hadoop/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml.origin
echo '
<configuration>
  <property>  
      <name>dfs.namenode.http-address</name>  
      <value>'${ipMaster}':50070</value>  
      <description> fetch NameNode images and edits.注意主机名称 </description>  
  </property>
<!--  <property>  
    <name>dfs.namenode.secondary.http-address</name>  
    <value>'${ipSlaver1}':50090</value>  
    <description> fetch SecondNameNode fsimage </description>  
  </property> -->


  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <!--配置备份数-->

  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:'${HADOOP_HOME}'/data/hdfs/namenode</value>
  </property>
  <!--namenode数据存放地址-->

  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:'${HADOOP_HOME}'/data/hdfs/datanode</value>
  </property>
  <!--datanode 数据存放地址-->
  <property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>'${HADOOP_HOME}'/tmp</value>
    <description>A base for other temporary directories.</description>
  </property>
  <!--用于临时文件夹的基础路径-->
</configuration>

' > $HADOOP_HOME/etc/hadoop/hdfs-site.xml

echo '
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <!--执行mapreduce的框架-->
<!--   <property>
      <name>mapreduce.jobhistory.address</name>
      <value>'${ipMaster}':10020</value>
  </property>
  <property>
      <name>mapreduce.jobhistory.webapp.address</name>
      <value>'${ipMaster}':19888</value>
  </proerty>-->
</configuration>

' > $HADOOP_HOME/etc/hadoop/mapred-site.xml


# cp $HADOOP_HOME/etc/hadoop/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml.origin
echo '
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <!--NodeManage上运行的附属服务。需配置成mapreduce_shuffle，才可运行MapReduce程序-->

  <property>
    <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>
  <!-- mapreduce_shuffle服务的支持类-->

  <property>
    <name>yarn.nodemanager.container-manager.thread-count</name>
    <value>8</value>
    <final>true</final>
  </property>
  <!-- container manager 使用的线程数，默认20 -->

  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>8</value>
    <final>true</final>
  </property>
  <!-- 给containers分配的核数 -->

  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>2048</value>
    <final>true</final>
  </property>
  <!-- 允许给每一个container分配的最小内存，单位MB，默认1024 -->

  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>102400</value>
    <final>true</final>
  </property>
  <!--允许给每一个container分配的最大内存，单位MB，默认8192 -->

  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>32768</value>
    <final>true</final>
  </property>
  <!-- 给container分配的物理内存 -->

<property>
    <name>yarn.resourcemanager.hostname</name>
    <value>'${ipMaster}'</value>
</property>

</configuration>

' > $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo ${ipSlaver1} >> $HADOOP_HOME/etc/hadoop/slaves

# spark 
cp $HADOOP_HOME/etc/hadoop/slaves ${SPARK_HOME}/conf 
echo 'HADOOP_CONF_DIR='${HADOOP_HOME}/etc/hadoop >> ${SPARK_HOME}/conf/spark-env.sh


ssh hadoop@${ipSlaver1}
mkdir ~/libs/
exit
scp -r ~/libs/ hadoop@${ipSlaver1}:~/
hadoop namenode -format
start-all.sh
$HADOOP_HOME/bin/hadoop fs -mkdir       /tmp
$HADOOP_HOME/bin/hadoop fs -mkdir       /user
$HADOOP_HOME/bin/hadoop fs -mkdir       /user/hive/
$HADOOP_HOME/bin/hadoop fs -mkdir       /user/hive/warehouse
$HADOOP_HOME/bin/hadoop fs -chmod g+w   /tmp
$HADOOP_HOME/bin/hadoop fs -chmod g+w   /user/hive/
$HADOOP_HOME/bin/hadoop fs -chmod g+w   /user/hive/warehouse
echo 'export JAVA_HOME='${JAVA_HOME}'  ##Java路径' > ${HIVE_HOME}/conf/hive-env.sh  
echo 'export HADOOP_HOME='${HADOOP_HOME}'  ##Hadoop安装路径' >> ${HIVE_HOME}/conf/hive-env.sh  
echo 'export HIVE_HOME='${HIVE_HOME}'  ##Hive安装路径'  >> ${HIVE_HOME}/conf/hive-env.sh  
echo 'export HIVE_CONF_DIR=$HIVE_HOME/conf  ##Hive配置文件路径' >> ${HIVE_HOME}/conf/hive-env.sh  

sudo rpm -Uvh platform-and-version-specific-package-name.rpm
sudo yum install mysql-community-server
sudo service mysqld start
sudo service mysqld status
sudo grep 'temporary password' /var/log/mysqld.log
mysql -uroot -p  
# ALTER USER 'root'@'localhost' IDENTIFIED BY 'MyNewPass4!';
ALTER USER 'root'@'localhost' IDENTIFIED BY 'Weibo_bigdata_4ds';
#port : 33601
create database hive;
create user hive identified by 'Weibo_bigdata_ds_4hive';
grant all PRIVILEGES on *.* to hive@'%' identified by 'Weibo_bigdata_ds_4hive';
flush privileges;

# cat ${HIVE_HOME}/conf/hive-default.xml.template > ${HIVE_HOME}/conf/hive-site.xml 

cp ${HIVE_HOME}/conf/hive-log4j2.properties.template ${HIVE_HOME}/conf/hive-log4j2.properties 

cp ${HIVE_HOME}/conf/hive-exec-log4j2.properties.template ${HIVE_HOME}/conf/hive-exec-log4j2.properties

ipMaster=10.85.125.173
ipSlaver1=10.85.125.174

echo '
<configuration>  
  
<property>
  <name>javax.jdo.option.ConnectionURL</name>
  <value>jdbc:mysql://'${ipMaster}':33601/hive?characterEncoding=UTF-8</value>
  <description>JDBC connect string for a JDBC metastore</description>
</property>

<property>
  <name>javax.jdo.option.ConnectionDriverName</name>
  <value>com.mysql.jdbc.Driver</value>
  <description>Driver class name for a JDBC metastore</description>
</property>
<property>
  <name>javax.jdo.option.ConnectionUserName</name>
  <value>hive</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionPassword</name>
  <value>Weibo_bigdata_ds_4hive</value>
</property>
<property>
  <name>hive.metastore.warehouse.dir</name>
  <!-- base hdfs path -->
  <value>/user/hive/warehouse</value>
  <description>location of default database for the warehouse</description>
</property>

<property>  
    <name>datanucleus.readOnlyDatastore</name>  
    <value>false</value>  
</property>  
<property>   
    <name>datanucleus.fixedDatastore</name>  
    <value>false</value>   
</property>  
<property>   
    <name>datanucleus.autoCreateSchema</name>   
    <value>true</value>   
</property>  
<property>  
    <name>datanucleus.autoCreateTables</name>  
    <value>true</value>  
</property>  
<property>  
    <name>datanucleus.autoCreateColumns</name>  
    <value>true</value>  
</property> 

<property>
   <name>hive.metastore.schema.verification</name>
   <value>false</value>
    <description>
    Enforce metastore schema version consistency.
    True: Verify that version information stored in metastore matches with one from Hive jars.  Also disable automatic
          schema migration attempt. Users are required to manully migrate schema after Hive upgrade which ensures
          proper metastore schema migration. (Default)
    False: Warn if the version information stored in metastore doesnt match with one from in Hive jars.
    </description>
 </property>

</configuration>  

' > ${HIVE_HOME}/conf/hive-site.xml 



echo 'rm -rf ./data.txt
touch data.txt
for((i=0;i<20000000;i++))
do
str='',name'';
name=${i}${str}${i}
#echo $name
echo  $name>> data.txt
done

echo ''show testdata''
cat data.txt' > data_create.sh

sh data_create.sh

drop table test_hive;
create table test_hive(id int,name string) row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '/home/hadoop/ksp/test_hive/data' OVERWRITE INTO TABLE test_hive;
