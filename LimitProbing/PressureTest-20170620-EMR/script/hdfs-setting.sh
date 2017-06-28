ipFrom=10.85.125.175
ipMaster=10.85.123.43
ipSlaver1=10.85.123.44
ipSlaver2=10.85.123.45
ipSlaver3=10.85.123.46
mainPath=/home/hadoop/
useradd -d /home/hadoop -m hadoop -p hadoop
echo 'hadoop' | passwd --stdin hadoop
echo 'Weibo_bigdata_hdfs4feed' | passwd --stdin hadoop 
ssh localhost
cd ~/.ssh
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat id_rsa.pub >> authorized_keys

# cat ~/.ssh/id_rsa.pub>> authorized_keys
chmod 755 ~
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
scp authorized_keys hadoop@${ipSlaver1}:/home/hadoop/.ssh/authorized_keys
ssh $ipSlaver1
scp authorized_keys hadoop@${ipSlaver2}:/home/hadoop/.ssh/authorized_keys
ssh $ipSlaver2
scp authorized_keys hadoop@${ipSlaver3}:/home/hadoop/.ssh/authorized_keys
ssh $ipSlaver3

mkdir ~/libs
cp ~/transportInstalls/*.*gz ~/libs
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
echo 'alias hadoop-start=${HADOOP_HOME}/sbin/start-all.sh' >> ~/libs/env.sh
echo 'alias hadoop-restart="hadoop-stop;hadoop-start"' >> ~/libs/env.sh
echo 'alias spark-example=${SPARK_HOME}/bin/run-example' >> ~/libs/env.sh
echo 'ipFrom=10.85.125.175' >> ~/libs/env.sh
echo 'ipMaster=10.85.123.43' >> ~/libs/env.sh
echo 'ipSlaver1=10.85.123.44' >> ~/libs/env.sh
echo 'ipSlaver2=10.85.123.45' >> ~/libs/env.sh
echo 'ipSlaver3=10.85.123.46' >> ~/libs/env.sh
echo 'mainPath=/home/hadoop/' >> ~/libs/env.sh

echo "alias fsls='hadoop fs -ls '" >> ~/libs/env.sh
echo "alias fsmv='hadoop fs -mv '" >> ~/libs/env.sh
echo "alias fsrm='hadoop fs -rm '" >> ~/libs/env.sh
echo "alias fslsme='hadoop fs -ls $hadopath'" >> ~/libs/env.sh
echo "alias fscat='hadoop fs -cat '" >> ~/libs/env.sh
echo "alias fs='hadoop fs'" >> ~/libs/env.sh
echo "alias fsmkdir='hadoop fs -mkdir'" >> ~/libs/env.sh
echo "alias show='sh show.sh '" >> ~/libs/env.sh

source ~/libs/env.sh

mkdir /datab/hadoop
mkdir /datac/hadoop
mkdir /datad/hadoop
mkdir /datae/hadoop
mkdir /dataf/hadoop
mkdir /datag/hadoop
mkdir /datah/hadoop
mkdir /datai/hadoop
mkdir /dataj/hadoop
mkdir /datak/hadoop
mkdir /datal/hadoop
mkdir /datam/hadoop
mkdir /datan/hadoop
mkdir /datao/hadoop
mkdir /datap/hadoop
mkdir /dataq/hadoop
mkdir /datab/hadoop/tmp
mkdir /datac/hadoop/tmp
mkdir /datad/hadoop/tmp
mkdir /datae/hadoop/tmp
mkdir /dataf/hadoop/tmp
mkdir /datag/hadoop/tmp
mkdir /datah/hadoop/tmp
mkdir /datai/hadoop/tmp
mkdir /dataj/hadoop/tmp
mkdir /datak/hadoop/tmp
mkdir /datal/hadoop/tmp
mkdir /datam/hadoop/tmp
mkdir /datan/hadoop/tmp
mkdir /datao/hadoop/tmp
mkdir /datap/hadoop/tmp
mkdir /dataq/hadoop/tmp
mkdir /datab/hadoop/data
mkdir /datac/hadoop/data
mkdir /datad/hadoop/data
mkdir /datae/hadoop/data
mkdir /dataf/hadoop/data
mkdir /datag/hadoop/data
mkdir /datah/hadoop/data
mkdir /datai/hadoop/data
mkdir /dataj/hadoop/data
mkdir /datak/hadoop/data
mkdir /datal/hadoop/data
mkdir /datam/hadoop/data
mkdir /datan/hadoop/data
mkdir /datao/hadoop/data
mkdir /datap/hadoop/data
mkdir /dataq/hadoop/data
mkdir /datab/hadoop/mapred
mkdir /datac/hadoop/mapred
mkdir /datad/hadoop/mapred
mkdir /datae/hadoop/mapred
mkdir /dataf/hadoop/mapred
mkdir /datag/hadoop/mapred
mkdir /datah/hadoop/mapred
mkdir /datai/hadoop/mapred
mkdir /dataj/hadoop/mapred
mkdir /datak/hadoop/mapred
mkdir /datal/hadoop/mapred
mkdir /datam/hadoop/mapred
mkdir /datan/hadoop/mapred
mkdir /datao/hadoop/mapred
mkdir /datap/hadoop/mapred
mkdir /dataq/hadoop/mapred
mkdir /datab/hadoop/data/hdfs
mkdir /datac/hadoop/data/hdfs
mkdir /datad/hadoop/data/hdfs
mkdir /datae/hadoop/data/hdfs
mkdir /dataf/hadoop/data/hdfs
mkdir /datag/hadoop/data/hdfs
mkdir /datah/hadoop/data/hdfs
mkdir /datai/hadoop/data/hdfs
mkdir /dataj/hadoop/data/hdfs
mkdir /datak/hadoop/data/hdfs
mkdir /datal/hadoop/data/hdfs
mkdir /datam/hadoop/data/hdfs
mkdir /datan/hadoop/data/hdfs
mkdir /datao/hadoop/data/hdfs
mkdir /datap/hadoop/data/hdfs
mkdir /dataq/hadoop/data/hdfs
mkdir /datab/hadoop/data/hdfs/namenode/
mkdir /datac/hadoop/data/hdfs/namenode/
mkdir /datad/hadoop/data/hdfs/namenode/
mkdir /datae/hadoop/data/hdfs/namenode/
mkdir /dataf/hadoop/data/hdfs/namenode/
mkdir /datag/hadoop/data/hdfs/namenode/
mkdir /datah/hadoop/data/hdfs/namenode/
mkdir /datai/hadoop/data/hdfs/namenode/
mkdir /dataj/hadoop/data/hdfs/namenode/
mkdir /datak/hadoop/data/hdfs/namenode/
mkdir /datal/hadoop/data/hdfs/namenode/
mkdir /datam/hadoop/data/hdfs/namenode/
mkdir /datan/hadoop/data/hdfs/namenode/
mkdir /datao/hadoop/data/hdfs/namenode/
mkdir /datap/hadoop/data/hdfs/namenode/
mkdir /dataq/hadoop/data/hdfs/namenode/
mkdir /datab/hadoop/data/hdfs/datanode/
mkdir /datac/hadoop/data/hdfs/datanode/
mkdir /datad/hadoop/data/hdfs/datanode/
mkdir /datae/hadoop/data/hdfs/datanode/
mkdir /dataf/hadoop/data/hdfs/datanode/
mkdir /datag/hadoop/data/hdfs/datanode/
mkdir /datah/hadoop/data/hdfs/datanode/
mkdir /datai/hadoop/data/hdfs/datanode/
mkdir /dataj/hadoop/data/hdfs/datanode/
mkdir /datak/hadoop/data/hdfs/datanode/
mkdir /datal/hadoop/data/hdfs/datanode/
mkdir /datam/hadoop/data/hdfs/datanode/
mkdir /datan/hadoop/data/hdfs/datanode/
mkdir /datao/hadoop/data/hdfs/datanode/
mkdir /datap/hadoop/data/hdfs/datanode/
mkdir /dataq/hadoop/data/hdfs/datanode/
echo '
rm -rf /home/hadoop/libs/hadoop-2.7.3/logs/*

rm -rf /datab/hadoop/tmp
rm -rf /datac/hadoop/tmp
rm -rf /datad/hadoop/tmp
rm -rf /datae/hadoop/tmp
rm -rf /dataf/hadoop/tmp
rm -rf /datag/hadoop/tmp
rm -rf /datah/hadoop/tmp
rm -rf /datai/hadoop/tmp
rm -rf /dataj/hadoop/tmp
rm -rf /datak/hadoop/tmp
rm -rf /datal/hadoop/tmp
rm -rf /datam/hadoop/tmp
rm -rf /datan/hadoop/tmp
rm -rf /datao/hadoop/tmp
rm -rf /datap/hadoop/tmp
rm -rf /dataq/hadoop/tmp

rm -rf /datab/hadoop/data/hdfs/datanode/*
rm -rf /datac/hadoop/data/hdfs/datanode/*
rm -rf /datad/hadoop/data/hdfs/datanode/*
rm -rf /datae/hadoop/data/hdfs/datanode/*
rm -rf /dataf/hadoop/data/hdfs/datanode/*
rm -rf /datag/hadoop/data/hdfs/datanode/*
rm -rf /datah/hadoop/data/hdfs/datanode/*
rm -rf /datai/hadoop/data/hdfs/datanode/*
rm -rf /dataj/hadoop/data/hdfs/datanode/*
rm -rf /datak/hadoop/data/hdfs/datanode/*
rm -rf /datal/hadoop/data/hdfs/datanode/*
rm -rf /datam/hadoop/data/hdfs/datanode/*
rm -rf /datan/hadoop/data/hdfs/datanode/*
rm -rf /datao/hadoop/data/hdfs/datanode/*
rm -rf /datap/hadoop/data/hdfs/datanode/*
rm -rf /dataq/hadoop/data/hdfs/datanode/*

rm -rf /datab/hadoop/data/hdfs/namenode/*
rm -rf /datac/hadoop/data/hdfs/namenode/*
rm -rf /datad/hadoop/data/hdfs/namenode/*
rm -rf /datae/hadoop/data/hdfs/namenode/*
rm -rf /dataf/hadoop/data/hdfs/namenode/*
rm -rf /datag/hadoop/data/hdfs/namenode/*
rm -rf /datah/hadoop/data/hdfs/namenode/*
rm -rf /datai/hadoop/data/hdfs/namenode/*
rm -rf /dataj/hadoop/data/hdfs/namenode/*
rm -rf /datak/hadoop/data/hdfs/namenode/*
rm -rf /datal/hadoop/data/hdfs/namenode/*
rm -rf /datam/hadoop/data/hdfs/namenode/*
rm -rf /datan/hadoop/data/hdfs/namenode/*
rm -rf /datao/hadoop/data/hdfs/namenode/*
rm -rf /datap/hadoop/data/hdfs/namenode/*
rm -rf /dataq/hadoop/data/hdfs/namenode/*
' > ~/libs/cleanHadoop.sh 
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
    <value>file:///datab/hadoop/tmp,file:///datac/hadoop/tmp,file:///datad/hadoop/tmp,file:///datae/hadoop/tmp,file:///dataf/hadoop/tmp,file:///datag/hadoop/tmp,file:///datah/hadoop/tmp,file:///datai/hadoop/tmp,file:///dataj/hadoop/tmp,file:///datak/hadoop/tmp,file:///datal/hadoop/tmp,file:///datam/hadoop/tmp,file:///datan/hadoop/tmp,file:///datao/hadoop/tmp,file:///datap/hadoop/tmp,file:///dataq/hadoop/tmp</value>
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
  <property>  
    <name>dfs.namenode.secondary.http-address</name>  
    <value>'${ipSlaver1}':50090</value>  
    <description> fetch SecondNameNode fsimage </description>  
  </property>

  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <!--配置备份数-->
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:/datab/hadoop/data/hdfs/namenode/,file:/datac/hadoop/data/hdfs/namenode/,file:/datad/hadoop/data/hdfs/namenode/,file:/datae/hadoop/data/hdfs/namenode/,file:/dataf/hadoop/data/hdfs/namenode/,file:/datag/hadoop/data/hdfs/namenode/,file:/datah/hadoop/data/hdfs/namenode/,file:/datai/hadoop/data/hdfs/namenode/,file:/dataj/hadoop/data/hdfs/namenode/,file:/datak/hadoop/data/hdfs/namenode/,file:/datal/hadoop/data/hdfs/namenode/,file:/datam/hadoop/data/hdfs/namenode/,file:/datan/hadoop/data/hdfs/namenode/,file:/datao/hadoop/data/hdfs/namenode/,file:/datap/hadoop/data/hdfs/namenode/,file:/dataq/hadoop/data/hdfs/namenode/</value>
  </property>
  <!--namenode数据存放地址-->

  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:/datab/hadoop/data/hdfs/datanode/,file:/datac/hadoop/data/hdfs/datanode/,file:/datad/hadoop/data/hdfs/datanode/,file:/datae/hadoop/data/hdfs/datanode/,file:/dataf/hadoop/data/hdfs/datanode/,file:/datag/hadoop/data/hdfs/datanode/,file:/datah/hadoop/data/hdfs/datanode/,file:/datai/hadoop/data/hdfs/datanode/,file:/dataj/hadoop/data/hdfs/datanode/,file:/datak/hadoop/data/hdfs/datanode/,file:/datal/hadoop/data/hdfs/datanode/,file:/datam/hadoop/data/hdfs/datanode/,file:/datan/hadoop/data/hdfs/datanode/,file:/datao/hadoop/data/hdfs/datanode/,file:/datap/hadoop/data/hdfs/datanode/,file:/dataq/hadoop/data/hdfs/datanode/</value>
  </property>
  <!--datanode 数据存放地址-->
  <property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>file:///datab/hadoop/tmp,file:///datac/hadoop/tmp,file:///datad/hadoop/tmp,file:///datae/hadoop/tmp,file:///dataf/hadoop/tmp,file:///datag/hadoop/tmp,file:///datah/hadoop/tmp,file:///datai/hadoop/tmp,file:///dataj/hadoop/tmp,file:///datak/hadoop/tmp,file:///datal/hadoop/tmp,file:///datam/hadoop/tmp,file:///datan/hadoop/tmp,file:///datao/hadoop/tmp,file:///datap/hadoop/tmp,file:///dataq/hadoop/tmp</value>
    <description>A base for other temporary directories.</description>
  </property>
  <!--用于临时文件夹的基础路径-->
  <property>
    <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
    <value>false</value>
  </property>
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
  <property>
    <name>mapred.local.dir</name>
    <value>file:///datab/hadoop/mapred,file:///datac/hadoop/mapred,file:///datad/hadoop/mapred,file:///datae/hadoop/mapred,file:///dataf/hadoop/mapred,file:///datag/hadoop/mapred,file:///datah/hadoop/mapred,file:///datai/hadoop/mapred,file:///dataj/hadoop/mapred,file:///datak/hadoop/mapred,file:///datal/hadoop/mapred,file:///datam/hadoop/mapred,file:///datan/hadoop/mapred,file:///datao/hadoop/mapred,file:///datap/hadoop/mapred,file:///dataq/hadoop/mapred</value>
  </property>

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
    <value>48</value>
    <final>true</final>
  </property>
  <!-- container manager 使用的线程数，默认20 -->

  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>24</value>
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
    <value>22528</value>
    <final>true</final>
  </property>
  <!--允许给每一个container分配的最大内存，单位MB，默认8192 -->

  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>112640</value>
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
echo ${ipSlaver2} >> $HADOOP_HOME/etc/hadoop/slaves
echo ${ipSlaver3} >> $HADOOP_HOME/etc/hadoop/slaves

# spark 
cp $HADOOP_HOME/etc/hadoop/slaves ${SPARK_HOME}/conf 
echo 'HADOOP_CONF_DIR='${HADOOP_HOME}/etc/hadoop >> ${SPARK_HOME}/conf/spark-env.sh

# rm -rf ~/libs/

ssh hadoop@${ipSlaver1}
ssh hadoop@${ipSlaver2}
ssh hadoop@${ipSlaver3}
scp -r ~/libs hadoop@${ipSlaver1}:~/
scp -r ~/libs hadoop@${ipSlaver2}:~/
scp -r ~/libs hadoop@${ipSlaver3}:~/

hadoop namenode -format
sh start-all.sh
${SPARK_HOME}/sbin/start-all.sh

scp env.sh hadoop@${ipSlaver1}:/home/hadoop/libs/env.sh
scp env.sh hadoop@${ipSlaver2}:/home/hadoop/libs/env.sh
scp env.sh hadoop@${ipSlaver3}:/home/hadoop/libs/env.sh

scp /home/hadoop/libs/hadoop-2.7.3/etc/hadoop/yarn-site.xml hadoop@${ipSlaver1}:/home/hadoop/libs/hadoop-2.7.3/etc/hadoop/yarn-site.xml
scp /home/hadoop/libs/hadoop-2.7.3/etc/hadoop/yarn-site.xml hadoop@${ipSlaver2}:/home/hadoop/libs/hadoop-2.7.3/etc/hadoop/yarn-site.xml
scp /home/hadoop/libs/hadoop-2.7.3/etc/hadoop/yarn-site.xml hadoop@${ipSlaver3}:/home/hadoop/libs/hadoop-2.7.3/etc/hadoop/yarn-site.xml


scp path hadoop@${ipSlaver1}:path