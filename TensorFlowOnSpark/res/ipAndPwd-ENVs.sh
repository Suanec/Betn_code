#10.85.125.175   spark-test
#10.85.125.176   spark-test

cd /home
mkdir shixi_enzhao
cd shixi_enzhao
mkdir suanec
cd suanec
mkdir installs
mkdir libs
mkdir scripts
mkdir tmp
mkdir wsp
yum install vim-enhanced
yum install lrzsz
cd /home/shixi_enzhao/suanec/installs
pip install --upgrade pip
# wget -c -t 10 https://repo.continuum.io/archive/Anaconda2-4.4.0-Linux-x86_64.sh
wget -c -t 10 http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jdk-8u131-linux-x64.tar.gz
wget -c -t 10 https://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz
wget -c -t 10 http://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz 
wget -c -t 10 https://downloads.lightbend.com/scala/2.11.8/scala-2.11.8.tgz
wget -c -t 10 
wget -c -t 10 

ipMaster=10.85.125.175
ipSlaver1=10.85.125.176
mainPath=/home/hadoop/
useradd -d /home/hadoop -m hadoop -p hadoop
echo 'hadoop' | passwd --stdin hadoop
ssh localhost
cd ~/.ssh
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat id_rsa.pub >> authorized_keys
ssh root@10.85.125.175 
cat ~/.ssh/id_rsa.pub>> authorized_keys

chmod 755 ~
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
scp authorized_keys hadoop@${ipSlaver1}:/home/hadoop/.ssh/authorized_keys
sudo echo ${ipMaster}' iZ2zeh3rmvn71widtik73gZ' >> /etc/hosts
sudo echo ${ipSlaver1}' iZ2zeh3rmvn71widtik72kZ' >> /etc/hosts
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
cp $HADOOP_HOME/etc/hadoop/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml.origin
echo '
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->

<configuration>

  <property>
    <name>fs.default.name</name>
    <value>hdfs://iZ2zeh3rmvn71widtik73gZ:9000</value>
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

cp $HADOOP_HOME/etc/hadoop/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml.origin
echo '
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->

<configuration>
<!--  <property>  
      <name>dfs.namenode.http-address</name>  
      <value>iZ2zeh3rmvn71widtik73gZ:50070</value>  
      <description> fetch NameNode images and edits.注意主机名称 </description>  
  </property>
  <property>  
    <name>dfs.namenode.secondary.http-address</name>  
    <value>iZ2zeh3rmvn71widtik72kZ:50090</value>  
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
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->

<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <!--执行mapreduce的框架-->
<!--   <property>
      <name>mapreduce.jobhistory.address</name>
      <value>iZ2zeh3rmvn71widtik73gZ:10020</value>
  </property>
  <property>
      <name>mapreduce.jobhistory.webapp.address</name>
      <value>iZ2zeh3rmvn71widtik73gZ:19888</value>
  </proerty>-->
</configuration>

' > $HADOOP_HOME/etc/hadoop/mapred-site.xml


cp $HADOOP_HOME/etc/hadoop/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml.origin
echo '
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
<configuration>

<!-- Site specific YARN configuration properties -->

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
    <value>iZ2zeh3rmvn71widtik73gZ</value>
</property>

</configuration>

' > $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo 'iZ2zeh3rmvn71widtik72kZ' >> $HADOOP_HOME/etc/hadoop/slaves

# spark 
cp $HADOOP_HOME/etc/hadoop/slaves ${SPARK_HOME}/conf 
echo 'HADOOP_CONF_DIR='${HADOOP_HOME}/etc/hadoop >> ${SPARK_HOME}/conf/spark-env.sh


ssh hadoop@10.85.125.176
scp -r ~/libs hadoop@10.85.125.176: ~/
hadoop namenode –format
sh start-all.sh
${SPARK_HOME}/sbin/start-all.sh
