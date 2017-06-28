CentOS7安装Hadoop2.7完整步骤

[日期：2015-11-04] 来源：Linux社区  作者：Linux  [字体：大 中 小]
总体思路，准备主从服务器，配置主服务器可以无密码SSH登录从服务器，解压安装JDK，解压安装Hadoop，配置hdfs、mapreduce等主从关系。

1、环境，3台CentOS7，64位，Hadoop2.7需要64位Linux，CentOS7 Minimal的ISO文件只有600M，操作系统十几分钟就可以安装完成，
Master 192.168.0.182
 Slave1 192.168.0.183
 Slave2 192.168.0.184

2、SSH免密码登录，因为Hadoop需要通过SSH登录到各个节点进行操作，我用的是root用户，每台服务器都生成公钥，再合并到authorized_keys
(1)CentOS默认没有启动ssh无密登录，去掉/etc/ssh/sshd_config其中2行的注释，每台服务器都要设置，
#RSAAuthentication yes
#PubkeyAuthentication yes
(2)输入命令，ssh-keygen -t rsa，生成key，都不输入密码，一直回车，/root就会生成.ssh文件夹，每台服务器都要设置，
(3)合并公钥到authorized_keys文件，在Master服务器，进入/root/.ssh目录，通过SSH命令合并，
cat id_rsa.pub>> authorized_keys
ssh root@192.168.0.183 cat ~/.ssh/id_rsa.pub>> authorized_keys
ssh root@192.168.0.184 cat ~/.ssh/id_rsa.pub>> authorized_keys
(4)把Master服务器的authorized_keys、known_hosts复制到Slave服务器的/root/.ssh目录
(5)完成，ssh root@192.168.0.183、ssh root@192.168.0.184就不需要输入密码了

3、安装JDK，Hadoop2.7需要JDK7，由于我的CentOS是最小化安装，所以没有OpenJDK，直接解压下载的JDK并配置变量即可
(1)下载“jdk-7u79-linux-x64.gz”，放到/home/java目录下
(2)解压，输入命令，tar -zxvf jdk-7u79-linux-x64.gz
(3)编辑/etc/profile
export JAVA_HOME=/home/java/jdk1.7.0_79
export CLASSPATH=.:$JAVA_HOME/jre/lib/rt.jar:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
export PATH=$PATH:$JAVA_HOME/bin 
(4)使配置生效，输入命令，source /etc/profile
(5)输入命令，java -version，完成

4、安装Hadoop2.7，只在Master服务器解压，再复制到Slave服务器
(1)下载“hadoop-2.7.0.tar.gz”，放到/home/hadoop目录下
(2)解压，输入命令，tar -xzvf hadoop-2.7.0.tar.gz
(3)在/home/hadoop目录下创建数据存放的文件夹，tmp、hdfs、hdfs/data、hdfs/name

5、配置/home/hadoop/hadoop-2.7.0/etc/hadoop目录下的core-site.xml
 <configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://192.168.0.182:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>file:/home/hadoop/tmp</value>
    </property>
    <property>
        <name>io.file.buffer.size</name>
        <value>131702</value>
    </property>
 </configuration>

6、配置/home/hadoop/hadoop-2.7.0/etc/hadoop目录下的hdfs-site.xml
 <configuration>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:/home/hadoop/dfs/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:/home/hadoop/dfs/data</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>2</value>
    </property>
    <property>
        <name>dfs.namenode.secondary.http-address</name>
        <value>192.168.0.182:9001</value>
    </property>
    <property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
    </property>
 </configuration>

7、配置/home/hadoop/hadoop-2.7.0/etc/hadoop目录下的mapred-site.xml
 <configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.address</name>
        <value>192.168.0.182:10020</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.webapp.address</name>
        <value>192.168.0.182:19888</value>
    </property>
 </configuration>


 8、配置/home/hadoop/hadoop-2.7.0/etc/hadoop目录下的yarn-site.xml
 <configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.auxservices.mapreduce.shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>192.168.0.182:8032</value>
    </property>
    <property>
        <name>yarn.resourcemanager.scheduler.address</name>
        <value>192.168.0.182:8030</value>
    </property>
    <property>
        <name>yarn.resourcemanager.resource-tracker.address</name>
        <value>192.168.0.182:8031</value>
    </property>
    <property>
        <name>yarn.resourcemanager.admin.address</name>
        <value>192.168.0.182:8033</value>
    </property>
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <value>192.168.0.182:8088</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>768</value>
    </property>
 </configuration>

9、配置/home/hadoop/hadoop-2.7.0/etc/hadoop目录下hadoop-env.sh、yarn-env.sh的JAVA_HOME，不设置的话，启动不了，
export JAVA_HOME=/home/java/jdk1.7.0_79

10、配置/home/hadoop/hadoop-2.7.0/etc/hadoop目录下的slaves，删除默认的localhost，增加2个从节点，
192.168.0.183
192.168.0.184

11、将配置好的Hadoop复制到各个节点对应位置上，通过scp传送，
scp -r /home/hadoop 192.168.0.183:/home/
scp -r /home/hadoop 192.168.0.184:/home/

12、在Master服务器启动hadoop，从节点会自动启动，进入/home/hadoop/hadoop-2.7.0目录
(1)初始化，输入命令，bin/hdfs namenode -format
(2)全部启动sbin/start-all.sh，也可以分开sbin/start-dfs.sh、sbin/start-yarn.sh
(3)停止的话，输入命令，sbin/stop-all.sh
(4)输入命令，jps，可以看到相关信息

13、Web访问，要先开放端口或者直接关闭防火墙
(1)输入命令，systemctl stop firewalld.service
(2)浏览器打开http://192.168.0.182:8088/
(3)浏览器打开http://192.168.0.182:50070/

14、安装完成。这只是大数据应用的开始，之后的工作就是，结合自己的情况，编写程序调用Hadoop的接口，发挥hdfs、mapreduce的作用。