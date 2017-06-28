#!/bin/bash


src_hdfs=hdfs://10.87.216.197:8020
dest_hdfs=hdfs://10.85.123.43:9000

# /user/feed_weibo/warehouse/feed_data_v0/dt=20170619
# /user/feed_weibo/warehouse/feed_data_v0/dt=20170618

from_hdfs1=/user/feed_weibo/warehouse/feed_data_v0/dt=20170619
from_hdfs2=/user/feed_weibo/warehouse/feed_data_v0/dt=20170618
from_hdfs3=/user/feed_weibo/warehouse/feed_data_v0/dt=20170617
from_hdfs4=/user/feed_weibo/warehouse/feed_data_v0/dt=20170616
from_hdfs5=/user/feed_weibo/warehouse/feed_data_v0/dt=20170615
to_hdfs1=$dest_hdfs$from_hdfs1
to_hdfs2=$dest_hdfs$from_hdfs2
to_hdfs3=$dest_hdfs$from_hdfs3
to_hdfs4=$dest_hdfs$from_hdfs4
to_hdfs5=$dest_hdfs$from_hdfs5
# hadoop distcp /user/feed_weibo/pengyu7/feed_rank/auc_100bilion hdfs://10.85.123.43:9000/user/feed_weibo/pengyu7/feed_rank
# test_cmd="hadoop distcp -bandwidth 20 $from_hdfs $to_hdfs"
# test_cmd="hadoop fs -du -s -h /user/feed_weibo/warehouse/feed_data_v0"

echo ===========================================
cmd="hadoop distcp -m 100 -bandwidth 70 $from_hdfs1 $to_hdfs1"
hadoop fs -ls from_hdfs1
dateBeforeStart=`date +%F_%H-%M-%S`
echo $cmd
# $cmd
dataAfterStart=`date +%F_%H-%M-%S`

dataAfterEnd=`date +%F_%H-%M-%S`
echo ===========================================
echo dateBeforestart : $dateBeforestart
echo dataAfterStart : $dataAfterStart
echo dataAfterEnd : $dataAfterEnd

echo ===========================================
cmd="hadoop distcp -m 100 -bandwidth 70 $from_hdfs2 $to_hdfs2"
hadoop fs -ls from_hdfs1
dateBeforeStart=`date +%F_%H-%M-%S`
echo $cmd
# $cmd
dataAfterStart=`date +%F_%H-%M-%S`

dataAfterEnd=`date +%F_%H-%M-%S`
echo ===========================================
echo dateBeforestart : $dateBeforestart
echo dataAfterStart : $dataAfterStart
echo dataAfterEnd : $dataAfterEnd

echo ===========================================
cmd="hadoop distcp -m 100 -bandwidth 70 $from_hdfs3 $to_hdfs3"
hadoop fs -ls from_hdfs1
dateBeforeStart=`date +%F_%H-%M-%S`
echo $cmd
# $cmd
dataAfterStart=`date +%F_%H-%M-%S`

dataAfterEnd=`date +%F_%H-%M-%S`
echo ===========================================
echo dateBeforestart : $dateBeforestart
echo dataAfterStart : $dataAfterStart
echo dataAfterEnd : $dataAfterEnd

echo ===========================================
cmd="hadoop distcp -m 100 -bandwidth 70 $from_hdfs4 $to_hdfs4"
hadoop fs -ls from_hdfs1
dateBeforeStart=`date +%F_%H-%M-%S`
echo $cmd
# $cmd
dataAfterStart=`date +%F_%H-%M-%S`

dataAfterEnd=`date +%F_%H-%M-%S`
echo ===========================================
echo dateBeforestart : $dateBeforestart
echo dataAfterStart : $dataAfterStart
echo dataAfterEnd : $dataAfterEnd

echo ===========================================
cmd="hadoop distcp -m 100 -bandwidth 70 $from_hdfs5 $to_hdfs5"
hadoop fs -ls from_hdfs1
dateBeforeStart=`date +%F_%H-%M-%S`
echo $cmd
# $cmd
dataAfterStart=`date +%F_%H-%M-%S`

dataAfterEnd=`date +%F_%H-%M-%S`
echo ===========================================
echo dateBeforestart : $dateBeforestart
echo dataAfterStart : $dataAfterStart
echo dataAfterEnd : $dataAfterEnd



