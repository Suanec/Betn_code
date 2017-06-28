#!/bin/sh

if [ $# -lt 1 ];then
    echo "usage: sh dist_copy.sh path"
    exit 1
fi

path=$1

src_hdfs=hdfs://10.87.216.197:8020
dest_hdfs=hdfs://10.85.123.43:9000
from_hdfs=/user/feed_weibo/pengyu7/feed_rank/spar_libsvm
/user/feed_weibo/pengyu7
to_hdfs=hdfs://10.85.123.43:9000$from_hdfs
# hadoop distcp /user/feed_weibo/pengyu7/feed_rank/auc_100bilion hdfs://10.85.123.43:9000/user/feed_weibo/pengyu7/feed_rank
cmd="$HADOOP_BIN distcp -m 100 -bandwidth 20 $from_hdfs $to_hdfs"
echo $cmd
$cmd

if [ $? -ne 0 ];then
    echo "emr namenode with ip:10.87.216.166 is standy, use ip:10.87.216.197 instead"
    from_hdfs=hdfs://10.87.216.197:8020$path
    cmd="$HADOOP_BIN distcp -m 100 -bandwidth 20 $from_hdfs $to_hdfs"
    echo $cmd
    
    $cmd
fi
