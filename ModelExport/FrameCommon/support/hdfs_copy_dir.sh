#/usr/bin/env bash

while getopts d:s:p: arg
do
  case ${arg} in
    d) HDFS_ROOT=${OPTARG};;
    s) HDFS_SOURCE_DIR=${OPTARG};;
    p) DIR_SUFFIX=${OPTARG};;
  esac
done

echo "Root: $HDFS_ROOT"

INPUT_DIR="$HDFS_ROOT/$HDFS_SOURCE_DIR"
echo "Input: $INPUT_DIR"

DATE=`date +%F_%H-%M-%S`
OUTPUT_DIR="$HDFS_ROOT/dt=${DATE}_$DIR_SUFFIX"
echo "Output: $OUTPUT_DIR"

echo "copy..."
hadoop fs -cp $INPUT_DIR $OUTPUT_DIR
echo "done."



#用法如下：
#bash hdfs_copy_dir.sh -d /user/weibo_bigdata_ds/facai/copy -s dt=20161102 -p 0
#Root: /user/weibo_bigdata_ds/facai/copy
#Input: /user/weibo_bigdata_ds/facai/copy/dt=20161102
#Output: /user/weibo_bigdata_ds/facai/copy/dt=2016-12-02_16-28-50_0
#copy...
#done.
