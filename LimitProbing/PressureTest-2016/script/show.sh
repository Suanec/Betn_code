#####/usr/bin/env bash
#
#while getopts n:f: arg
#do
#  case ${arg} in
#    n) numCount=${OPTARG};;
#    f) featureSize=${OPTARG};;
#  esac
#done

numCount=$1
featureSize=$2
echo "numCount : $numCount"
echo "featureSize : $featureSize"

targetDir="/user/weibo_bigdata_ds/wulei3/shixi_enzhao/warehouse/libsvmfile/sampleData-$numCount-$featureSize-903-libsvm/"
echo "targetDir : $targetDir"
hadoop fs -ls $targetDir


#DATE=`date +%F_%H-%M-%S`
