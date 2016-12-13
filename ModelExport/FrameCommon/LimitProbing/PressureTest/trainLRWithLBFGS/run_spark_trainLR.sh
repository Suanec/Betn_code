SPARK_BIN=spark-submit
TARGET_JAR=trainlrwithsgd-0-1-0_2.11-0.1.0-SNAPSHOT.jar
MASTER_URL="yarn"
W_NUM=10000
NUMDATACOUNT=`expr 100 \* $W_NUM`
FEATURESIZE=`expr 100 \* $W_NUM`
IDXCOUNT=903
SAVEPATH=/user/weibo_bigdata_ds/wulei3/shixi_enzhao/warehouse/libsvmfile/sampleData-`expr $NUMDATACOUNT / 10000`W-`expr $FEATURESIZE / 10000`W-${IDXCOUNT}-libsvm
trainpath=/user/weibo_bigdata_ds/wulei3/shixi_enzhao/warehouse/libsvmfile/sampleData-120Y-10W-903-libsvm/*
testpath=/user/weibo_bigdata_ds/wulei3/shixi_enzhao/warehouse/libsvmfile/sampleData-120Y-10W-903-libsvm/test10W.libsvm


date1=`date +%F_%H-%M-%S`
config="
  $SPARK_BIN \
  --master $MASTER_URL \
  --deploy-mode client \
  --num-executors 100 \
  --driver-memory 7g \
  --executor-cores 4 \
  --executor-memory 7g \
  --conf spark.driver.maxResultSize=3g \
  --conf spark.ui.retainedJobs=2 \
  --conf spark.ui.retainedStages=2 \
  --conf spark.worker.ui.retainedExecutors=5 \
  --conf spark.worker.ui.retainedDrivers=5 \
  --conf spark.eventLog.enabled=false \
  $TARGET_JAR $trainpath $testpath
"
echo $config
$config
date2=`date +%F_%H-%M-%S`
echo date start : $date1
echo data size : $trainpath
echo $config
echo date ended : $date2


