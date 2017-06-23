#!/bin/bash

size="120Y*10W"
date1=`date +%F_%H-%M-%S`
config="
  spark-submit \
  --jars /data0/work_space/service/spark-2.0.0-bin-hadoop2.4/jars/hadoop-lzo-0.4.15.jar \
  --master yarn \
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
  --class com.weibo.datasys.pipeline.Runner \
  weispark-ml-0.5.0-SNAPSHOT.jar pipeline.xml [5]
"
echo $config
$config
echo ===========================================
date2=`date +%F_%H-%M-%S`
echo ===========================================
echo date start : $date1
echo data size : $size
echo config : $config
echo date ended : $date2
