SPARK_BIN=spark-submit
TARGET_JAR=trainlr-0-1-0_2.11-0.1.0-SNAPSHOT.jar
MASTER_URL="yarn"
SRC_FILE=file:///home/hadoop/suanec/betn_code/data/data-libsvm
TARGET_FILE=file:///home/hadoop/suanec/betn_code/data/pressure/3w
W_NUM=10000
NUMDATACOUNT=`expr 100 \* $W_NUM`
FEATURESIZE=`expr 100 \* $W_NUM`
IDXCOUNT=903
SAVEPATH=/user/weibo_bigdata_ds/wulei3/shixi_enzhao/warehouse/libsvmfile/sampleData-`expr $NUMDATACOUNT / 10000`W-`expr $FEATURESIZE / 10000`W-${IDXCOUNT}-libsvm
echo NUMDATACOUNT = $NUMDATACOUNT
echo FEATURESIZE = $FEATURESIZE
echo IDXCOUNT = $IDXCOUNT
echo $SAVEPATH



$SPARK_BIN \
  --master $MASTER_URL \
  --deploy-mode client \
  --num-executors 10 \
  --driver-memory 3g \
  --executor-cores 2 \
  --executor-memory 2g\
  $TARGET_JAR $SAVEPATH

