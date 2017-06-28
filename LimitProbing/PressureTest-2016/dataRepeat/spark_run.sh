SPARK_BIN=spark-submit
TARGET_JAR=datarepeat-0-1-0_2.11-0.1.0-SNAPSHOT.jar
CLASS_NAME=dataRepeat
MASTER_URL="local[2]"
SRC_FILE=file:///home/hadoop/suanec/betn_code/data/data-libsvm
TARGET_FILE=file:///home/hadoop/suanec/betn_code/data/pressure/3w
TARGET_NUM=30000

$SPARK_BIN $TARGET_JAR $SRC_FILE $TARGET_FILE $TARGET_NUM

