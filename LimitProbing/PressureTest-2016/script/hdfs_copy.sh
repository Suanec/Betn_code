#!/usr/bin/env bash


INPUT=hdfs://ns1/user/weibo_bigdata_ds/zhangtong1/hive/zhangtong1_ranking_with_user_info/dt=20161128
#OUTPUT=hdfs://10.77.16.121:9000/weibo_bigdata_ds/sample/dt=20161128

for turn in {0..500}; do
    echo "turn: $turn ..............."
	echo $INPUT
	OUTPUT_DIR=${OUTPUT}_${turn}
    echo $OUTPUT_DIR
    hadoop distcp $INPUT $OUTPUT_DIR
    echo "done."
done
