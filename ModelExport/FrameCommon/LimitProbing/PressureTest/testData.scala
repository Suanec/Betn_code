import com.weibo.datasys._
import com.weibo.datasys.etl._
import com.weibo.datasys.pipeline._
import com.weibo.datasys.macros._
import com.weibo.datasys.common._
import XmlConfig._

//// val src_data = """hdfs://ns1/user/weibo_bigdata_ds/zhangtong1/hive/zhangtong1_ranking_with_user_info/dt=20161128"""
//// val tar_data = """hdfs://ns1/user/weibo_bigdata_ds/wulei3/shixi_enzhao/hive/"""
//// val src_data = """hdfs://ns1/user/weibo_bigdata_ds/warehouse/mds_datastrategy_unread_pool_ctr_feature_all"""
/**
/// hadoop fs -cp hdfs://ns1/user/weibo_bigdata_ds/warehouse/mds_datastrategy_unread_pool_ctr_feature_all hdfs://ns1/user/weibo_bigdata_ds/wulei3/shixi_enzhao/warehouse/mds_datastrategy_unread_pool_ctr_feature_all
hive select count(*) from mds_datastrategy_unread_pool_ctr_feature_all = 3008716749

**/
// object testData {
  val src_data_zt = """hdfs://ns1/user/weibo_bigdata_ds/zhangtong1/hive/zhangtong1_ranking_with_user_info/dt=20161128"""
  val src_data_30Y = """hdfs://ns1/user/weibo_bigdata_ds/warehouse/mds_datastrategy_unread_pool_ctr_feature_all"""

  val data = sc.textFile(src_data_30Y)


  
// }