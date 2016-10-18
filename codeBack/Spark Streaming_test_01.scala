$SPARK_DIR/bin/spark-shell
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
val ssc = new StreamingContext(sc, Seconds(10))
val lines = ssc.socketTextStream("10.237.62.52",15209)
val words = lines.flatMap( _.split(" ") )
val pairs = words.map( word => (word -> 1) )
val wordCount = pairs.reduceByKey(_+_)
wordCount.print
ssc.start 
ssc.awaitTermination