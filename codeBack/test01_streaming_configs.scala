import java.io.File
val path = new File("").getAbsolutePath
val fs = new File(path).listFiles
var count = 0
fs.map{
  s =>
  count += 1
  s -> (count - 1)
}.foreach(println)
val file = /data1/work/datasys/weispark/runtime/107716109/framework-init/redislist2redislist.xml
val conf = xml.XML.load(file.toString)
val sparkConf = conf \ "spark" \ "property"
val sparkConfMap = sparkConf.map{
  x =>
  ( x \ "@name" ).text -> x.text
}


import scala.reflect.runtime.universe
import org.apache.spark.streaming.StreamingContext
object TaskType extends Enumeration {
  val INPUT = Value("input")
  val OUTPUT = Value("output")
  val PROCESS = Value("process")
}
// object Margin extends Enumeration {
//          type Margin = Value
//          val TOP, BOTTOM, LEFT, RIGHT = Value
//        }

class TaskConfig(val objectname: String, val tasktype: TaskType.Value, val parameters: Map[String, String]) {}
class SparkConfig(val appname: String, val serializer: String) {}
class SparkStreamingConfig(val blockInterval: Long, val blockQueueSize: Long) {} 
class StreamingTask(val id: Int, val preid: Int, val taskcon: TaskConfig) {}
class StreamingConfig(val strname: String, val stasks: Array[StreamingTask]) {
  def run(ssc: StreamingContext) {
    val ordered = stasks.sortWith((x, y) => if (x.id <= y.id) true else false)
    val dstreams = scala.collection.mutable.Map.empty[Int, AnyRef]
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    for (task <- ordered) {
      val id = task.id
      val preid = task.preid
      val taskcon = task.taskcon
      val taskobj = runtimeMirror.reflectModule(runtimeMirror.staticModule(taskcon.objectname)).
        instance.asInstanceOf[StreamingRunnable]
      dstreams(id) = if (preid <= 0)  taskobj.run(ssc, None, taskcon.parameters)
                     else taskobj.run(ssc, dstreams(preid), taskcon.parameters)
    }
   }
}
trait StreamingRunnable {
  def run(ssc: StreamingContext, input: AnyRef, conf: Map[String, String]): AnyRef
}

object XmlConfig {
  def loadSparkConf(source: xml.Elem): Map[String,String] = {
    (source \ "spark" \ "property").map { x => ((x \ "@name").text, x.text)}.toMap
  }
  def loadSparkStreamingConf(source: xml.Elem): Map[String,String]= {
    (source \ "sparkstreaming" \ "property").map { x => ((x \ "@name").text, x.text)}.toMap
  }

  // def loadTaskConf(source: xml.Elem): Map[String, TaskConfig] = {
  //   val inputtasks = (source \ "inputs" \ "input").map { x =>
  //     {
  //       val name = (x \ "@name").text
  //       val objectname = (x \ "objectname").text
  //       val tasktype = TaskType.withName("input")
  //       val args = (x \ "args" \ "arg").map { x => ((x \ "@name").text, x.text) }.toMap
  //       (name, new TaskConfig(objectname, tasktype, args.toMap))
  //     }
  //   }.toMap
  //   val processtasks = (source \ "processes" \ "process").map { x =>
  //     {
  //       val name = (x \ "@name").text
  //       val objectname = (x \ "objectname").text
  //       val tasktype = TaskType.withName("process")
  //       val args = (x \ "args" \ "arg").map { x => ((x \ "@name").text, x.text) }.toMap
  //       (name, new TaskConfig(objectname, tasktype, args.toMap))
  //     }
  //   }.toMap
  //   val outputtasks = (source \ "outputs" \ "output").map { x =>
  //     {
  //       val name = (x \ "@name").text
  //       val objectname = (x \ "objectname").text
  //       val tasktype = TaskType.withName("output")
  //       val args = (x \ "args" \ "arg").map { x => ((x \ "@name").text, x.text) }.toMap
  //       (name, new TaskConfig(objectname, tasktype, args.toMap))
  //     }
  //   }.toMap
  //   inputtasks ++ processtasks ++ outputtasks
  // }
  def loadKafkasConf(source: xml.Elem): Map[String, Map[String, String]] = {
    (source \ "kafkas" \ "kafka").map {
      kafka =>
        {
          val name = (kafka \ "@name").text
          val kc =  (kafka \ "property").map { x => ((x \ "@name").text, x.text)}.toMap
          (name, kc)
        }
    }.toMap
  }
  // def loadStreamsConf(source: xml.Elem): Map[String, StreamingConfig] = {
  //   val streams = loadKafkasConf(source)
  //   val tasks = loadTaskConf(source)
  //   (source \ "streams" \ "stream").map { s =>
  //     {
  //       val name = (s \ "@name").text
  //       val input = Array(new StreamingTask(1, 0, tasks((s \ "input").text)))
  //       val processes = (s \ "process").map { process =>
  //         new StreamingTask(
  //           (process \ "@id").text.toInt,
  //           (process \ "@preid").text.toInt,
  //           tasks(process.text))
  //       }
  //       val outputs = (s \ "output").map { process =>
  //         new StreamingTask(
  //           (process \ "@id").text.toInt,
  //           (process \ "@preid").text.toInt,
  //           tasks(process.text))
  //       }
  //       val stasks = input ++ processes ++ outputs
  //       (name, new StreamingConfig(name, stasks))
  //     }
  //   }.toMap
  // }
  // def main(args: Array[String]) {
  //   val source = xml.XML.load("./config.xml")
  //   val conf = loadStreamsConf(source)
  //   for ((name, con) <- conf)
  //     con.run(null);
  // }
}

