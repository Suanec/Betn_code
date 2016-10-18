///author := "suanec_Betn"
///data := 20160913
import org.json4s._
import org.json4s.native._
import org.json4s._
import org.json4s.native.JsonMethods._
import scala.io.Source
import java.io.File

object JsonHelper{
  val confPath = """
    /data1/work/datasys/weispark/runtime/107716109/monitor/
    """
  val resPath = """
    /data1/work/datasys/weispark/runtime/107716109/monitor/
    """
  implicit val formats = Serialization.formats(ShortTypeHints(List()))
  def main(args : Array[String]){
    val path = new File("").getAbsolutePath
    val file = new File(path + "\\conf\\config.json")
    val conf = io.Source.fromFile(file)("utf-8").mkString
    val jsonConf = parse(conf)
    val jsonRst = pretty(render(jsonConf))
    val writter = new java.io.PrintWriter(path + "\\rst\\rst.json", "utf-8")
    writter.write(jsonRst)
    writter.flush
    writter.close
  }


}  



