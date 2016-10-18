///author := "suanec_Betn"
///data := 20160913
// package weiSpark.monitor 
import org.jsoup._
import scala.collection.JavaConverters._
import collection.mutable.Buffer
// import JsonHelper._ 

object MonitorSparkUI{

  val url_0 = "http://h107716109.cluster.ds.weibo.com:8088"
  /// grammar sugar
  def get( _url : String , _timeOut : Int = 1000*60*5 ) = Jsoup.connect(_url).timeout(_timeOut).get
  /// show the given Array
  def showArray( _arr : Array[_ <: Any] ) = _arr foreach println
  def showArrWithLineNum( _arr : Array[_ <: Any] ) = {
    var count = 0
    val arr = _arr.map{
      k =>
        count += 1
        ((count - 1),k)
    }
    showArray(arr)
  }
  /// grammar sugar 
  def showArr( _arr : Array[_ <: Any] ) = showArrWithLineNum(_arr)
  /// generate RUNNING page URL from UI root URL
  def genRunningUrl( _rootUrl : String ) : String = _rootUrl + "/cluster/apps/RUNNING"
  /// get Job's ID from webUI's RUNNING page
  def getJobId( _running_Url : String ) : (String,String) = {
    val doc = get(_running_Url)
    val content = doc.getElementsByClass("content")
    val list = content.first.getAllElements.asScala.init
    val str = list.last.toString
    val splits = str.split("\"")
    val split = splits.init.last
    val id_elem = Jsoup.parse(split)
    val id_url = id_elem.body.select("a").attr("href")
    val id_rst = id_url.split('/').last
    (id_url->id_rst)
  }
  /// get Jobs' ID from webUI's RUNNING page with many jobs
  def getJobsId( _running_Url : String ) : Array[(String,String)] = {
    val doc = get(_running_Url)
    // val table = doc.select("table").last
    val table = doc.select(".ui")
    val scriptStr = table.first.toString
    val jobsStr = scriptStr.split("[\\[^\\]]").filter(_.size > 210)
    val jobsElem = jobsStr.map(Jsoup.parse)
    val jobsUrl = jobsElem.map(_.select("a:contains(master)").attr("href"))
    val jobsId = jobsUrl.map( _.split('/').last)
    val jobsRst = new Array[(String,String)](jobsUrl.size)
    (0 until jobsUrl.size).indices.map{
      i =>
        jobsRst(i) = jobsUrl(i) -> jobsId(i)
    }.toArray
    jobsRst
  }
  /// gen Streaming URL from id_url
  def idUrl2StreamingUrl( _id_url : String ) : String = _id_url + "streaming/"
  /// get rst from StreamingUrl , bool config can let the rst show on screen or not
  def streamingUrl2Rst( _streamUrl : String , _isPrint : Boolean = true ) : Array[String] = {
    val doc = get(_streamUrl)
    val ulunstyled = doc.select("ul.unstyled")
    val content = ulunstyled.first
    val rst = content.select("li").asScala.map(_.text).toArray
    if(_isPrint) showArray(rst)
    rst
  }
  /// get rst from spark main page , bool config can let the rst show on screen or not
  def getStreamingJobInfoAtMain( _id_url : String , _isPrint : Boolean = true ) : Array[String] = {
    val doc = get(_id_url)
    val content = doc.body.select("ul.unstyled").first
    val rst = content.select("li").asScala.map(_.text).toArray
    if(_isPrint) showArray(rst)
    rst
  }
  /// get spark streaming job info from running ui pages
  def getSparkStreamingJobInfo( _running_url : String ) : Array[String] = {
    val (id_url,id) = getJobId(_running_url)
    val streamingUrl = idUrl2StreamingUrl(id_url)
    val rst1 = getStreamingJobInfoAtMain(id_url,false)
    val rst2 = streamingUrl2Rst(streamingUrl,false)
    val rst = rst1 ++: rst2
    rst
  }
  /// get spark streaming jobs info from running ui pages Many Jobs
  def getSparkStreamingJobsInfo( _running_url : String ) : Array[(String,Array[String])] = {
    // val (id_url,id) = getJobId(_running_url)
    val jobsId = getJobsId(_running_url)
    // val streamingUrl = idUrl2StreamingUrl(id_url)
    // val jobsStreamingUrl = jobsId.map( x => idUrl2StreamingUrl(x._1))
    // val rst1 = getStreamingJobInfoAtMain(id_url,false)
    // val rst2 = streamingUrl2Rst(streamingUrl,false)
    // val rst = rst1 ++: rst2
    jobsId.map{
      pair =>
        val id_url = pair._1
        val id = pair._2
        println(id)
        val streamingUrl = idUrl2StreamingUrl(id_url)
        val rst = getStreamingJobInfoAtMain(id_url,false) ++: streamingUrl2Rst(streamingUrl,false)
        (id,rst)
    }
  }

  /// main
  /// args(0) sparkUI
  /// args(1) conf.json
  /// args(2) rst.json
  /// args(3) print or Not
  def main(args : Array[String]) : Unit = {
    var rootURL = url_0
    var isPrint : Boolean = false
    if(args.size >= 4) isPrint = args(3).toBoolean
    if( args.size >0 ) rootURL = args.head
    val runningUrl = genRunningUrl(rootURL)
    val rst = getSparkStreamingJobsInfo(runningUrl)
    // showArray(rst)
    // val writer = new java.io.PrintWriter("SparkUIinfo.rst","utf-8")
    rst.foreach{
      x =>
        println("==================" + x._1 + "==================")
        showArray(x._2)
    }
    // val conf = readConf(args(1))
    // val jsonRst = info2Json(rst,conf)
    // writer.write(jsonRst)
    // writer.flush
    // writer.close
    println("Mission Completed.")
  }

}  