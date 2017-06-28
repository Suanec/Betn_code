val metaPath = """D:\Docs\Works_And_Jobs\Sina\Betn_code\dataflowBugFix\dataflow\haibo2\push.meta"""
val metaData = scala.io.Source.fromFile(metaPath).getLines.toArray
metaData.map(_.split(':').head.toInt).sorted.zipWithIndex.filter(x => x._1 != x._2)//.size == 0
lines.map(_.split(':').head.toInt).sorted.zipWithIndex.filter(x => x._1 != x._2).size == 0
assert(
  metaData.map(_.split(':').head.toInt).sorted.zipWithIndex.filter(x => x._1 != x._2).size == 0,
  s"Features'index in DataMeta Error! "
)
val metaPattern: scala.util.matching.Regex = "([0-9]+):([a-z0-9A-Z_]+):[a-zA-Z]+".r
val sortedIndexName = metaData.map{
      case r: Row =>
        val line: String = r.getAs[String]("metaLine")
        val metaPattern(index, name) = line
        (index.toInt, name)
    }.sortBy(_._1)
