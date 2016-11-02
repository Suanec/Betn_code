object example{
  def testIteratorConvert(_s : String) = {
    // val r = """max([1-9][\d]*)""".r
    val r = """max(\num)""".r
    _s match {
      case "log10" => println("log10")
      case "log" => println("log")
      case r(x) => println(s"max${x}")
      case _ =>
    }
  }
  def main(args : Array[String]) = {
    testIteratorConvert(args.head)
  }
}