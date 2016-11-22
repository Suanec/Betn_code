import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
// import org.apache.spark.
// import org.apache.spark.
// import org.apache.spark.
// import org.apache.spark.


// val path = "/user/hadoop/testSpark/"
val dcf = "/home/hadoop/suanec/Betn_code/data/dcf"
val wsp = "/home/hadoop/suanec/Betn_code/data/"
// val de = getDataElems(dcf)
// val dst = genStructField(de).init
val dataPath = "file://" + wsp + "data.sample"

object RawDataFilter extends Serializable {

  @transient
  def typeToType(_type : String) : DataType = {
    _type match {
      case "enum" => IntegerType
      case "origin" => DoubleType
      case "bool" => StringType
      case _ => StringType
    }
  }

  @transient
  def typeMappor(_elem_splits : Array[String]) : DataType = {
    _elem_splits.size match {
      case 2 => StringType
      case 3 => typeToType(_elem_splits(2))
      case 4 => typeToType(_elem_splits(2))
      case _ => StringType
    }
  }

  @transient
  def genStructField( _dcc_elems : Array[String]) : Option[Seq[StructField]] = {
    if(_dcc_elems.size <= 0){
      println(s"error with dataConf for elements.size, size : ${_dcc_elems.size}")
      return None
    }
    Some(_dcc_elems.map{
      elem => 
      val elem_splits = elem.split('@')
      val elemDataType = typeMappor(elem_splits)
      val elem_name = elem_splits(1)
      StructField(elem_name,elemDataType,nullable = true)
    })
  }

  @transient
  def getDataElems( _dataConf : String ) : Array[String] = {
    val dcc = scala.io.Source.fromFile(_dataConf).getLines.toArray
    dcc.splitAt(dcc.indexOf(dcc.find(_.startsWith("@")).get) + 1)._2
  }

  @transient
  def dataStructParser( _dataConf : String ) : StructType = {
    val dcc_elems = getDataElems(_dataConf)
    val dataStruct = genStructField(dcc_elems).get
    StructType(dataStruct)
  }  

  @transient
  def dataCleansing( _name : String, _value : String) : String = {
    val cData = _name match {
      case name if name.contains("gender") =>
        _value match {
        case "m" => "1"
        case "f" => "0"
        case _ => "2"
      }
      case _ => _value match {
        case "\\N" => 0.toString
        case _ => _value
      }
    }
    cData
  }

  @transient
  def rddToRowRDD(_data : RDD[String], _schema : StructType) : RDD[Row] = {
    val schema_b = spark.sparkContext.broadcast(_schema)
    _data.map{
      row =>
      val rowSplits = row.split('\t')
      val rowData = rowSplits.indices.map{
        i =>
        // println(rowSplits(i),schema_b.value(i))
        val schema = schema_b.value
        schema(i).dataType match {
          case StringType => rowSplits(i)
          case BooleanType => rowSplits(i)
          case IntegerType => {
            //rowSplits(i).toInt 
            dataCleansing(schema(i).name,rowSplits(i)).toInt
          }
          case DoubleType => {
            //rowSplits(i).toDouble 
            dataCleansing(schema(i).name,rowSplits(i)).toDouble
          }
          case _ => rowSplits(i)
        }
      }
      println(rowData)
      Row.fromSeq(rowData)
    }
  }
  // rddToRowRDD(data,dsp).first

  @transient
  def rddToDF( _spark : org.apache.spark.sql.SparkSession,
    _data : RDD[String],
    _dataConf : String) : Option[DataFrame] = {
    val dataStruct = dataStructParser(_dataConf)
    val rowData = rddToRowRDD(_data,dataStruct)
    if(rowData.first.size != dataStruct.size){
      println(s"dataConf not match the data! with dataConf size of ${dataStruct.size}")
      return None 
      }
    Some(_spark.sqlContext.createDataFrame(
      rowData,dataStruct))
  }

  @transient
  def getDF( _spark : org.apache.spark.sql.SparkSession,
    _dataPath : String, 
    _dataConf : String) : DataFrame = {
    val rddData = _spark.sparkContext.textFile(_dataPath)
    rddToDF(_spark,rddData,_dataConf).get
  }

  @transient
  def sqlQuery( _spark : org.apache.spark.sql.SparkSession,
    _df : DataFrame,
    _table : String, 
    _sqlQuery : String) : DataFrame = {
    _df.registerTempTable(_table)
    _spark.sql(_sqlQuery)
  }

  @transient
  def dataSelect( _spark : org.apache.spark.sql.SparkSession,
    _dataPath : String,
    _dataConf : String,
    _table : String,
    _sqlQuery : String) : RDD[String] = {
    val df = getDF(_spark,_dataPath,_dataConf)
    val dfSelected = sqlQuery(_spark,df,_table,_sqlQuery)
    dfSelected.map(r => r.mkString("\t")).rdd
  }
}

// val df = RawDataFilter.getDF(spark,dataPath,dcf)
// val sql1 = "select * from dft where u_gender!='f' and u_gender!='m'"
// val sql2 = "select label,u_gender,m_mid,m_has_face from dft where u_iar_his > 0.5"
// val table = "dft"
// val df1 = RawDataFilter.sqlQuery(spark,df,table,sql1)
// val df2 = RawDataFilter sqlQuery(spark,df,table,sql2)
// val targetData = RawDataFilter dataSelect(spark,dataPath,dcf,table,sql2)
