  def createSchema(schemaString: String): StructType = StructType(
    schemaString.split(",").map {
      case fieldName_fieldType:String =>
        StructField(fieldName_fieldType.split(":").head,
          dataType = fieldName_fieldType.split(":").toSeq(1) match {
            case "int" => IntegerType
            case "bigint" => LongType
            case "double" => DoubleType
            case "float" => FloatType
            case "string" => StringType
            //case "date" => DateType
            case _ => StringType
          },
          nullable = true)
    }
  )
spark.sqlContext.createDataFrame(tableRDD, tableSchema).toDF

val dfData = sqlContext.createDataFrame(dfRDD).toDF("label", "features")
