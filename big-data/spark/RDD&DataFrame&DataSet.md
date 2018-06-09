# `DataFrame`

## `DataFrame` `creation`

1. `DataFrame`可以从`json`,`csv`,`parquet`,`hdfs`,`hive table`,`jdbc`等数据源读取数据(注意:最好是像`hdfs`分布式文件系统,否则需要保证每台机器的相同位置有盖资源或者网络系统挂载)

   - 从`json`读取的数据默认为`StringType`和`LongType`类型,次序并非文件中的次序.
   - 读取`csv`文件有两种方式:
     - 带头的文件用`option`读取(感觉并不好),这种读取的数据类型全部为`StringType`
     - 不带头的文件用`schema`读取(其实是一种普通的txt,非常常见,好用),自定义的头可以方便的读取数据类型,但必须认为保证数据类型的正确.
   - 从`table`读取的数据类型保持原类型.
   - 读取`parquet`类型的文件,能够读取其原有数据类型,推荐用这种!
   - 从数据库读取的类型`todo`
   - 从`rdd`构建`dataframe`,需要注意用`map`变成一个`Row`

   ```scala
    private def generateDF(spark: SparkSession) = {
       //generate rdd from json file( directly get dataType,you can use row.getLong)
       import spark.implicits._
       val rdd1 = spark.read.json("src/main/resources/data/people.json")
       rdd1.printSchema()
       rdd1.map(row => (row.getAs[String]("name"), row.getLong(0), row.getString(1), row.getLong(2), row.getString(3)))
         .foreach(row => println(row._1 + "|" + row._2 + "|" + row._3 + "|" + row._4 + "|" + row._5))
   ```


       //generate rdd from csv file with header which will generate  all StringType fields
       val rdd2 = spark.read.option("header", "true").csv("src/main/resources/data/people.csv")
       rdd2.printSchema()
       rdd2.map(row =>
         row.getAs[String](0) + "|" + row.getAs[String](1).toInt + "|" +
           row.getAs[String]("gender") + "|" + row.getAs[String](3).toInt)
         .foreach(str => println(str))


       //generate rdd from csv file without header which will control the field type
       val schema = StructType(Seq(
         StructField("name", StringType, true),
         StructField("age", IntegerType, true),
         StructField("gender", StringType, true),
         StructField("index", IntegerType, true)
       ))
       val rdd3 = spark.read.schema(schema).csv("src/main/resources/data/people1.csv")
       rdd3.printSchema()
       rdd3.map(row => (row.getString(0), row.getInt(1), row.getInt(3)))
         .foreach(tup => println(tup))
     } private def generateDF(spark: SparkSession) = {
       //generate rdd from json file( directly get dataType,you can use row.getLong)
       import spark.implicits._
       val df1 = spark.read.json("src/main/resources/data/people.json")
       df1.printSchema()
       df1.map(row => (row.getAs[String]("name"), row.getLong(0), row.getString(1), row.getLong(2), row.getString(3)))
         .foreach(row => println(row._1 + "|" + row._2 + "|" + row._3 + "|" + row._4 + "|" + row._5))


       //generate rdd from csv file with header which will generate  all StringType fields
       val df2 = spark.read.option("header", "true").csv("src/main/resources/data/people.csv")
       df2.printSchema()
       df2.map(row =>
         row.getAs[String](0) + "|" + row.getAs[String](1).toInt + "|" +
           row.getAs[String]("gender") + "|" + row.getAs[String](3).toInt)
         .foreach(str => println(str))


       //generate rdd from csv file without header which will control the field type
       val schema = StructType(Seq(
         StructField("name", StringType, true),
         StructField("age", IntegerType, true),
         StructField("gender", StringType, true),
         StructField("index", IntegerType, true)
       ))
       val df3 = spark.read.schema(schema).csv("src/main/resources/data/people1.csv")
       df3.printSchema()
       df3.map(row => (row.getString(0), row.getInt(1), row.getInt(3)))
         .foreach(tup => println(tup))
    
       //read data from hive table
       df3.write.saveAsTable("people")
       val df4 = spark.read.table("people")
       df4.printSchema()
    
       //read data from hdfs
       val df5 = spark.read.load("")
      
      //read data from parquet,best form hdfs
       val df6 = spark.read.parquet("src/main/resources/data/people")
       df6.printSchema()
       df6.map(row => (row.getInt(1), row.getString(0)))
         .foreach(tuple => println(tuple))
      
      //generate dataframe from rdd
       val rdd = spark.sparkContext.textFile("src/main/resources/data/people1.csv")
       val rdd2 = rdd.map(line => line.split(",")).map(arr => Row(arr(0), arr(1).toInt, arr(2), arr(3).toInt))
       val schema = StructType(
         StructField("name", StringType, true) ::
           StructField("index", IntegerType, true) ::
           StructField("gender", StringType, true) ::
           StructField("age", IntegerType, true) :: Nil
       )
       val df7 = spark.createDataFrame(rdd2, schema)
       df7.printSchema()
       df7.show()
     }