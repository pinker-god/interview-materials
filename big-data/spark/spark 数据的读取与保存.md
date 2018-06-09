# spark 数据的读取与保存

## 文件格式

Spark 支持的一些常见格式：

| 格式名称          | 结构化  | 备注                                       |
| ------------- | ---- | ---------------------------------------- |
| 文本文件          | 否    | 普通的文本文件，每行一条记录                           |
| JSON          | 半结构化 | 常见的基于文本的格式；大多数库都要求每行一条记录                 |
| CSV           | 是    | 基于文本，通常在电子表格中使用                          |
| SequenceFiles | 是    | 用于键值对数据的常见Hadoop文件格式                     |
| Proto buffers | 是    | 快速、解决空间的跨语言格式                            |
| 对象文件          | 是    | 用来将Spark作业的数据存储下来以让共享的代码读取。改变类的时候它会失效，因为它依赖于Java序列化 |

### 文本文件

```scala
private def textDemo(spark: SparkSession) = {
    val datas = spark.sparkContext.textFile("src/main/resources/rddData/people.csv")
    datas.foreach(println)
    val wholesData = spark.sparkContext.wholeTextFiles("src/main/resources/rddData/cartesian.csv")
    wholesData.mapValues(x => x).foreach(println)

    //Spark将传入的路径当做目录，会在目录下输出多个文件。我们不能控制数据的哪一部分输出到哪个文件中，不过有些输出格式支持控制
    wholesData.saveAsTextFile("src/main/resources/rddData/cartesian2.csv")
  
  //下面是DataFrame格式的,常用!
    val schema = StructType(Seq(StructField("col1", IntegerType, true), StructField("col2", IntegerType, true)))
    import spark.implicits._
    val df = spark.createDataset(Seq(1 to 10: _*)).map(num=>num.toString).toDF()
    df.write.text("src/main/resources/rddData/cartesian.txt")
  }
```

**注意spark的输出思想,是分布式的,而不是像windows下我们常见的把数据聚集到一个文件,后面的json或者csv的输出你会发现也是一样,但格式不同,也就是csv输出的会标记为csv,json的会标记为json,后面会有截图**

###json

```scala
private def jsonDemo(spark: SparkSession) = {
  val datas = spark.read.format("com.databricks.spark.csv")
    .option("header", "false") // Use first line of all files as header
    .load("src/main/resources/rddData/people.csv")
  datas.write.json("src/main/resources/rddData/people1.json")
  //读取json格式的数据(本身就带有schema,故不用自己加schema)
  val jDatas = spark.read.json("src/main/resources/rddData/people1.json")
  //写出json格式的数据
  jDatas.write.json("src/main/resources/rddData/people2.json")
}
```

###csv

```scala
	val schmema = StructType(Seq(StructField("name", StringType, true), StructField("age", IntegerType, true),
      StructField("gender", StringType, true), StructField("index", IntegerType, true)))
    val datas = spark.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .schema(schmema)
      .load("src/main/resources/rddData/people.csv")
    datas.write.format("com.databricks.spark.csv").option("header", "true").mode(SaveMode.Append).csv("src/main/resources/rddData/peopleWithHeader.csv")
```

这里我们读取的数据加上了自己的schema,方便很多,常用这种形式,把数据全部保存为csv而不加头.csv是常用的数据处理文件,其中mode方法里的saveMode参数常见的值如下表:

表`SaveMode`

| SaveMode  Value          | Any Language                          | 把DataFrame 里的数据写出到数据源时:                  |
| ------------------------ | ------------------------------------- | ---------------------------------------- |
| `SaveMode.ErrorIfExists` | `"error" or "errorifexists"`(default) | 如果数据源(可以理解为文件)已经存在,将抛出异常                 |
| `SaveMode.Append`        | `"append"`                            | 如果数据源或者表已经存在,继续追加(append)                |
| `SaveMode.Overwrite`     | `"overwrite"`                         | 如果数据源已经存在,覆盖写出.(overwite)                |
| `SaveMode.Ignore`        | `"ignore"`                            | 如果数据源已经存在,将忽略(ignore) DataFrame中的 数据,如果不存在,则创建并写出.官网的比喻是类似这条SQL语句,很形象. `CREATE TABLE IF NOT EXISTS` |