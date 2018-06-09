# coalesce/repartition 详解

### coalesce 

```scala
def coalesce(numPartitions: Int): Dataset[T] = withTypedPlan {
  Repartition(numPartitions, shuffle = false, planWithBarrier)
}
```

可以看出 coalesce 是调用了 repartition 的方法

### repartition

```scala
def repartition(numPartitions: Int): Dataset[T] = withTypedPlan {
    Repartition(numPartitions, shuffle = true, planWithBarrier)
  }
case class Repartition(numPartitions: Int, shuffle: Boolean, child: LogicalPlan)
  extends RepartitionOperation {
  require(numPartitions > 0, s"Number of partitions ($numPartitions) must be positive.")
}
```

先简单分析下上面的API:coalesce的shuffle传参为false,repartition的传参为true. child传参都为 `planWithBarrier`,而`RepartitionByExpression`方法通过用于某些特定的数据排序或者分布 .

```scala
abstract class RepartitionOperation extends UnaryNode {
  def shuffle: Boolean
  def numPartitions: Int
  override def output: Seq[Attribute] = child.output
}
@transient private val planWithBarrier = AnalysisBarrier(logicalPlan)
case class AnalysisBarrier(child: LogicalPlan) extends LeafNode {
  override protected def innerChildren: Seq[LogicalPlan] = Seq(child)
  override def output: Seq[Attribute] = child.output
  override def isStreaming: Boolean = child.isStreaming
  override def doCanonicalize(): LogicalPlan = child.canonicalized
}
@transient private[sql] val logicalPlan: LogicalPlan = {
    // For various commands (like DDL) and queries with side effects, we force query execution
    // to happen right away to let these side effects take place eagerly.
    queryExecution.analyzed match {
      case c: Command =>
        LocalRelation(c.output, withAction("command", queryExecution)(_.executeCollect()))
      case u @ Union(children) if children.forall(_.isInstanceOf[Command]) =>
        LocalRelation(u.output, withAction("command", queryExecution)(_.executeCollect()))
      case _ =>
        queryExecution.analyzed
    }
  }
```



其实可以发现这两个的实现和以前的版本不一样了.但是通过实现还是可以看出,repartition 的shuffle为true,coalesce的shuffle为false.

1. **这两个方法来修改分区数量,shuffle=true发生了shuffle操作(比如将10个分区repartition为100个,则一个父RDD对应多个子RDD(类似于groupByKey)),shuffle=false没有发生了shuffle操作(比如将100个分区coalesce 为10个,每个父RDD只对应一个子RDD,故不用发生shuffle操作,效率较高) **
2. **用这两个方法来修改分区,虽然可能采用netty通信方式来获取它的parent RDD数据(不在一个Node上),但是没有到要发生磁盘的操作,避免了磁盘IO.因此比正真的shuffle还是要快不少**

### 测试用例

#### Demo

```scala
private def coalesceDemo(spark: SparkSession) = {
    val schmema = StructType(Seq(StructField("name", StringType, true), StructField("age", IntegerType, true),
      StructField("gender", StringType, true), StructField("index", IntegerType, true)))
    val datas = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .schema(schmema)
      .load("src/main/resources/rddData/people.csv")
    import spark.implicits._
    datas.map(row => row.formatted("\t")).foreach(str => println)
    println("原始分区" + datas.rdd.getNumPartitions)
    val datas1 = datas.repartition(4)
    println("repartitions 后的分区" + datas1.rdd.getNumPartitions)
    datas1.printSchema()
    val datas2 = datas1.filter(row => {
      println(row.get(1) + "-" + row.get(2) + row.get(3))
      val age = row.getInt(1)
      age > 20
    })
    datas2.collect().foreach(println)
    val datas3 = datas2.coalesce(2)
    println("coalesce后的大小" + datas3.rdd.getNumPartitions)
  }
```

#### Result(中间数据有省略)

```shell
原始分区1
repartitions 后的分区4
root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- gender: string (nullable = true)
 |-- index: integer (nullable = true)
 coalesce后的大小2
```



### 背景知识

#### narrow dependency & full dependency(shuffle):关键从父分区的角度来考虑,如果一个分区的数据要对应到多个子分区,其它.

- narrow: 
  - 一个父RDD对应一个子RDD(map操作)
  - 多个父RDD对应1个子RDD(比如上面的coalesce操作,把某个节点上父RDD多的节点选为新的节点,就能大量避免数据的通信搬运)
- full:
  - 一个父RDD对应所有的子RDD(join操作)
  - 一个父RDD对应非全部的多个子RDD(groupByKey操作)

