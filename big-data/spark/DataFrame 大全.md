# `DataFrame`

## 过滤: 注意这些API最后都转换为了`DataSet`

1. where | filter: 

   这里我们推荐使用filter或者where(因为filter不能用 and 和 or) 的第一个API,因为少调了一次`sql`解析器,且代码更清晰,不容易出错.

```scala
   def where(condition: Column): Dataset[T] = filter(condition)
   def where(conditionExpr: String): Dataset[T] = {
       filter(Column(sparkSession.sessionState.sqlParser.parseExpression(conditionExpr)))
   }
```

看 where 的两个 API,发现都调用了 filter 方法,下面我们演示用法:

```scala
df.where("sequence==6410 or sequence=6411").show()
df.where($"sequence" === 6411 or $"sequence" === 6410).show()
df.filter($"sequence" !== 6411).show()
```
2. limit: 这个 API 简单,但是注意他是transformation 级别的API,和 take及head action级别的API一样的效果 .

---

## 选择或切片:   

1. `select` | `selectExpr`

   注意

2. drop:

   ```scala
    def drop(colNames: String*): DataFrame = {
       val resolver = sparkSession.sessionState.analyzer.resolver
       val allColumns = queryExecution.analyzed.output
       val remainingCols = allColumns.filter { attribute =>
         colNames.forall(n => !resolver(attribute.name, n))
       }.map(attribute => Column(attribute))
       if (remainingCols.size == allColumns.size) {
         toDF()
       } else {
         this.select(remainingCols: _*)
       }
     }
   ```

   我们分析上面drop的源码,发现其实它调用了select的API.

---

## 排序

1. `orderBy` |`sort`:建议用 sort

   ```scala
   def orderBy(sortExprs: Column*): Dataset[T] = sort(sortExprs : _*)
   def sort(sortCol: String, sortCols: String*): Dataset[T] = {
       sort((sortCol +: sortCols).map(Column(_)) : _*)
   }
   @scala.annotation.varargs
   def sort(sortExprs: Column*): Dataset[T] = {
       sortInternal(global = true, sortExprs)
   }
   private def sortInternal(global: Boolean, sortExprs: Seq[Column]): Dataset[T] = {
       val sortOrder: Seq[SortOrder] = sortExprs.map { col =>
         col.expr match {
           case expr: SortOrder =>
             expr
           case expr: Expression =>
             SortOrder(expr, Ascending)
         }
       }
   }
   ```

   ---

## 集聚

1. `groupBy`|`cube`|`rollup`:我看了下实现是一样,但是三个的结果科不一样额!具体间下面!

```scala
 def groupBy(col1: String, cols: String*): RelationalGroupedDataset = {
    val colNames: Seq[String] = col1 +: cols
    RelationalGroupedDataset(
      toDF(), colNames.map(colName => resolve(colName)), RelationalGroupedDataset.GroupByType)
  }
def cube(col1: String, cols: String*): RelationalGroupedDataset = {
    val colNames: Seq[String] = col1 +: cols
    RelationalGroupedDataset(
      toDF(), colNames.map(colName => resolve(colName)), RelationalGroupedDataset.CubeType)
  }
 def rollup(col1: String, cols: String*): RelationalGroupedDataset = {
    val colNames: Seq[String] = col1 +: cols
    RelationalGroupedDataset(
      toDF(), colNames.map(colName => resolve(colName)), RelationalGroupedDataset.RollupType)
  }
```



groupby

| sequence | gender | group |
| -------- | ------ | ----- |
| 6412     | M      | 3     |
| 6411     | M      | 3     |
| 6410     | M      | 3     |
| 6412     | F      | 4     |
| 6410     | F      | 2     |
| 6411     | F      | 3     |


cube:


| sequence | gender | cube |
| -------- | ------ | ---- |
| 6412     | null   | 7    |
| 6410     | null   | 5    |
| null     | F      | 9    |
| null     | null   | 18   |
| 6410     | F      | 2    |
| 6410     | M      | 3    |
| 6412     | M      | 3    |
| 6411     | F      | 3    |
| null     | M      | 9    |
| 6412     | F      | 4    |
| 6411     | null   | 6    |
| 6411     | M      | 3    |



rollup:

| sequence | gender | rollup |
| -------- | ------ | ------ |
| 6412     | null   | 7      |
| 6410     | null   | 5      |
| null     | null   | 18     |
| 6410     | F      | 2      |
| 6410     | M      | 3      |
| 6412     | M      | 3      |
| 6411     | F      | 3      |
| 6412     | F      | 4      |
| 6411     | null   | 6      |
| 6411     | M      | 3      |

下面我们将研究一个稍微复杂点的问题,分组计算样本方差的问题:

传统实现:

```scala
//method1
df.groupBy("sequence").agg(variance("age")).show()
//method2
df.createOrReplaceTempView("people")
val variances = spark.sql("select sequence,VARIANCE(age) as var,max(age) as max,min(age) as min,count(*) as count from people group by sequence")
```

下面我们将研究自已定义`udaf`函数来实现这个功能:



---

## join
join比较简单,和sql中的join用法一样,默认是inner join=join.一共4个:
  - inner join
  - left join
  - right join
  - full join
-----

## pivot & agg

```scala
val df1 = spark.createDataFrame(Seq(("a", "foo", 1), ("a", "foo", 3), ("a", "bar", 2), ("a", "car", 4), ("b", "foo", 3), ("b", "car", 8), ("b", "bar", 5), ("b", "bar", 1))).toDF("xb", "y", "z")
df1.groupBy('xb).pivot("y").max("z").show()
df1.groupBy('xb).pivot("y", List("foo", "car")).max("z").show()
df1.groupBy('xb).pivot("y", List("foo", "car")).agg("z" -> "max").show()
df1.groupBy('xb, 'y).agg("z" -> "max").show()
```

上面演示了其用法,下面看下 pivot 的 :API

```scala
def pivot(pivotColumn: String, values: Seq[Any]): RelationalGroupedDataset
```

注意 pivot必须用在`groupBy`之后.

agg函数用得较多,当不能直接调用自定义聚合函数或者类似 variance 这样的函数的时候,你可以用 agg 函数来调用.

## 交集,并集,差集

```scala
	//交集
    df1.intersect(df2).show()
    //差集
    df1.except(df2).show()
    //并集
    df1.union(df2).show()

	import spark.implicits._
    df3.withColumnRenamed("_1", "col1").show()
    df3.withColumn("newcolum", $"_1").show()
```

**注意:**有个小技巧,DataFrame如果直接通过Seq创建会默认为"1,_2......."等列名.

## explode 行转列,比如把一个List换成好几行

```scala
 val df4 = spark.createDataFrame(Seq(("a", "65-66-67-68"), ("b", "35-68-37-98"), ("c", "5-60-77-28"))).toDF("stu", "scores")
df4.select($"stu", explode(split($"scores", "-"))).show()
```

