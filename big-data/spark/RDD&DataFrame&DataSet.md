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