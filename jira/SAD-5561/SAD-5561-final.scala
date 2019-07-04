import org.apache.hadoop.fs.{Path,FileSystem}

val fs = FileSystem.get(sc.hadoopConfiguration)

val df = sqlContext.read.json("SAD-5561.V2/2019/*/*/*").toDF()

//df.printSchema

val sumAll = df.columns.collect{ case x if x != "_1" => col(x) }.reduce(_ + _)
df.withColumn("sum", sumAll)
