import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
  
val JIRA = "SAD-4849"
val HDFS_BASE = "/user/thresher/quality/logs"
val YEAR = "2018"
val MONTH = "05"

val INPUT = s"${HDFS_BASE}/${YEAR}/${MONTH}"
val OUTPUT = s"${JIRA}/spark/${YEAR}/${MONTH}"

val ipAddressIndex=3
val originalIPAddressIndex=70
val givtIndex=80

def countsAsMapGenerator(line:String):scala.collection.mutable.Map[String,Long] = {
    var counts:scala.collection.mutable.Map[String,Long] = scala.collection.mutable.Map()
    val chunks = line.split("""\t""")
    counts += ("total_count" -> 1)
    if (chunks(ipAddressIndex) == "ERROR" || chunks(originalIPAddressIndex) == "ERROR") counts += ("error_count" -> 1) else counts += ("error_count" -> 0)
    if (chunks(givtIndex) == "1") counts += ("givt" -> 1) else counts += ("givt" -> 0)
    counts
}

def mapCountingReducer(countsThis:scala.collection.mutable.Map[String,Long]
    , countsThat:scala.collection.mutable.Map[String,Long])
    : scala.collection.mutable.Map[String,Long] = {
    countsThis.keys.foreach { key => countsThis(key) = countsThis(key).toLong + countsThat(key).toLong }
    countsThis
}

val DAY = 01
val HOUR = 12
val FULL_IN_PATH = f"${INPUT}/${DAY}%02d/${HOUR}%02d/impressions/*"
val FULL_OUT_PATH = f"${OUTPUT}/${DAY}%02d/${HOUR}%02d"
val fs = FileSystem.get(sc.hadoopConfiguration)
if(fs.exists(new Path(FULL_OUT_PATH)))
  fs.delete(new Path(FULL_OUT_PATH),true)

sc.textFile(FULL_IN_PATH).
    map(line => ("counts", countsAsMapGenerator(line))).
    reduceByKey(mapCountingReducer(_,_)).
    saveAsTextFile(s"${FULL_OUT_PATH}")
