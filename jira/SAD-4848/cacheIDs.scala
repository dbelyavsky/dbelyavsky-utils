import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

val jira = "SAD-4848"

val e_javascriptInfoIndex=36

val d_asIdIndex=8
val d_timeValIndex=9
val d_eventTypeIndex=10

val (year, month, day, hour) = ("2018", "07", "17", "*")
val inPath = s"/user/thresher/dt/raw/$year/$month/$day/$hour/*/DT*"
val outPath = s"$jira/projectCacheId"

def rm(pathStr:String) : Boolean = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val aPath = new Path(pathStr)
    if ( fs.exists(aPath) ) {
        fs.delete(aPath, true)
        true
    } else {
        false
    }
}

val cacheIdRE = """.*\Wc:(\w+)\W.*""".r

def countsAsMapGenerator(line:String):scala.collection.mutable.Map[String,Long] = {
    var counts:scala.collection.mutable.Map[String,Long] = scala.collection.mutable.Map()
    val fields = line.split("""\t""")
    fields(d_timeValIndex) match {
        case cacheIdRE(cacheId) => counts += ("invalid_cache_id" -> 0)
        case _ => counts += ("invalid_cache_id" -> (if (fields(d_eventTypeIndex) == "cookieDT") 0 else 1))
    }
    counts += ("total_count" -> 1)
}

def mapCountingReducer(countsThis:scala.collection.mutable.Map[String,Long]
    , countsThat:scala.collection.mutable.Map[String,Long]) 
    : scala.collection.mutable.Map[String,Long] = {
    countsThis.keys.foreach { key => countsThis(key) = countsThis(key).toLong + countsThat(key).toLong }
    countsThis
}

rm(outPath)

sc.textFile(inPath).
    map(line => ("counts", countsAsMapGenerator(line))).
    reduceByKey(mapCountingReducer(_,_)).
    collect()
    //saveAsTextFile(outPath)
