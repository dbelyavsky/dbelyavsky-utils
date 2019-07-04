import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

val jira = "SAD-4848"

val d_timeValIndex = 9

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

val cacheIdPattern = """.*\Wc:(\w+)\W.*"""
def filterDTWithoutCacheId(logLine:String):Boolean = {
    val fields = logLine.split("""\t""")
    (fields(d_timeValIndex) != "null")  && ( ! fields(d_timeValIndex).matches(cacheIdPattern) )
}

val (year, month, day, hour) = ("2018", "07", "17", "12")
val inPath = s"/user/thresher/dt/raw/$year/$month/$day/$hour/*/DT*"
val outPath = s"$jira/projectCacheId"
rm(outPath)
sc.textFile(inPath).filter(line => filterDTWithoutCacheId(line)).saveAsTextFile(outPath)
