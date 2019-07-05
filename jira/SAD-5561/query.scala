import org.apache.hadoop.fs.{Path,FileSystem}
import java.util.Calendar

val OVERWRITE_OUTPUT = true

val givtIndex = 80
val fraudScoresIndex = 95


val HDFS_BASE = "/user/thresher/quality/logs"
val JIRA = "SAD-5561.query"
val YEAR = "2019"
val MONTH = "04"
var DAY = "30"
var HOUR = "12"

val fraudScoreKeysA = Set("givta", "givtb", "givts")
val fraudScoreKeysB = Set("givtc", "givtt")

def isGivt(qlogFields:Array[String]) : Boolean = {
    qlogFields(givtIndex) == "1"
}

def scores2Dict(
    line : String
) : scala.collection.immutable.Map[String,String] = {
    if ( line.contains("=") ) {
        line.replaceAll("[{} ]","").split(",").map(_.split("=")).map(v => v(0) -> v(1)).toMap
    } else {
        scala.collection.immutable.Map[String,String]()
    }
}

def filterMe(line : String) : Boolean = {
    val chunks = line.split("\t")
    val fraudScores = scores2Dict(chunks(fraudScoresIndex))

  ( isGivt(chunks)
    && ! (fraudScoreKeysA intersect fraudScores.keySet).isEmpty
    && ! (fraudScoreKeysB intersect fraudScores.keySet).isEmpty )
}

def time[R](block: => R): (R, Double) = { 
    val startTime = System.nanoTime()
    val result = block // call-by-name
    val timeDiffMS = (System.nanoTime() - startTime) / 1000000d
    (result, timeDiffMS)
}

@transient val fs = FileSystem.get(sc.hadoopConfiguration)
val INPUT = s"$HDFS_BASE/$YEAR/$MONTH/$DAY/$HOUR/impressions/*"
val OUTPUT = s"${JIRA}/$YEAR/$MONTH/$DAY/$HOUR"

println(s"Looking at input from [$INPUT]")

if (OVERWRITE_OUTPUT) {
  FileSystem.get(sc.hadoopConfiguration).delete(new Path(OUTPUT), true)
}

if ( fs.exists(new Path(OUTPUT) ) ) {
  println("\t[$OUTPUT] Already Exists and OVERWRITE is OFF: Skipping.")
} else {
  println(s"Writing to [$OUTPUT]")
  val returnStatus = time {
    sc.textFile(INPUT).
      filter(line => filterMe(line)).
      saveAsTextFile(OUTPUT)
  }

  println(f"${Calendar.getInstance.getTime} INFO: Execution completed with status in ${returnStatus._2} ms")
}
