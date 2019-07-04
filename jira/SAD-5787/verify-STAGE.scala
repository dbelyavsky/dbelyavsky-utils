import org.apache.hadoop.fs.{Path,FileSystem}
import java.util.Calendar

val OVERWRITE_OUTPUT = false

val givtIndex = 80
val fraudScoresIndex = 95

val HDFS_BASE_STAGE = "/user/etlstage/quality/logs"
val HDFS_BASE_USER = "quality/logs"
val OUTPUT_BASE_STAGE = "SAD-5787.STAGE"
val OUTPUT_BASE_USER = "SAD-5787.USER"
val YEAR = "2019"
val MONTH = "06"
var DAY = "05"
var HOUR = "12"

def scores2Dict(
                 line : String
               ) : scala.collection.immutable.Map[String,String] = {
  if ( line.contains("=") ) {
    line.replaceAll("[{} ]","").split(",").map(_.split("=")).map(v => v(0) -> v(1)).toMap
  } else {
    scala.collection.immutable.Map[String,String]()
  }
}

def scoreCountsMapGenerator(line : String) : scala.collection.mutable.Map[String,Long] = {
  var scoreCounts : scala.collection.mutable.Map[String,Long] = scala.collection.mutable.Map()
  val chunks = line.split("\t")
  val fraudScores = scores2Dict(chunks(fraudScoresIndex))

  scoreCounts += ("TOTAL" -> 1)

  scoreCounts += ("sivt" -> (if(fraudScores.contains("sivt")) 1 else 0))
  scoreCounts += ("ds" -> (if(fraudScores.contains("ds")) 1 else 0))
  scoreCounts += ("dsu" -> (if(fraudScores.contains("dsu")) 1 else 0))
  scoreCounts += ("dsr" -> (if(fraudScores.contains("dsr")) 1 else 0))
  scoreCounts += ("dse" -> (if(fraudScores.contains("dse")) 1 else 0))

  scoreCounts
}

def mapCountingReducer(
                        countsThis:scala.collection.mutable.Map[String,Long]
                        , countsThat:scala.collection.mutable.Map[String,Long]
                      ) : scala.collection.mutable.Map[String,Long] = {
  (countsThis.keys ++ countsThat.keys) foreach {
    key => countsThis(key) = countsThis.getOrElse(key,0L) + countsThat.getOrElse(key,0L)
  }
  countsThis
}

def time[R](block: => R): (R, Double) = {
  val startTime = System.nanoTime()
  val result = block                                              // call-by-name
  val timeDiffMS = (System.nanoTime() - startTime) / 1000000d
  (result, timeDiffMS)
}

@transient val fs = FileSystem.get(sc.hadoopConfiguration)
//for (day <- 20 to 28) {
//  for (hour <- 0 to 23) {
//    DAY = f"$day%02d"
//    HOUR = f"$hour%02d"

for (inOut <- List(List(HDFS_BASE_STAGE, OUTPUT_BASE_STAGE), List(HDFS_BASE_USER, OUTPUT_BASE_USER)).toSeq ) {
  val INPUT = s"${inOut(0)}/$YEAR/$MONTH/$DAY/$HOUR/impressions/*"
  val OUTPUT = s"${inOut(1)}/$YEAR/$MONTH/$DAY/$HOUR"

  println(s"Looking at input from [$INPUT]")

  if (OVERWRITE_OUTPUT) {
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(OUTPUT), true)
  } else if (fs.exists(new Path(OUTPUT))) {
    println("\t[$OUTPUT] Already Exists and OVERWRITE is OFF: Skipping.")
  } else {
    println(s"Writing to [$OUTPUT]")
    val returnStatus = time {
      sc.textFile(INPUT).
        map(line => (s"$YEAR-$MONTH-$DAY-$HOUR", scoreCountsMapGenerator(line))).
        reduceByKey(mapCountingReducer(_, _)).
        toDF().
        write.
        json(OUTPUT)
    }

    println(f"${Calendar.getInstance.getTime} INFO: Execution completed with status in ${returnStatus._2} ms")
  }
}
//  }
//}
