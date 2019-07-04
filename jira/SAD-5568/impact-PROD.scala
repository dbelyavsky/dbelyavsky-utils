import org.apache.hadoop.fs.{Path,FileSystem}
import java.util.Calendar

val OVERWRITE_OUTPUT = false

val actionIndex = 16
val fraudScoresIndex = 95

val HDFS_BASE = "/user/thresher/quality/logs"
val OUTPUT_BASE = "jira/SAD-5568/IMPACT"
val YEAR = "2019"
val MONTH = "06"

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


  for (scoreKey <- List("tivt", "sivt", "givt", "givtt")) {
    scoreCounts += (scoreKey -> (if (fraudScores.contains(scoreKey)) 1 else 0))
  }

  for (actionVal <- List("preview", "failed", "passed")) {
    scoreCounts += (actionVal -> (if (actionVal == chunks(actionIndex)) 1 else 0))
  }

  scoreCounts += ("NotYetGIVT" -> (
    if (chunks(actionIndex) == "preview"
      && ! fraudScores.contains("givt")
      && ! fraudScores.contains("sivt")
    ) 1 else 0))

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
  val result = block // call-by-name
  val timeDiffMS = (System.nanoTime() - startTime) / 1000000d
  (result, timeDiffMS)
}


@transient val fs = FileSystem.get(sc.hadoopConfiguration)
for (day <- 11 to 17) {
  for (hour <- 0 to 23) {
    val DAY = f"$day%02d"
    val HOUR = f"$hour%02d"

    val INPUT = s"$HDFS_BASE/$YEAR/$MONTH/$DAY/$HOUR/impressions/*"
    val OUTPUT = s"${OUTPUT_BASE}/$YEAR/$MONTH/$DAY/$HOUR"

    println(s"Looking at input from [$INPUT]")

    if (OVERWRITE_OUTPUT) {
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(OUTPUT), true)
    }

    if ( fs.exists(new Path(OUTPUT) ) ) {
      println(s"\t[$OUTPUT] Already Exists and OVERWRITE is OFF: Skipping.")
    } else {
      println(s"Writing to [$OUTPUT]")
      val returnStatus = time {
        sc.textFile(INPUT).
          map(line => (s"$YEAR-$MONTH-$DAY-$HOUR", scoreCountsMapGenerator(line))).
          reduceByKey(mapCountingReducer(_,_)).
          toDF().
          write.
          json(OUTPUT)
      }

      println(f"${Calendar.getInstance.getTime} INFO: Execution completed with status in ${returnStatus._2} ms")
    }
  }
}

