import org.apache.hadoop.fs.{Path,FileSystem}

val actionIndex = 16
val fraudScoresIndex = 95

val HDFS_BASE_THRESHER = "/user/thresher/quality/logs"
val HDFS_BASE_USER = "quality/logs"
val OUTPUT_BASE = "SAD-5568"
val YEAR = "2019"
val MONTH = "06"
val DAY = "17"
val HOUR = "12"

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


  for (scoreKey <- List("tivt", "rvi", "sivt", "ha", "ds", "nht", "pnht", "pnht1", "pnht2", "givt", "givtt", "givta", "givtb", "givtc", "givts")) {
    scoreCounts += (scoreKey -> (if (fraudScores.contains(scoreKey)) 1 else 0))
  }


  for (actionVal <- List("preview", "failed", "passed")) {
    scoreCounts += (actionVal -> (if (actionVal == chunks(actionIndex)) 1 else 0))
  }

  scoreCounts += ("NotYetGIVT" -> (
    if (chunks(actionIndex) == "preview"
      && ! (fraudScores.contains("givt") || fraudScores.contains("sivt"))) 1 else 0))

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

val INPUT_THRESHER = s"$HDFS_BASE_THRESHER/$YEAR/$MONTH/$DAY/$HOUR/impressions/*"
val INPUT_USER = s"$HDFS_BASE_USER/$YEAR/$MONTH/$DAY/$HOUR/impressions/*"

val OUTPUT = s"${OUTPUT_BASE}/dev_compare/$MONTH/$DAY/$HOUR"
FileSystem.get(sc.hadoopConfiguration).delete(new Path(OUTPUT), true)

sc.textFile(INPUT_THRESHER).
  map(line => (s"INPUT_THRESHER", scoreCountsMapGenerator(line))).
  reduceByKey(mapCountingReducer(_,_)).
  saveAsTextFile(s"${OUTPUT}/thresher")

sc.textFile(INPUT_USER).
  map(line => (s"INPUT_USER", scoreCountsMapGenerator(line))).
  reduceByKey(mapCountingReducer(_,_)).
  saveAsTextFile(s"${OUTPUT}/user")
