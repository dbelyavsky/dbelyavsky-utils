import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

val OVERWRITE_OUTPUT = false
  
val JIRA = "MAPP-1328"
val YEAR = "2019"
val MONTH = "06"

val IN_BASE = "/user/etlstage/quality"

val mediaTypeIdIndex=90
val mrcAccreditedIndex=107
val impsIndex=19

def mrcAccreditedCounts(line : String) : scala.collection.mutable.Map[String,Long] = {
  var scoreCounts : scala.collection.mutable.Map[String,Long] = scala.collection.mutable.Map()
  val chunks = line.split("\t")

  scoreCounts += ("TOTAL" -> chunks(impsIndex).toDouble.toLong)

  scoreCounts += ("mrcAccredited" -> (if(chunks(mrcAccreditedIndex) == "1"
  										&& List("221", "222", "231", "232").contains(chunks(mediaTypeIdIndex))) 1 else 0))

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

@transient val fs = FileSystem.get(sc.hadoopConfiguration)
for (day <- 14 to 16) {
	for (hour <- 0 to 23) {
		val DAY = f"$day%02d"
		val HOUR = f"$hour%02d"

		val INPUT = f"${IN_BASE}/scores/aggdt4/${YEAR}/${MONTH}/${DAY}/${HOUR}/*"
		val OUTPUT = f"${JIRA}/${DAY}/${HOUR}/aggdt4"

		println(s"Looking at input from [$INPUT]")

		if (OVERWRITE_OUTPUT) {
			FileSystem.get(sc.hadoopConfiguration).delete(new Path(OUTPUT), true)
		} 

		if (fs.exists(new Path(OUTPUT))) {
			println("\t[$OUTPUT] Already Exists and OVERWRITE is OFF: Skipping.")
		} else {
			println(s"Writing to [$OUTPUT]")
			sc.textFile(INPUT).
			map(line => (s"$YEAR-$MONTH-$DAY-$HOUR", mrcAccreditedCounts(line))).
			reduceByKey(mapCountingReducer(_, _)).
			toDF().
			write.
			json(OUTPUT)
		}
	}
}
