val DATE_TIME = "2019/02/28/12"
val fraudScoresIndex=95
 
val scoreKeys = List("sivt", "givt", "givtt")
 
def scores2Dict(
    line : String
) : scala.collection.immutable.Map[String,String] = {
    if ( line.contains("=") ) {
        line.replaceAll("[{} ]","").split(",").map(_.split("=")).map(v => v(0) -> v(1)).toMap
    } else {
        scala.collection.immutable.Map[String,String]()
    }
}
 
def scoreCountsMapGenerator(
    line : String
) : scala.collection.mutable.Map[String,Long] = {
    var scoreCounts : scala.collection.mutable.Map[String,Long] = scala.collection.mutable.Map()
    val chunks = line.split("\t")
    val fraudScoresDict = scores2Dict(chunks(fraudScoresIndex))
 
    scoreCounts += ("TOTAL" -> 1)
 
    scoreKeys foreach { scoreKey => {
            if (fraudScoresDict.contains(scoreKey)) {
                scoreCounts += (scoreKey -> 1)
            } else {
                scoreCounts += (scoreKey -> 0)
            }
        }
    }
 
    scoreCounts
}
 
def mapCountingReducer(
    countsThis:scala.collection.mutable.Map[String,Long]
    , countsThat:scala.collection.mutable.Map[String,Long]
) : scala.collection.mutable.Map[String,Long] = {
    countsThis.keys.foreach {
        key => countsThis(key) = countsThis(key).toLong + countsThat(key).toLong
    }
    countsThis
}
 
sc.textFile(f"/user/thresher/quality/logs/${DATE_TIME}/impressions/*").
    map(line => ("counts",scoreCountsMapGenerator(line))).
    reduceByKey(mapCountingReducer(_,_)).
    saveAsTextFile(f"RES-88/${DATE_TIME}/PROD")

sc.textFile(f"quality/logs/${DATE_TIME}/impressions/*").
    map(line => ("counts",scoreCountsMapGenerator(line))).
    reduceByKey(mapCountingReducer(_,_)).
    saveAsTextFile(f"RES-88/${DATE_TIME}/DEV")
