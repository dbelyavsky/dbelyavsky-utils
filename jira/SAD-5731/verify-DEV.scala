val fraudScoresIndex=95
 
val scoreKeys = List("ds", "dsu", "dsr", "dse")
 
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

sc.textFile("quality.SAD-5731-DEV/logs/2019/04/30/12/impressions/*").
    map(line => ("counts",scoreCountsMapGenerator(line))).
    reduceByKey(mapCountingReducer(_,_)).
    collect()

// RESULTS
// mapCountingReducer: (countsThis: scala.collection.mutable.Map[String,Long], countsThat: scala.collection.mutable.Map[String,Long])scala.collection.mutable.Map[String,Long]
// res5: Array[(String, scala.collection.mutable.Map[String,Long])] = Array((counts,Map(dse -> 0, TOTAL -> 440541712, dsu -> 3061, dsr -> 0, ds -> 3061)))