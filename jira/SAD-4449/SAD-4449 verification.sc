val fraudScoresIndex=95

val scoreKeys = List("sivt", "pnht", "nht", "tivt", "civt", "tsivt", "givt")

val nhtKeys = List("nht1", "nht2", "nht3", "nht4", "nht5", "nht6", "nht7")

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

    // this should be equal to "pnht" by itself
    if (fraudScoresDict.contains("pnht") && fraudScoresDict.contains("sivt") && fraudScoresDict.contains("nht")) {
        scoreCounts += ("pnhts+sivt+nht" -> 1)
    } else {
        scoreCounts += ("pnhts+sivt+nht" -> 0)
    }

    // this should be equal to nht by itself (only one sub-category should exist)
    val nhtSubCatCountKey = "nhtSubCatCountKey"
    if ( nhtKeys.intersect( fraudScoresDict.keySet.toSeq ).nonEmpty ) {
        scoreCounts += (nhtSubCatCountKey -> 1)
    } else {
        scoreCounts += (nhtSubCatCountKey -> 0)
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

sc.textFile("quality.SAD-4449.dev_test/logs/2019/01/22/12/impressions/*").
    map(line => ("counts",scoreCountsMapGenerator(line))).
    reduceByKey(mapCountingReducer(_,_)).
    collect()

sc.textFile("quality.SAD-4449.dev_test/logs/2019/01/23/12/impressions/*").
    map(line => ("counts",scoreCountsMapGenerator(line))).
    reduceByKey(mapCountingReducer(_,_)).
    collect()
