val sadEvidenceIndex=57
val fraudScoresIndex=95

val scoreKeys = List("sivt", "pnht", "nht", "tivt", "civt", "tsivt", "givt")

val nhtKeys = List("nht1", "nht2", "nht3", "nht4", "nht5", "nht6", "nht7")

val mwPattern = "\\Wmw\\W".r

val pnhtSivtNhtCountKey = "pnhts+sivt+nht"
val nhtSubCatCountKey = "SubCategoryCount"
val pnhtAndMwCountKey = "pnht+mw"

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
        scoreCounts += (pnhtSivtNhtCountKey -> 1)
    } else {
        scoreCounts += (pnhtSivtNhtCountKey -> 0)
    }

    // this should be equal to nht by itself (only one sub-category should exist)
    if ( nhtKeys.intersect( fraudScoresDict.keySet.toSeq ).nonEmpty ) {
        scoreCounts += (nhtSubCatCountKey -> 1)
    } else {
        scoreCounts += (nhtSubCatCountKey -> 0)
    }

    // the count of pnht & mw should be 0, i.e. no impressions with "mw" in sadEvidence should be marked as pnht
    if ( fraudScoresDict.contains("pnht") && ! mwPattern.findAllIn(chunks(sadEvidenceIndex)).isEmpty) {
        scoreCounts += (pnhtAndMwCountKey -> 1)
    } else {
        scoreCounts += (pnhtAndMwCountKey -> 0)
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

sc.textFile("/user/etlstage/quality/logs/2019/03/27/05/impressions/*").
    map(line => ("counts",scoreCountsMapGenerator(line))).
    reduceByKey(mapCountingReducer(_,_)).
    collect()
