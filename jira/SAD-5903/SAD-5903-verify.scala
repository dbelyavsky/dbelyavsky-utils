// EvengAgg:apc report fields (field 0 is the "apc" report name)
val apcHitDateIndex = 1
val apcHourIndex = 2
val apcLookupIdIndex = 3
val apcPassbackIdIndex = 4
val apcExtAdnetworkIdIndex = 5
val apcImpsIndex = 6
val apcMediaTypeIndex = 7
val apcOriginalLookupIdIndex = 8
val apcOriginalAdnetworkIdIndex = 9

val OUPUT_BASE = "jira/SAD-5903/verifyDev"
val DATE_PATH = "2019/07/01/12"
val DEV_EVENTAGG = "aggdt/apc"
val PROD_EVENTAGG = "/user/thresher/aggdt/apc"

def scoreCountsMapGenerator(
                             line : String
                           ) : scala.collection.mutable.Map[String,Long] = {
  var scoreCounts : scala.collection.mutable.Map[String,Long] = scala.collection.mutable.Map()
  val chunks = line.split("\t")

  val imps = if (
    "null".equals(chunks(apcOriginalLookupIdIndex))
      && "null".equals(chunks(apcOriginalAdnetworkIdIndex)))
  {
    chunks(apcImpsIndex).toInt
  } else {
    0
  }

  val givt = if (
    "null".equals(chunks(apcOriginalLookupIdIndex))
      && "null".equals(chunks(apcOriginalAdnetworkIdIndex)))
  {
    0
  } else {
    chunks(apcImpsIndex).toInt
  }

  scoreCounts += ("TOTAL" -> (imps + givt))
  scoreCounts += ("imps" -> imps)
  scoreCounts += ("givt" -> givt)

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

sc.textFile(s"$DEV_EVENTAGG/$DATE_PATH/*").
  map(line => ("counts",scoreCountsMapGenerator(line))).
  reduceByKey(mapCountingReducer(_,_)).
  collect()

sc.textFile(s"$PROD_EVENTAGG/$DATE_PATH/*").
  map(line => ("counts",scoreCountsMapGenerator(line))).
  reduceByKey(mapCountingReducer(_,_)).
  collect()




