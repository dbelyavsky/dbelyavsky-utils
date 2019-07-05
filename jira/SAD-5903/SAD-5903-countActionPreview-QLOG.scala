val actionIndex=16
var givtIndex=80

val DATE_PATH = "2019/07/01/12"
val PROD_QLOG = "/user/thresher/quality/logs"

def scoreCountsMapGenerator(
                             line : String
                           ) : scala.collection.mutable.Map[String,Long] = {
  var scoreCounts : scala.collection.mutable.Map[String,Long] = scala.collection.mutable.Map()
  val chunks = line.split("\t")

  scoreCounts += ("TOTAL" -> 1)
  scoreCounts += ("actionPreviewNOTGIVT" -> (
    if ( "preview".equals(chunks(actionIndex)) && "1".equals(chunks(givtIndex)) ) {
      1
    } else {
      0
    })
    )

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

sc.textFile(s"$PROD_QLOG/$DATE_PATH/impressions/*").
  map(line => ("counts",scoreCountsMapGenerator(line))).
  reduceByKey(mapCountingReducer(_,_)).
  collect()
