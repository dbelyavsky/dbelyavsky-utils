val impsIndex=19
val generalInvalidImpsindex=101
val mrcAccreditedIndex=107
val fraudScoresIndex=115

val inputDirUSER = "/user/vbelide/MAPP-1378-Canary-FilteredAgencyData-CanaryCode-VbelideUserProfile"
val inputDirETLSTAGE = "/user/vbelide/MAPP-1378-Canary-FilteredAgencyData-MasterCode-EtlStageUserProfile"

def scores2Dict(
                 line : String
               ) : scala.collection.immutable.Map[String,Long] = {
  if ( line.contains("#") ) {
    line.replaceAll("[\\[\\]]","").split(",").map(_.split("#")).map(v => v(0) -> v(1).toLong).toMap
  } else {
    scala.collection.immutable.Map[String,Long]()
  }
}

def scoreCountsMapGenerator(
                             line : String
                           ) : scala.collection.mutable.Map[String,Long] = {
  var counts : scala.collection.mutable.Map[String,Long] = scala.collection.mutable.Map()

  val chunks = line.split("\t")

  val ivtScores = scores2Dict(chunks(fraudScoresIndex))

  val imps = chunks(impsIndex).toDouble
  counts += ("TOTAL" -> imps.toLong)

  val mrc = chunks(mrcAccreditedIndex)

  counts += ("MRCAccredited" -> (if ("1" == mrc) 1 else 0))

  List("sivt", "nht", "ha", "ds", "pnht", "pnht1", "pnht2") foreach {
    scoreKey => {
      if (ivtScores.contains(scoreKey)) {
        counts += (scoreKey -> ivtScores(scoreKey))
      } else {
        counts += (scoreKey -> 0)
      }
    }
  }

  val givt = chunks(generalInvalidImpsindex).toDouble
  counts += ("givt" -> givt.toLong)

  counts
}

def mapCountingReducer(
                        countsThis:scala.collection.mutable.Map[String,Long]
                        , countsThat:scala.collection.mutable.Map[String,Long]
                      ) : scala.collection.mutable.Map[String,Long] = {
  countsThis.keys.foreach {
    key => countsThis(key) = countsThis(key) + countsThat(key)
  }
  countsThis
}


sc.textFile(inputDirUSER).
  map(line => ("countsUSER",scoreCountsMapGenerator(line))).
  reduceByKey(mapCountingReducer(_,_)).
  collect()


sc.textFile(inputDirETLSTAGE).
  map(line => ("countsETLSTAGE",scoreCountsMapGenerator(line))).
  reduceByKey(mapCountingReducer(_,_)).
  collect()
