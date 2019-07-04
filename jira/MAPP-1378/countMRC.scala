val mrcAccreditedIndex=107
val fraudScoresIndex=115

val inputDirUSER = "/user/vbelide/MAPP-1378-Canary-FilteredAgencyData-CanaryCode-VbelideUserProfile"
val inputDirETLSTAGE = "/user/vbelide/MAPP-1378-Canary-FilteredAgencyData-MasterCode-EtlStageUserProfile"

def scoreCountsMapGenerator(
                             line : String
                           ) : scala.collection.mutable.Map[String,Long] = {
  var counts : scala.collection.mutable.Map[String,Long] = scala.collection.mutable.Map()
  val chunks = line.split("\t")

  counts += ("TOTAL" -> 1)

  val mrc = chunks(mrcAccreditedIndex)

  counts += ("MRCAccredited" -> (if ("1" == mrc) 1 else 0))

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
