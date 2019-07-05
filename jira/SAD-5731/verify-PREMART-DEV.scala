import org.apache.hadoop.fs.{Path,FileSystem}

val fraudScoresIndex=115

// extract all the DS scored records 
// def filterLogs (line : String) : Boolean = {
//     val fields = line.split("\t")
//     if ( fields(fraudScoresIndex).contains("#") ) {
//         val scores = fields(fraudScoresIndex).replaceAll("[\\[\\] ]","")
//             .split(",")
//             .map(_.split("#"))
//             .map(v => v(0) -> v(1))
//             .toMap
     
//         scores.contains("ds")
//     } else {
//        false
//     }
// }

val inputDir = "quality.SAD-5731-DEV/scores/aggdt4/2019/04/30/12/*"
//val outputDir = "SAD-5731/data/aggdt4-DEV/"

// FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputDir), true)
// sc.textFile(inputDir).
//     filter(line => filterLogs(line)).
//     repartition(20).
//     saveAsTextFile(outputDir)

val scoreKeys = List("ds", "dsu", "dsr", "dse", "sivt")
 
def scores2Dict(
    line : String
) : scala.collection.immutable.Map[String,Long] = {
    if ( line.contains("#") ) {
        line.replaceAll("[\\[\\] ]","").split(",").map(_.split("#")).map(v => v(0) -> v(1).toLong).toMap
    } else {
        scala.collection.immutable.Map[String,Long]()
    }
}

def scoreCountsMapGenerator(
    line : String
) : scala.collection.mutable.Map[String,Long] = {
    var scoreCounts : scala.collection.mutable.Map[String,Long] = scala.collection.mutable.Map()
    val chunks = line.split("\t")
    val fraudScoresDict = scores2Dict(chunks(fraudScoresIndex))
 
    scoreCounts += ("TOTAL" -> 1)
 
    scoreKeys foreach { 
        scoreKey => {
            if (fraudScoresDict.contains(scoreKey)) {
                scoreCounts += (scoreKey -> fraudScoresDict(scoreKey))
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
        key => countsThis(key) = countsThis(key) + countsThat(key)
    }
    countsThis
}
 
sc.textFile(inputDir).
    map(line => ("counts",scoreCountsMapGenerator(line))).
    reduceByKey(mapCountingReducer(_,_)).
    collect()

// res9: Array[(String, scala.collection.mutable.Map[String,Long])] = Array((counts,Map(sivt -> 3497451, dse -> 0, TOTAL -> 162600674, dsu -> 2989, dsr -> 0, ds -> 2989)))