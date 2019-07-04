val fraudScoresIndex=95

val nhtSubCategories = Seq ("nht1", "nht2", "nht3", "nht4", "nht5", "nht6", "nht7")

def filterCondition( fraudScoresField : String) : Boolean = {
    if ( fraudScoresField.contains("=") ) {
        val scores = fraudScoresField.replaceAll("[{} ]","").split(",").map(_.split("=")).map(v => v(0) -> v(1)).toMap
    
        scores.contains("nht") && nhtSubCategories.intersect(scores.keySet.toSeq).isEmpty
    } else {
       false
    }
}

def filterLogs (line : String) : Boolean = {
    val fields = line.split("\t")
    filterCondition(fields(fraudScoresIndex))
}

sc.textFile("quality.SAD-4449.dev_test/logs/2019/01/23/12/impressions/*").
    filter(line => filterLogs(line)).
    repartition(20).
    saveAsTextFile("SAD-4449/looseNHTs/")

