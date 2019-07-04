import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

val jira = "SAD-4848"

val asId = "69b2638c-b825-494e-aed0-a0d8c8eaf067"

val e_javascriptInfoIndex=36

val d_asIdIndex=8
val d_timeValIndex=9

val q_javascriptInfoIndex=37

def filterEventLogs (logLine:String) : Boolean = {
    val fields = logLine.split("""\t""")
    fields(e_javascriptInfoIndex).contains(s" id=$asId")
}


def filterDTLogs (logLine:String) : Boolean = {
    val fields = logLine.split("""\t""")
    fields(d_asIdIndex).contains(s"$asId")
}

def filterQualityLogs (logLine:String) : Boolean = {
    val fields = logLine.split("""\t""")
    fields(q_javascriptInfoIndex).contains(s" id=$asId")
}

def rm(pathStr:String) : Boolean = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val aPath = new Path(pathStr)
    if ( fs.exists(aPath) ) {
        fs.delete(aPath, true)
        true
    } else {
        false
    }
}

val configs = Map (
    "event" -> Map (
        "input" -> "/user/thresher/event/2018/06/29/13/*/LOG.*",
        "output" -> s"$jira/event",
        "filter" -> filterEventLogs _),
    "dt" -> Map(
        "input" -> "/user/thresher/dt/raw/2018/06/29/13/*/DT.*",
        "output" -> s"$jira/dt",
        "filter" -> filterDTLogs _),
    "quality" -> Map(
        "input" -> "/user/thresher/quality/logs/2018/06/29/13/impressions/impr*",
        "output" -> s"$jira/quality",
        "filter" -> filterQualityLogs _)
    )

for ((key, conf) <- configs ) {
    rm(conf("output").toString)
    sc.textFile(conf("input").toString).filter(line => conf("filter").asInstanceOf[String => Boolean](line)).repartition(100).saveAsTextFile(conf("output").toString)
}
