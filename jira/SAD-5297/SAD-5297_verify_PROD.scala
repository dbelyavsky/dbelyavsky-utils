import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
  
val JIRA = "SAD-5297"
val YEAR = "2019"
val MONTH = "03"
val DAY = 20
val HOUR = 05

val IN_BASE = "/user/thresher/quality"
val IN_PATH_QLOG = f"${IN_BASE}/logs/${YEAR}/${MONTH}/${DAY}%02d/${HOUR}%02d/impressions/*"
val IN_PATH_PREMART = f"${IN_BASE}/scores/aggdt4/${YEAR}/${MONTH}/${DAY}%02d/${HOUR}%02d/*"

val OUT_BASE = f"${JIRA}/${DAY}%02d/${HOUR}%02d"
val OUT_PATH_QLOG = f"${OUT_BASE}/impressions"
val OUT_PATH_PREMART = f"${OUT_BASE}/aggdt4"

val qlogFraudScoresIndex=95
val premartFraudScoresIndex=115

def filterQLOG( line : String) : Boolean = {
    val fields = line.split("\t")
    fields(qlogFraudScoresIndex).contains("habs")
}

def filterPREMART( line : String) : Boolean = {
    val fields = line.split("\t")
    fields(premartFraudScoresIndex).contains("habs")
}

val fs = FileSystem.get(sc.hadoopConfiguration)
if( fs.exists( new Path(OUT_PATH_QLOG) ) )
  fs.delete( new Path(OUT_PATH_QLOG), true)
if( fs.exists( new Path(OUT_PATH_PREMART) ) )
  fs.delete( new Path(OUT_PATH_PREMART), true)

sc.textFile(IN_PATH_QLOG).
    filter(line => filterQLOG(line)).
    repartition(1).
    saveAsTextFile(OUT_PATH_QLOG)

sc.textFile(IN_PATH_PREMART).
    filter(line => filterPREMART(line)).
    repartition(1).
    saveAsTextFile(OUT_PATH_PREMART)
