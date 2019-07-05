import org.apache.hadoop.fs.{Path,FileSystem}
import scala.util.{Try, Success, Failure}

"""
- so need to do this `sca_results RLIKE '"bt":"firefox"' and sca_results RLIKE '"bv":"63"'` 
- for Firefox 60 you should see `sca_results RLIKE '"n":1,"r":"1"'` and `sca_results RLIKE '"n":17,"r":"1"'` to co-appear
- for Firefox 63 you should not see `sca_results RLIKE '"n":1,"r":"1"'` and should see some `sca_results RLIKE '"n":17,"r":"1"'`
"""

val scaResultsIdx = 82

val datePath = "2019/05/23/00"
val inputPath = s"/user/thresher/quality/logs/${datePath}/impressions/*"
val outputPath= s"SAD-5343/impressions/${datePath}"

val overallResultPattern = """.*"r":(\d+),"rules":.*""".r
val browserTypePattern = """.*"bt":"(\w+)".*""".r
val browserVersionPattern = """.*"bv":"(\d+)".*""".r

def filterSca(scaStr:String) : Try[Boolean] = {
  Try {
    //    val overallResultPattern(rrResult) = scaStr
    val browserTypePattern(bt) = scaStr
    val browserVersionPattern(bv) = scaStr

    //    ( rrResult.toInt == 1
    //      && (
    (bt == "firefox" && bv.toInt >= 55 ) ||
      (bt == "chrome" && bv.toInt >= 65 )
    //      )
    //      )
  }
}

def filterMe(logLine : String) : Boolean = {
    val chunks = logLine.split("""\t""")
    filterSca(chunks(scaResultsIdx)) match {
        case Success(v) => v
        case Failure(e) => false
    }
}



@transient val fs = FileSystem.get(sc.hadoopConfiguration)
FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputPath), true)
sc.textFile(inputPath).
    filter(line => filterMe(line)).
    saveAsTextFile(outputPath)
