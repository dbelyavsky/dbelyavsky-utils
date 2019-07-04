import org.apache.hadoop.fs.{Path,FileSystem}

val fraudScoresIndex=115

// extract all the DS scored records 
def filterLogs (line : String) : Boolean = {
    val fields = line.split("\t")
    if ( fields(fraudScoresIndex).contains("#") ) {
        val scores = fields(fraudScoresIndex).replaceAll("[\\[\\] ]","")
            .split(",")
            .map(_.split("#"))
            .map(v => v(0) -> v(1))
            .toMap
  
        scores.contains("ds")
    } else {
       false
    }
}

val inputDir = "quality.SAD-5731-DEV/scores/aggdt4/2019/04/30/12/*"
val outputDir = "SAD-5731/data/aggdt4-DEV/"

FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputDir), true)
sc.textFile(inputDir).
  filter(line => filterLogs(line)).
  repartition(20).
  saveAsTextFile(outputDir)
