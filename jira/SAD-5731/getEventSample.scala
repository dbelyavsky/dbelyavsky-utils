val event_sadEvidenceIndex=44

val domainSpoofingPattern = "uva".r

def filterLogs (line : String) : Boolean = {
    val fields = line.split("\t")
    domainSpoofingPattern.findFirstIn(fields(event_sadEvidenceIndex)) match {
        case Some(s) => true
        case _ => false
    }
}

sc.textFile("/user/thresher/event/2019/04/18/12/40/LOG.201904181240.app85.8080.final.gz").
    filter(line => filterLogs(line)).
    repartition(20).
    saveAsTextFile(s"SAD-5731/eventSample")
