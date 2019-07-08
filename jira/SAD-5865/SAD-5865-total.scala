import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Purpose
  * The SAD-5865-hourly.scala will produce hourly counts of various metrics in the following (json) format:
  * {"_1":"2019-05-01-00", "_2":{"tag1":1,"tag2":12}}
  * {"_1":"2019-05-01-01", "_2":{"tag1":23,"tag2":2}}
  * {"_1":"2019-05-01-02", "_2":{"tag1":4,"tag2":8}}
  * {"_1":"2019-05-01-03", "_2":{"tag1":6,"tag2":9}}
  * {"_1":"2019-05-01-04", "_2":{"tag1":10,"tag2":16}}
  *
  * this script will combine the above into total count for the entire month
  */

sc.setLogLevel("ERROR")

//val INPUT = "jira/SAD-5865/hourly/2019/05/*/*/*"
val INPUT = "jira/SAD-5865/SAD-5865-hourly.json"

def toValueMap(line: String) : scala.collection.mutable.Map[String, BigInt] = {
  val jsonObject = parse(line).
    values.
    asInstanceOf[Map[String,Map[String, BigInt]]]

  scala.collection.mutable.Map[String,BigInt](jsonObject("_2").toSeq: _*)
}

def mapCountingReducer(
                        countsThis:scala.collection.mutable.Map[String,BigInt]
                        , countsThat:scala.collection.mutable.Map[String,BigInt]
                      ) : scala.collection.mutable.Map[String,BigInt] = {
  (countsThis.keys ++ countsThat.keys) foreach {
    key => countsThis(key) = countsThis.getOrElse(key,0).asInstanceOf[BigInt] + countsThat.getOrElse(key,0).asInstanceOf[BigInt]
  }
  countsThis
}

sc.textFile(INPUT).
  map(line => ("counts", toValueMap(line))).
  reduceByKey(mapCountingReducer(_, _)).
  collect
