object SQLConvert {
  def main(args: Array[String]): Unit = {
    val source = if (args.length > 0) scala.io.Source.fromFile(args(0)) else scala.io.Source.stdin
    var lines = try source.mkString finally source.close()

    import java.io._
    val fw = if (args.length > 0) new FileWriter(new File(args(0) + ".out")) else new OutputStreamWriter(System.out)
    try fw.write(replaceAll(lines)) finally fw.close()

  }

  def replaceAll(str : String) : String = {
    var lines = str
    var sb = new StringBuilder
    while ( lines != ""){
      val tpl = findIt(lines);
      sb.append(tpl._1)
      if ( tpl._2 != "" ) {
        println(tpl._2)
        val cvt = replaceAll(convert((tpl._2)))
        println(cvt)
        sb.append(cvt)
      }
      lines = tpl._3
    }
    return sb.toString()
  }

  def findIt(str: String): (String, String, String) = {
    import scala.util.matching.Regex

    val ptn="""(?is)(?<![$])\bIf\b\s*\(""".r
    val im = ptn.findAllMatchIn(str)
    for ( m <- im){
      val end = findMatchingRightP(str, m.end, 1)
      if ( end >= m.end ){
        val s1 = if (m.start == 0) "" else str.substring(0, m.start)
        val s2 = str.substring(m.start, end+1)
        val s3 = str.substring(end+1)
        return (s1, s2, s3)
      }
    }
    return (str, "", "")

  }

  def findMatchingRightP(str: String, start: Int, cnt : Int) : Int = {
    var end = start
    var cv = cnt
    if (cv <= 0)return start

    for (c <- str.substring(start) ) {
      cv = c match  {
        case ')' => cv - 1
        case '(' => cv + 1
        case _ => cv
      }
      if (cv == 0) return end
      end += 1
    }
    return -1

  }

  def findComma(str: String, start: Int) : Int = {
    var end = start
    var cv = 0
    for (c <- str.substring(start) ) {
      c match  {
        case ')' => cv = cv - 1
        case '(' => cv = cv + 1
        case ',' => if (cv == 0) return end
        case _ => cv
      }
      end += 1
    }
    return -1

  }

  def convert(stmt: String) : String =  {
    val ptn="""(?is)(?<![$])\bIf\b\s*\(""".r
    val om = ptn.findFirstMatchIn(stmt)
    if (om.isEmpty) throw new RuntimeException("IF not found")
    var stmt1 = stmt.substring(om.get.end, stmt.length - 1)

    val c1 = findComma(stmt1, 0)
    if ( c1 < 0 ) throw new RuntimeException("can't find first comma" + stmt1 )
    val c2 = findComma(stmt1, c1 + 1)
    if ( c2 < 0 ) throw new RuntimeException("can't find second comma" + stmt1 )
    return "CASE WHEN " + stmt1.substring(0, c1).trim() + " THEN " + stmt1.substring(c1 + 1, c2).trim() + " ELSE " + stmt1.substring(c2+1).trim() + " END "
  }
}

