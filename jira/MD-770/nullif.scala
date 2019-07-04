object NullIf {
  object TokenType extends Enumeration {
    val TERM = Value
    val WHITESPACE = Value
    val IF_MACRO = Value
    val ENDIF_MACRO = Value
    val MACRO = Value
    val FUNC_START = Value
    val LEFT_PARENTHESES = Value
    val RIGHT_PARENTHESES = Value
    val CASE = Value
    val END = Value
    //val AS = Value
    val PUNC = Value
    val DIV = Value
    val EOF = Value
    val STRING = Value
  }
  case class Token(val v:String, val t: TokenType.Value, val s:Int);
  var m_POS:Int = 0
  val Null_If:String = "NULLIF("

  def main(args: Array[String]): Unit = {
    val source = scala.io.Source.fromFile("risk_category_columns.stg")
    var lines = try source.mkString finally source.close()

    import java.io._
    val fw = new FileWriter(new File("risk_category_columns.nullif.out"))
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
        val cvt = replaceAll(wrapWithNullIf((tpl._2)))
        println(cvt)
        sb.append(cvt)
      }
      lines = tpl._3
    }
    return sb.toString()
  }

  // find the div operator
  def findIt(str: String): (String, String, String) = {
    m_POS = 0
    var idx = str.indexOf('/', m_POS)
    import scala.util.matching.Regex
    while (idx >= 0){
      m_POS = idx+1;
      // skip "//" as comment
      if ( m_POS < str.size && str.charAt(m_POS) == '/' ){
        m_POS +=1
        idx = str.indexOf('\n', m_POS)
        if ( idx >= 0 ){
          m_POS = idx
        }
        idx = str.indexOf('/', m_POS)
      } else {
        findTerm(str)
        if ( m_POS == str.size ){
          return (str.substring(0,idx+1),str.substring(idx+1,m_POS), "")
        } else {
          return (str.substring(0,idx+1),str.substring(idx+1,m_POS), str.substring(m_POS))
        }

      }

    }
    return (str, "", "")

  }

  def findTerm(str: String){
    var start = m_POS
    var tk = nextToken(str)
    var stack = List[Token]()
    if (tk.t == TokenType.WHITESPACE){
      start = m_POS
      tk = nextToken(str)
    }
    var done = false
    while (!done){
      tk.t match {
        case TokenType.EOF => done = true
        case TokenType.FUNC_START => stack = tk :: stack
        case TokenType.IF_MACRO => stack = tk :: stack
        case TokenType.ENDIF_MACRO => stack = tk :: stack
        case TokenType.CASE => stack = tk :: stack
        case TokenType.LEFT_PARENTHESES => stack = tk :: stack
        case TokenType.RIGHT_PARENTHESES =>
          val tkOpt: Option[Token] = stack.headOption
          if (!tkOpt.isEmpty && (tkOpt.get.t == TokenType.LEFT_PARENTHESES || tkOpt.get.t == TokenType.FUNC_START)) {
            stack = stack.tail
            if (stack.size == 0){
              done = true
            }
          } else {
            if ( stack.size == 0){
              unread(tk)
              done = true
            } else {
              throw new RuntimeException("unmatched " + tk.v)
            }

          }
        case TokenType.END =>
          val tkOpt: Option[Token] = stack.headOption
          if (!tkOpt.isEmpty && (tkOpt.get.t == TokenType.CASE)) {
            stack = stack.tail
            if (stack.size == 0){
              done = true
            }
          } else {
            if (stack.size == 0){
              unread(tk)
              done = true
            } else {
              throw new RuntimeException("unmatched " + tk.v)
            }

          }
        case TokenType.ENDIF_MACRO =>
          val tkOpt: Option[Token] = stack.headOption
          if (!tkOpt.isEmpty && (tkOpt.get.t == TokenType.IF_MACRO)) {
            stack = stack.tail
          } else {
            if (stack.size == 0){
              unread(tk)
              done = true
            } else {
              throw new RuntimeException("unmatched " + tk.v)
            }

          }
        case TokenType.WHITESPACE | TokenType.PUNC | TokenType.DIV | TokenType.STRING =>
          if (stack.size == 0){
            unread(tk)
            done = true
          }
        case _ =>

      }
      if (!done){
        tk = nextToken(str)
      }
    }


  }

  def unread(token: Token) = {
    m_POS -= token.v.size
  }

  def nextToken(str:String):Token = {
    if (m_POS == str.size ){
      return Token("", TokenType.EOF, m_POS)
    }
    val prob = str.substring(m_POS)
    val ptn_ws="""(?is)^\s+""".r
    ptn_ws.findFirstIn(prob) match {
      case None => ;
      case Some(x) => m_POS += x.size;
        return Token(x, TokenType.WHITESPACE, m_POS)
    }
    val ptn_STR1="""(?is)^'[^']*'""".r
    ptn_STR1.findFirstIn(prob) match {
      case None =>  ;
      case Some(x) => m_POS += x.size;
        return Token(x, TokenType.STRING, m_POS)
    }
    val ptn_STR2="""(?is)^"[^"]*'""".r
    ptn_STR2.findFirstIn(prob) match {
      case None =>  ;
      case Some(x) => m_POS += x.size;
        return Token(x, TokenType.STRING, m_POS)
    }
    val ptn_ifmacro="""(?is)^\$if[^$]+?\$""".r
    ptn_ifmacro.findFirstIn(prob) match {
      case None =>  ;
      case Some(x) => m_POS += x.size;
        return Token(x, TokenType.IF_MACRO, m_POS)
    }
    val ptn_endifmacro="""(?is)^\$endif[^$]+?\$""".r
    ptn_endifmacro.findFirstIn(prob) match {
      case None =>  ;
      case Some(x) => m_POS += x.size;
        return Token(x, TokenType.ENDIF_MACRO, m_POS)
    }
    val ptn_macro="""(?is)^\$[^$]+?\$""".r
    ptn_macro.findFirstIn(prob) match {
      case None =>  ;
      case Some(x) => m_POS += x.size;
        return Token(x, TokenType.MACRO, m_POS)
    }
    val ptn_CASE="""(?is)^CASE\b""".r
    ptn_CASE.findFirstIn(prob) match {
      case None =>  ;
      case Some(x) => m_POS += x.size;
        return Token(x, TokenType.CASE, m_POS)
    }
    val ptn_END="""(?is)^END\b""".r
    ptn_END.findFirstIn(prob) match {
      case None =>  ;
      case Some(x) => m_POS += x.size;
        return Token(x, TokenType.END, m_POS)
    }
    val ptn_FUNC="""(?is)^[a-z0-9][a-z0-9_\-]*\s*\(""".r
    ptn_FUNC.findFirstIn(prob) match {
      case None =>   ;
      case Some(x) => m_POS += x.size;
        return Token(x, TokenType.FUNC_START, m_POS)
    }

    val ptn_PUNC="""(?is)^(\+|-|\*|\\|/|\(|\)|%|!|,|;|'|"|>|<|=)""".r
    ptn_PUNC.findFirstIn(prob) match {
      case None =>  ;
      case Some(x) => m_POS += x.size;
        x match {
          case ")" => return Token(x, TokenType.RIGHT_PARENTHESES, m_POS)
          case "(" => return Token(x, TokenType.LEFT_PARENTHESES, m_POS)
          case "/" => return Token(x, TokenType.DIV, m_POS)
          case _ => return Token(x, TokenType.PUNC, m_POS)
        }
    }
    val ptn_TERM1="""(?is)^[a-z0-9][a-z0-9_\-]*\s*\.\s*[a-z0-9][a-z0-9_\-]*""".r
    ptn_TERM1.findFirstIn(prob) match {
      case None =>  ;
      case Some(x) => m_POS += x.size;
        return Token(x, TokenType.TERM, m_POS)
    }
    val ptn_TERM2="""(?is)^[a-z0-9][a-z0-9_\-]*""".r
    ptn_TERM2.findFirstIn(prob) match {
      case None =>  ;
      case Some(x) => m_POS += x.size;
        return Token(x, TokenType.TERM, m_POS)
    }
    throw new RuntimeException("Can't parse:" + prob)
  }

  def wrapWithNullIf(expr: String) : String =  {
    val ptn_ws="""(?is)^\s+""".r
    val prefix = ptn_ws.findFirstIn(expr) match {
      case None => ""
      case Some(x) => x
    }
    val exprTrimmed = expr.substring(prefix.size)
    return try {
      exprTrimmed.toFloat
      return expr
    } catch {
      case e: NumberFormatException => return if (exprTrimmed.startsWith(Null_If)) expr else prefix + Null_If + exprTrimmed + ",0)"
    }
  }
}
