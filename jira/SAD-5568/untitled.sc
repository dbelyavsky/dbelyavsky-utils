var m = Map("a" -> 1)

m += ("b" -> 2)
m += ("c" -> 3)
m += ("d" -> 4)

def map2CSV(aMap : Map[String, Int]): String = {
  var header : String = ""
  var data : String = ""

  for (k <- aMap.keys) {
    header += k + ","
    data += aMap(k) + ","
  }

  header + "\n" + data
}

print(map2CSV(m))


