/*
    Get a sample of impressions that underly the DomainSpoofing report per 

    p-etl09{dbelyavsky}~ [19] $ zcat /tmp/Domain_Spoofing_7688_2018_11_27.gz | awk -F '\t' 'BEGIN { OFS="\t" } $5 > 0 {print $0}' | head
    Publisher    Site_Id    Bid_URL    URL    Mismatch_Count    Total
    156078    173321    topixpawsome.com    http://richmetrics.com    1    1
    47150    98424    http://seesaa.net/    http://www.yasube3gou.com    1    1
    137711    137812    http://9gag.com/nsfw    http://sitepoint.com    1    1
    47150    98424    http://seesaa.net/s/    http://www.tanoshiikoto.xyz    1    1
    156377    206807    https://uk.yahoo.com/    http://adacado.com    2    2
    47150    98424    http://blog.seesaa.jp/    http://blog.tsdo.net    2    2
    47150    47151    http://blog.seesaa.jp/    http://dawman.seesaa.net    1    1
    47150    47151    http://blog.seesaa.jp/    http://bike--z.seesaa.net    1    1
    47150    98424    http://blog.seesaa.jp/    http://ccalpha.seesaa.net    2    2

*/
val inputPath = "/user/thresher/quality/logs/2018/11/27/*/impressions/*"
val outputPath="SAD-5317"

val extAdNetworkIds = Array("7688")

val filterData = Array (
    ("156078","173321","topixpawsome.com","http://richmetrics.com"),
    ("47150","98424","http://seesaa.net/","http://www.yasube3gou.com"),
    ("137711","137812","http://9gag.com/nsfw","http://sitepoint.com"),
    ("47150","98424","http://seesaa.net/s/","http://www.tanoshiikoto.xyz"),
    ("156377","206807","https://uk.yahoo.com/","http://adacado.com"),
    ("47150","98424","http://blog.seesaa.jp/","http://blog.tsdo.net"),
    ("47150","47151","http://blog.seesaa.jp/","http://dawman.seesaa.net"),
    ("47150","47151","http://blog.seesaa.jp/","http://bike--z.seesaa.net"),
    ("47150","98424","http://blog.seesaa.jp/","http://ccalpha.seesaa.net")
)

val hostIndex = 9
val siteIdIndex = 12
val extAdnetworkIdIndex = 17
val extPublisherIdIndex = 21
val extChannelIdIndex = 22
val ext_bidUrlIndex = 53

 // conditions for filtering of the logs
 def filterLogs(logLine:String) : Boolean = {
    val chunks = logLine.split("""\t""")
    (
        ( extAdNetworkIds contains chunks(extAdnetworkIdIndex) )
        && ( filterData contains ( chunks(extPublisherIdIndex), chunks(extChannelIdIndex), chunks(ext_bidUrlIndex), chunks(hostIndex) ) )  
    )
}

sc.textFile(inputPath).filter(line => filterLogs(line)).repartition(1).saveAsTextFile(outputPath)
