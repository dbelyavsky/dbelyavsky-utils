IMPORT '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

%default HDFS_BASE 'quality/logs'
%default JIRA 'SAD-4849'
%default YEAR '2018'
%default MONTH '07'
%default DAY '09'
%default HOUR '12'

%default INPUT '$HDFS_BASE/$YEAR/$MONTH/$DAY/$HOUR/impressions/*'
%default OUTPUT '$JIRA/$YEAR/$MONTH/$DAY/$HOUR' 

qlog = LOAD_JOINED('$INPUT');

qlog_flagged = FOREACH qlog GENERATE
   (givt == 1 AND fraudScores MATCHES '.*givta=1.0.*' ? 1 : 0) AS abfBot,
   (givt == 1 AND fraudScores MATCHES '.*givtb=1.0.*' ? 1 : 0) AS iabBrowser,
   (givt == 1 AND fraudScores MATCHES '.*givts=1.0.*' ? 1 : 0) AS iabBot,
   (fraudScores MATCHES '.* sivt=1.0.*' ? 1 : 0) AS sivtTotal,
   (fraudScores MATCHES '.* sivt=1.0.*' AND fraudScores MATCHES '.* ha=1.0.*' ? 1 : 0) AS ha,
   (fraudScores MATCHES '.* sivt=1.0.*' AND fraudScores MATCHES '.* nht1=1.0.*' ? 1 : 0) AS nht1,
   (fraudScores MATCHES '.* sivt=1.0.*' AND fraudScores MATCHES '.* nht2=1.0.*' ? 1 : 0) AS nht2,
   (fraudScores MATCHES '.* sivt=1.0.*' AND fraudScores MATCHES '.* nht3=1.0.*' ? 1 : 0) AS nht3,
   (fraudScores MATCHES '.* sivt=1.0.*' AND fraudScores MATCHES '.* nht4=1.0.*' ? 1 : 0) AS nht4,
   (fraudScores MATCHES '.* sivt=1.0.*' AND fraudScores MATCHES '.* nht5=1.0.*' ? 1 : 0) AS nht5,
   (fraudScores MATCHES '.* sivt=1.0.*' AND fraudScores MATCHES '.* nht6=1.0.*' ? 1 : 0) AS nht6,
   (fraudScores MATCHES '.* sivt=1.0.*' AND fraudScores MATCHES '.* nht7=1.0.*' ? 1 : 0) AS nht7;

result = FOREACH (GROUP qlog_flagged ALL) GENERATE
    CONCAT('$YEAR', '-', '$MONTH', '-', '$DAY') as date,
    $HOUR as hour,
    COUNT_STAR(qlog_flagged) as totalCount,
    SUM($1.abfBot) AS abfBotCnt,
    SUM($1.iabBrowser) AS iabBrowserCnt,
    SUM($1.iabBot) AS iabBotCnt,
    SUM($1.sivtTotal) AS sivtTotal,
    SUM($1.ha) AS haCnt,
    SUM($1.nht1) AS nht1Cnt,
    SUM($1.nht2) AS nht2Cnt,
    SUM($1.nht3) AS nht3Cnt,
    SUM($1.nht4) AS nht4Cnt,
    SUM($1.nht5) AS nht5Cnt,
    SUM($1.nht6) AS nht6Cnt,
    SUM($1.nht7) AS nht7Cnt;

RMF $OUTPUT
STORE result INTO '$OUTPUT' USING PigStorage('\t');
