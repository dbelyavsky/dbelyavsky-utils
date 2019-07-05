%default JIRA 'SAD-4849'
%default MONTH '05'

%default INVALID_COUNTS_INPUT '$JIRA/invalid*$MONTH*.csv'
%default FRAUD_INPUT '$JIRA/2018/$MONTH/*/*/part*'
%default OUTPUT '$JIRA/final/$MONTH' 

invalid_counts = LOAD '$INVALID_COUNTS_INPUT' USING PigStorage(',') AS (timestamp, invalidCount);
invalid_total = FOREACH (GROUP invalid_counts ALL) GENERATE SUM($1.invalidCount) as invalidCount;

fraud_counts = LOAD '$FRAUD_INPUT' USING PigStorage('\t') AS (
    date,
    hour,
    totalCount,
    abfBot,
    iabBrowser,
    iabBot,
    sivtTotal,
    ha,
    nht1,
    nht2,
    nht3,
    nht4,
    nht5,
    nht6,
    nht7
);

fraud_counts_total = FOREACH (GROUP fraud_counts ALL) GENERATE 
    SUM($1.totalCount) AS totalCount,
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

result = CROSS fraud_counts_total, invalid_total;

RMF $OUTPUT
STORE result INTO '$OUTPUT' USING PigStorage('\t');
