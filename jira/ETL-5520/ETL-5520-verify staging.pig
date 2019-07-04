-- %declare RESULTS_OUT 'verify-staging.ETL-5520';

qlog_new = load '/user/etlstage/quality/logs/2017/05/25/*/impressions/impressions*' using PigStorage('\t');
-- qlog_old = load '/user/etlstage/quality/logs/2017/05/23/*/impressions/impressions*' using PigStorage('\t');

multipleEvents_new = filter qlog_new by $41 > 1;
-- multipleEvents_old = filter qlog_old by $41 > 1;

countDtMinimizer = FOREACH (GROUP multipleEvents_new BY $86) GENERATE FLATTEN(group), COUNT(multipleEvents_new);
dump countDtMinimizer;

-- rmf $RESULTS_OUT
-- store results into '$RESULTS_OUT' using PigStorage('\t');

/****************** RESULTS **************

...

Input(s):
Successfully read 34603710 records (16737125190 bytes) from: "/user/etlstage/quality/logs/2017/05/25/*/impressions/impressions*"

Output(s):
Successfully stored 1 records (11 bytes) in: "hdfs://nameservice1/tmp/temp-1033042205/tmp1060659912"

...


([],20780)

20780 / 34603710  = 0.0006005136443462276
******************************************/
