%declare RESULTS_OUT 'verify-staging.ETL-5520';

qlog_new = load '/user/etlstage/quality/logs/2017/05/25/*/impressions/impressions*' using PigStorage('\t');
-- qlog_old = load '/user/etlstage/quality/logs/2017/05/23/*/impressions/impressions*' using PigStorage('\t');

multipleEvents_new = filter qlog_new by $41 > 1;
-- multipleEvents_old = filter qlog_old by $41 > 1;

countDtMinimizer = FOREACH (GROUP multipleEvents_new BY $86) GENERATE FLATTEN(group), COUNT(multipleEvents_new);
dump countDtMinimizer;

-- rmf $RESULTS_OUT
-- store results into '$RESULTS_OUT' using PigStorage('\t');

