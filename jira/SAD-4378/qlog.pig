import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

%declare JIRA 'SAD-4378';
%declare QLOG_OUT '$JIRA.qlog';

-- qlogs = LOAD_JOINED('/user/thresher/quality/logs/2018/01/25/*/impressions/*');

-- qlogs_filtered = filter qlogs by lookupId == 119040;

-- rmf $QLOG_OUT
-- store qlogs_filtered into '$QLOG_OUT' using PigStorage('\t');

%declare QLOG_FRAUD '$JIRA.qlog.fraud';
my_qlogs = LOAD_JOINED('$QLOG_OUT');
qlogs_fraud = filter my_qlogs by impressionScores matches '.*, sus=1.*';
rmf $QLOG_FRAUD
store qlogs_fraud into '$QLOG_FRAUD' using PigStorage('\t');
