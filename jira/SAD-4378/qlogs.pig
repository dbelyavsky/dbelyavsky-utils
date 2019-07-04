import '/Users/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

%declare JIRA 'SAD-4378';
%declare QLOG_OUT '$JIRA.qlog';

qlogs = LOAD_JOINED('/user/thresher/quality/logs/2018/01/25/*/impressions/*');

qlogs_filtered = filter qlogs by lookupId == 119040;

rmf $QLOG_OUT
store qlogs_filtered into `$QLOG_OUT` using PigStorage('\t');
