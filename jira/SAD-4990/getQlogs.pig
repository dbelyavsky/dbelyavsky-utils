IMPORT '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

%default JIRA 'SAD-4990'
%default DATE '2018/09/07'

qlogs = LOAD_JOINED('/user/thresher/quality/logs/$DATE/*/impressions/*');
a_qlog = FILTER qlogs BY ( javascriptInfo MATCHES '.* id=f6d8c76a-56d7-d658-4f7e-c5a334f17b8d.*');
rmf $JIRA/qlog
STORE a_qlog INTO '$JIRA/qlog' USING PigStorage('\t');
