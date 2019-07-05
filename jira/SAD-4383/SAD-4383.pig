import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

qlog = LOAD_JOINED('/user/thresher/quality/logs/2018/04/19/12/impressions/*');

filtered = filter qlog by userAgentStr matches '.*FetchTV.*';

rmf SAD-4383.qlog
store filtered into 'SAD-4383.qlog' using PigStorage('\t');
