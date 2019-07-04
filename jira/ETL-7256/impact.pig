import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

-- find all IMPs from PROD data where scores=ERROR and sus=1.0
qlog = LOAD_JOINED('/user/thresher/quality/logs/2018/01/31/12/impressions/imp*');

qlog_groupall = GROUP qlog ALL;
qlog_count = FOREACH qlog_groupall GENERATE COUNT(qlog);

sus = FILTER qlog by impressionScores MATCHES '.*\\Wsus=1.0\\W.*';
sus_groupall = GROUP sus ALL;
sus_count = FOREACH sus_groupall GENERATE COUNT(sus);

error = FILTER qlog BY scores == 'ERROR';
error_groupall = GROUP error ALL;
error_count = FOREACH error_groupall GENERATE COUNT(error);

error_sus = FILTER error BY impressionScores MATCHES '.*\\Wsus=1.0\\W.*';
error_sus_groupall = GROUP error_sus ALL;
error_sus_count = FOREACH error_sus_groupall GENERATE COUNT(error_sus);

all_counts = UNION qlog_count, sus_count, error_count , error_sus_count;

dump all_counts

-- rmf ETL-7256.impact
-- store error_sus into 'ETL-7256' using PigStorage('\t');
