import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

-- find all IMPs from PROD data where scores=ERROR and sus=1.0
qlog = LOAD_JOINED('quality.ETL-7256.201801311200.base/logs/2018/01/31/12/impressions/imp*');
error = filter qlog by scores == 'ERROR';
error_sus = filter error by impressionScores matches '.*\\Wsus=1.0\\W.*';

rmf qlog.base.error_sus
store error_sus into 'qlog.base.error_sus' using PigStorage('\t');

-- find all IMPs from BRANCH data where scores=ERROR and sus=1.0
qlog = LOAD_JOINED('quality.ETL-7256.201801311200.branch/logs/2018/01/31/12/impressions/imp*');
error = filter qlog by scores == 'ERROR';
error_sus = filter error by impressionScores matches '.*\\Wsus=1.0\\W.*';

rmf qlog.branch.error_sus
store error_sus into 'qlog.branch.error_sus' using PigStorage('\t');
