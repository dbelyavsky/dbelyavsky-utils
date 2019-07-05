IMPORT '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

qlog = LOAD_JOINED('/user/etlstage/quality/logs/2018/02/08/*/impressions/imp*');
filtered = FILTER qlog BY (lookupId IN (124838) 
	AND impressionScores MATCHES '.*\\Wsus=1.0\\W.*' 
	AND NOT fraudScores MATCHES '.*\\Wsivt=1.0\\W.*');
rmf ETL-7256.qlog
STORE filtered INTO 'ETL-7256.qlog' USING PigStorage('\t');

------------------------------------------
-- events = LOAD_EVENTS('/user/etlstage/event/2018/02/0{2,3,4}/*/00/LOG*');
-- filtered = FILTER events BY (javascriptInfo matches '.*\\Wid=(c9cd1587-6dbc-c281-649c-be9f558d2877|d33653df-7339-b680-67e4-7e3f774231e4|92e89b71-5af4-0f5a-7d13-caeaba25fe21|5d6a03ff-52ee-57ce-f1e7-ced23148e909)\\W.*');
-- filtered = FILTER events BY (javascriptInfo matches '.*\\Wid=(ffa99897-42a1-e67e-c484-8bbc69925cf2|b6ec5d8f-42dc-59ac-2247-42aeb0e69728|a5d1f9b7-4221-3762-e7ec-f0dd6919183b)\\W.*');
-- rmf ETL-7256.events
-- STORE filtered INTO 'ETL-7256.events' USING PigStorage('\t');

-------------------------------------
-- premart = LOAD_QUALITY_PREMART('/user/etlstage/quality/scores/aggdt3/2018/02/06/*/*');
-- filtered = FILTER premart by lookupId == 132184;
-- rmf ETL-7256.aggdt3
-- STORE filtered INTO 'ETL-7256.aggdt3' USING PigStorage('\t');
