import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

%default DATE_TIME '2018/08/08/12'
%default JIRA 'SAD-4593'

-- qlogs = LOAD_JOINED('/user/thresher/quality/logs/$DATE_TIME/impressions/*');
-- qlogs_sample = FILTER qlogs BY ( givt == 0 AND scores MATCHES '.* rsa=1000.*');
-- qlogs_sample_limited = LIMIT qlogs_sample 1;
-- rmf $JIRA/qlog
-- STORE qlogs_sample_limited INTO '$JIRA/qlog' USING PigStorage('\t');

%default pattern '(03f319ce-d417-616d-8840-c585fdb2b50d|04432a78-d066-ea83-f24c-22009c1842f9|04c97b2c-3ab4-3df3-59bc-e15a93879d93|08e7d1da-fede-0830-1a46-8d24d1499d14|0ad78cc7-190e-5f61-01e3-f12edd831ec8|0cd14416-93ed-6be3-4ac5-d4c969a9d6e0|0ebef627-537d-9e60-0a0a-a07976a719ab|0fdced7d-01a3-e575-a0c5-ae6390e30e32|100c5c0a-ffe3-2b9a-3709-dafaa9a6473b|10262b46-4d89-2f0a-f02d-c3655e185766)'

events = load '/user/thresher/event/$DATE_TIME/00/*' using PigStorage('\t');
events_filtered = FILTER events BY ( $36 MATCHES '.* id=$pattern.*');
events_out = FOREACH (GROUP events_filtered BY 1) GENERATE FLATTEN(events_filtered);
rmf $JIRA/event
STORE events_out INTO '$JIRA/event' USING PigStorage('\t');


-- dts = LOAD_RAWDT('/user/thresher/dt/raw/$DATE_TIME/00/*');
-- dts_filtered = FILTER dts BY ( asid MATCHES '$pattern');
-- dts_out = FOREACH (GROUP dts_filtered BY 1) GENERATE FLATTEN(dts_filtered);
-- rmf $JIRA/dt
-- STORE dts_out INTO '$JIRA/dt' USING PigStorage('\t');
