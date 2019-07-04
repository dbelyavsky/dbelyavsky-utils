import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

event = LOAD_EVENTS('/user/thresher/event/20170104*/LOG.20170104*.app01.*');

event_filtered_by_asid = FILTER event BY javascriptInfo matches '.* id=f2a6be3e-a801-3e9b-83a2-dcd65b41a106, .*';

STORE event_filtered_by_asid INTO 'event_sample.20170104.app51' USING PigStorage('\t');

dt = LOAD_RAWDT('/user/thresher/dt/raw/20170104*/DT.20170104*.dt??.*.gz');

dt_filtered_by_asid = FILTER dt by asid == 'f2a6be3e-a801-3e9b-83a2-dcd65b41a106';

STORE dt_filtered_by_asid INTO 'dt_sample.20170104' USING PigStorage('\t');

-- looking for examples of where DT calls appear to be duplicated
dt_dal = LOAD_RAWDT('/user/thresher/dt/raw/20170111*/DT*.dt*dal.*');
dt_with_time = FOREACH dt_dal GENERATE REGEX_EXTRACT(javascriptInfo, '.*,time:(\\d+),.*', 1) as impTime, asid, javascriptInfo;

dt_groupped = GROUP dt_with_time BY (asid, impTime);

-- dt_groupped: {group: (asid: chararray,impTime: chararray),dt_with_time: {(impTime: chararray,asid: chararray,javascriptInfo: chararray)}}
dt_counted = FOREACH dt_groupped GENERATE FLATTEN(group), COUNT(dt_with_time) AS countCalls;

-- dt_counted: {group::asid: chararray,group::impTime: chararray,countCalls: long}
dt_filtered_down = FILTER dt_counted BY countCalls > 2;

STORE dt_filtered_down INTO 'dt_suspicious.dal2' USING PigStorage('\t');

-- dt_dal_filtered_by_asid = FILTER dt_dal BY asid == 'f9ef619a-d0d8-01ab-1f7f-519b2287a747';

dt_dal_filtered_by_asid = FILTER dt_dal BY (asid == '30f91bb6-cb5f-cde2-1590-7433c91d50a2', javascriptInfo MATCHES CONCAT('.*,time:', 15440, ',.*'););

------ sample for 2017/01/13: 5e7855b3-29e3-8ffc-cc49-4e1b5860bd6f

------ filter by domain
qlog = load '/user/thresher/quality/logs/2017/01/13/0[4-6]/impressions/*' using PigStorage('\t');
domain_list = load 'domain_list';
-- qlog_filtered_by_domain = join qlog by REGEX_EXTRACT($10, 'http.?://(.*)', 0), domain_list by $0;
qlog_filtered_by_domain = join qlog by $9, domain_list by $0;
store 'qlog.20170113.04-06' using PigStorage('\t');

qlog_reduced = load 'qlog.20170113.04-06';
