%declare QLOG_SAMPLE_OUT 'qlog_sample.20170416';
%declare EVENT_SAMPLE_OUT 'event_sample.20170416';
%declare DT_SAMPLE_OUT 'dt_sample.20170416';
%declare ASID_COUNT_OUT 'qlog_asid_count.20170416';


-- qlog = load 'quality.SAD-2762.BRANCH3/logs/2017/04/16/12/impressions/*' using PigStorage('\t');
-- rmf $QLOG_SAMPLE_OUT
-- qlog_dup_dts = filter qlog by $86 matches '.*res1:.*res1:.*res1:.*';
-- store qlog_dup_dts into '$QLOG_SAMPLE_OUT' using PigStorage('\t');

event = load 'event.20170416/20170416120000/*' using PigStorage('\t');
event_sample = filter event by ($36 MATCHES '.*id=(8463d555-e00a-6ff0-fa0e-395f14d20acb|00aba0b3-7392-28bd-4f97-2d44479690b0).*');
rmf $EVENT_SAMPLE_OUT
store event_sample into '$EVENT_SAMPLE_OUT' using PigStorage('\t');

dt = load 'dt.20170416/raw/20170416120000/DT.*.dt*' using PigStorage('\t');
dt_sample = filter dt by ($8 MATCHES '.*(8463d555-e00a-6ff0-fa0e-395f14d20acb|00aba0b3-7392-28bd-4f97-2d44479690b0).*' OR $13 MATCHES '.*(8463d555-e00a-6ff0-fa0e-395f14d20acb|00aba0b3-7392-28bd-4f97-2d44479690b0).*');
rmf $DT_SAMPLE_OUT
store dt_sample into '$DT_SAMPLE_OUT' using PigStorage('\t');

-- find all events whose adID was duplicated
event = load 'event.20170416/20170416120000/*' using PigStorage('\t');
event_group_by_asID = GROUP event BY REGEX_EXTRACT($36, '\\Wid=([0-9a-zA-Z-]+)', 0);
event_count_asIDs = FOREACH event_group_by_asID GENERATE FLATTEN(group) as asId, COUNT(event) as count;
dup_asIds = FILTER event_count_asIDs BY count > 1;
rmf $ASID_COUNT_OUT
STORE dup_asIds INTO '$ASID_COUNT_OUT' USING PigStorage('\t');

-- random asID that was duplicated search
-- id=faeaa209-ce26-56db-ef7d-b1af55811563	3x
qlog = load 'quality.SAD-2762.BRANCH3/logs/2017/04/16/12/impressions/*' using PigStorage('\t');
qlog_from_dup = FILTER qlog BY $37 MATCHES '.*id=faeaa209-ce26-56db-ef7d-b1af55811563.*';
event_sample = FILTER event BY $36 MATCHES '.*id=faeaa209-ce26-56db-ef7d-b1af55811563.*';
dt_sample = FILTER dt BY ($8 MATCHES '.*faeaa209-ce26-56db-ef7d-b1af55811563.*' OR $13 MATCHES '.*faeaa209-ce26-56db-ef7d-b1af55811563.*');
rmf $QLOG_SAMPLE_OUT
store qlog_from_dup into '$QLOG_SAMPLE_OUT' using PigStorage('\t');
rmf $EVENT_SAMPLE_OUT
store event_sample into '$EVENT_SAMPLE_OUT' using PigStorage('\t');
rmf $DT_SAMPLE_OUT
store dt_sample into '$DT_SAMPLE_OUT' using PigStorage('\t');
