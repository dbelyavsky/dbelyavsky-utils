%declare JIRA 'SAD-2673';
%declare QLOG_SAMPLE_OUT 'qlog_sample.$JIRA';
%declare EVENT_SAMPLE_OUT 'event_sample.$JIRA';
%declare DT_SAMPLE_OUT 'dt_sample.$JIRA';
%declare ASID_COUNT_OUT 'qlog_asid_count.$JIRA';

%declare ASID_PATTERN '429f5ba3-547e-968b-081c-72f88ef82e01|579c26e3-2112-b5b2-7a6d-30e2f660ea49';

event = load '/user/thresher/event/2017050{418,419,420,421,422,423,501}*/LOG.*.app*dal.*.final.gz' using PigStorage('\t');
dt = load '/user/thresher/dt/raw/2017050{417,420,423,501}*/DT*.dt*dal.*.final.gz' using PigStorage('\t')
qlog = load 'quality.SAD-2762.BRANCH3/logs/2017/04/16/12/impressions/*' using PigStorage('\t');

event_filtered = filter event by ($36 MATCHES '.*id=($ASID_PATTERN).*');
rmf $EVENT_SAMPLE_OUT
store event_filtered into '$EVENT_SAMPLE_OUT' using PigStorage('\t');

dt_filtered = filter dt by ($8 MATCHES '.*($ASID_PATTERN).*' OR $13 MATCHES '.*($ASID_PATTERN).*');
rmf $DT_SAMPLE_OUT
store dt_filtered into '$DT_SAMPLE_OUT' using PigStorage('\t');

qlog_from_dup = FILTER qlog BY $37 MATCHES '.*id=faeaa209-ce26-56db-ef7d-b1af55811563.*';
rmf $QLOG_SAMPLE_OUT
store qlog_from_dup into '$QLOG_SAMPLE_OUT' using PigStorage('\t');
