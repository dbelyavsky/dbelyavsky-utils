import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

%default JIRA 'APPOPS-680'
%default DATE '20180629'
%default DATE_PATH '2018/06/29/13/00'
%default EVENT_LOG_PATTERN '/user/thresher/event/$DATE_PATH/LOG.$DATE*.app[0-9][0-9].*'
%default DT_LOG_PATTERN '/user/thresher/dt/raw/$DATE_PATH/DT.$DATE*.dt[0-9][0-9].*'
%default ASID '69b2638c-b825-494e-aed0-a0d8c8eaf067'

events = LOAD_EVENTS('$EVENT_LOG_PATTERN');

events_filtered_by_asid = FILTER events BY javascriptInfo matches '.* id=$ASID, .*';

rmf $JIRA/event/$DATE_PATH
STORE events_filtered_by_asid INTO '$JIRA/event/$DATE_PATH' USING PigStorage('\t');

-- dt = LOAD_RAWDT('$DT_LOG_PATTERN');

-- dt_filtered_by_asid = FILTER dt by asid == '$ASID';

-- rmf $JIRA/dt/raw/$DATE_PATH
-- STORE dt_filtered_by_asid INTO '$JIRA/dt/raw/$DATE_PATH' USING PigStorage('\t');
