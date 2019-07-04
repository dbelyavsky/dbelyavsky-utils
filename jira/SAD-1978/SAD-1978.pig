import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

event = LOAD_EVENTS('/user/thresher/event/20161227*/LOG.20161227*.app51dal.*');

event_filtered_by_asid = FILTER event BY javascriptInfo matches '.* id=005800b5-d3d9-ea47-7688-9b7482ee9659, .*';

STORE event_filtered_by_asid INTO 'event_sample.20161227.app51dal' USING PigStorage('\t');

dt = LOAD_RAWDT('/user/thresher/dt/raw/20161227*/DT*dal*');

dt_filtered_by_asid = FILTER dt by asid == '005800b5-d3d9-ea47-7688-9b7482ee9659';

STORE dt_filtered_by_asid INTO 'dt_sample.2016127' USING PigStorage('\t');
