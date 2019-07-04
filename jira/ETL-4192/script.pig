import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

joined_raw = LOAD_JOINED('/user/thresher/quality/logs/2016/06/28/14/impressions/*');
only_host = filter joined_raw by ( serverName == 'app11' );

only_avw = filter only_host by javascriptInfo matches '.*vc=avw.*';
count_avw = foreach (group only_avw all) generate COUNT(only_avw);

only_getAdCompanions = filter only_avw by videoCall matches '.*tp:getAdCompanions.*';
count_getAdCompanions = foreach (group only_getAdCompanions all) generate COUNT(only_getAdCompanions);

dump count_avw;
dump count_getAdCompanions;
