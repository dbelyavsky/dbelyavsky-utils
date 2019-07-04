import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

qlog = LOAD_JOINED('quality/logs/2018/06/05/*/impressions/*');

qlog_filtered = FILTER qlog BY (
    lookupId IN ( 135895, 137596, 138295, 156875, 158137, 169024 )
    AND scores MATCHES '.*\\Wrsa=0.*' 
    AND fraudScores MATCHES '.*\\Wha=1.*');

counts = FOREACH (GROUP qlog_filtered BY lookupId) GENERATE FLATTEN(group), COUNT($1);

STORE counts INTO 'MD-337.final_counts' USING PigStorage('\t');
