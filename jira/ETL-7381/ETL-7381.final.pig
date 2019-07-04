import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

%default OUTDIR 'ETL-7381.final'
qlog = LOAD_JOINED('/user/thresher/quality/logs/2018/02/{16,17,18}/*/impressions/imp*');

filtered = FILTER qlog BY ( lookupId == 131745
    AND impressionScores MATCHES '.*\\Wsus=1.0\\W.*' 
    AND NOT fraudScores MATCHES '.*\\Wsivt=1.*');

rmf $OUTDIR 
STORE filtered INTO '$OUTDIR' USING PigStorage('\t');

