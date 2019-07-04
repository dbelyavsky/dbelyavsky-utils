import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

%default OUTDIR 'ETL-7399'
qlogs = LOAD_JOINED('quality/logs/2018/02/09/*/impressions/*');

count_qlogs = FOREACH ( GROUP qlogs ALL ) GENERATE COUNT(qlogs);

suspicious = FILTER qlogs BY ( lookupId > 0 
    AND impressionScores MATCHES '.*\\Wsus=1.*' );

count_suspicious = FOREACH ( GROUP suspicious ALL ) GENERATE COUNT(suspicious);

bad_ones = FILTER qlogs BY ( lookupId > 0 
    AND impressionScores MATCHES '.*\\Wsus=1.*' 
    AND NOT fraudScores MATCHES '.*\\Wsivt=1.*');

count_bad = FOREACH ( GROUP bad_ones ALL ) GENERATE COUNT(bad_ones);

all_counts = UNION count_qlogs, count_suspicious, count_bad;

dump all_counts

-- rmf $OUTDIR
-- STORE count_per_date INTO '$OUTDIR' USING PigStorage('\t');

