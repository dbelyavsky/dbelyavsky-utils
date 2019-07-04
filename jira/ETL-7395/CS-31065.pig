import '/Users/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

qlog = LOAD_JOINED('/user/thresher/quality/logs/2018/02/{06,07,08,09,10,11,12,13,14,15}/*/impressions/imp*');

%default OUTDIR 'CS-31065.countAllundefinedPerCampaign.20180209'

filtered = FILTER qlog BY (lookupId > 0
    AND serverName MATCHES 'app(06|14)sje'
    AND impressionScores MATCHES '.*\\Wsus=1.*'
    AND scores == 'undefined'
    AND NOT fraudScores MATCHES '.*\\sivt=1.*'
    AND givt != 1);

projected = FOREACH filtered GENERATE SUBSTRING(dateReceived, 0, 10) as hit_date, lookupId;

groupped = GROUP projected BY (lookupId, hit_date);

result = FOREACH groupped  GENERATE FLATTEN(group), COUNT(projected);

rmf $OUTDIR 
STORE result INTO '$OUTDIR' USING PigStorage('\t');
