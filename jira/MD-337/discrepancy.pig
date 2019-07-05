import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

records = LOAD_JOINED('/user/thresher/3ms/20180611/quality_extracted/140329/*');

-- pub_entity = load '/user/thresher/mart/agency/2018/06/11/20180612000000/lookup.pub_entity' using PigStorage('\t');
-- pe_m = filter pub_entity by $4 == '1';

video_sus_in_view_qlogs = FILTER records BY (NOT(givt == 1) AND (action == 'passed' OR action == 'preview' or passbackId > 0) AND javascriptInfo matches '.*jsvid.*' AND impressionScores matches '.*\\Wiv2=1.*' AND impressionScores matches '.*\\Wsus=1.*' AND impressionScoreWeights matches '.*\\Wsus=1.*' AND impressionScoreWeights matches '.*\\Wsmp_raw=1.*' AND impressionScoreWeights matches '.*\\Wvtrust=1.*');

groupv = GROUP video_sus_in_view_qlogs ALL;
countv = FOREACH groupv generate COUNT($1.lookupId);

display_sus_in_view_qlogs = FILTER records BY (NOT(givt == 1) AND (action == 'passed' OR action == 'preview' or passbackId > 0) AND (NOT javascriptInfo matches '.*jsvid.*') AND impressionScores matches '.*\\Wiv1=1.*' AND impressionScores matches '.*\\Wsus=1.*' AND impressionScoreWeights matches '.*\\Wsus=1.*' AND impressionScoreWeights matches '.*\\Wsmp_raw=1.*' AND impressionScoreWeights matches '.*\\Wvtrust=1.*');

groupd = GROUP display_sus_in_view_qlogs ALL;
countd = FOREACH groupd generate COUNT($1.lookupId);

both = UNION video_sus_in_view_qlogs, display_sus_in_view_qlogs; 

groupall = GROUP both ALL;

-- cm_records = FILTER records BY ( passbackId > 0 AND impressionScores matches '.*\\Wiv1=1.*' );

-- groupall = GROUP cm_records ALL;

countall = FOREACH groupall generate COUNT($1.lookupId);

dump countall;
dump countv;
dump countd;

-- STORE counts INTO 'MD-337.discrepant.20180611' USING PigStorage('\t');
