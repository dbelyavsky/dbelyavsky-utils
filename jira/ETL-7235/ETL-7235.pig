import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';


-- qlog = load '/user/etlstage/quality/logs/2018/01/24/*/impressions/imp*' using PigStorage('\t');
qlog = load 'ETL-7235.qlog.20180124' using PigStorage('\t');

-- filtered = filter qlog by $14 in ('138828', '133198', '133327', '132562');
filtered = filter qlog by (
	( $50 matches '.*\\Wsus=1.*' or $95 matches '.*\\Wsivt=1.*')
	and not $95 matches '.*\\Wnht=.*' 
	and not $95 matches '.*\\Wha=1.*' 
	and not $95 matches '.*\\Wds=1.*' );

-- filtered = filter qlog by ($14 == '138858' and $95 matches '.*\\Wsivt=1.*');

rmf ETL-7235.qlog.20180124.sivt
store filtered into 'ETL-7235.qlog.20180124.sivt' using PigStorage('\t');

-- premart = LOAD_QUALITY_PREMART('/user/etlstage/quality/scores/aggdt3/2018/01/24/*/*');
-- filtered = filter premart by lookupId == 138858;

-- recordsForCampaignTrimmed = FOREACH filtered GENERATE
    -- lookupId,
    -- imps as imps,
    -- suspicious as sivt_imps,
    -- (fraud_traffic_imps#'sivt' is null ? 0 : fraud_traffic_imps#'sivt') as sivt_total_imps,
    -- (fraud_traffic_imps#'nht' is null ? 0 : fraud_traffic_imps#'nht') as nht,
    -- (fraud_traffic_imps#'ha' is null ? 0 : fraud_traffic_imps#'ha') as ha,
    -- (fraud_traffic_imps#'ds' is null ? 0 : fraud_traffic_imps#'ds') as ds;

-- rmf  ETL-7235.premart.20180124
-- store recordsForCampaignTrimmed into 'ETL-7235.premart.20180124' using PigStorage('\t');
