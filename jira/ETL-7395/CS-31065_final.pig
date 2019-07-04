import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

%default OUTDIR 'CS-31065.final'

adv_entity = LOAD_ADV_ENTITY('/user/thresher/mart/agency/2018/02/14/20180215000000/adv_join/*');

qlog = LOAD 'CS-31065.countAllundefinedPerCampaign' using PigStorage('\t')
    as (campaign_id:int, hit_date:chararray, diffCount:int);

joined = JOIN qlog BY campaign_id, adv_entity BY campaign_id;

projected = FOREACH joined GENERATE 
    adv_entity::team_id as team_id
    , qlog::campaign_id as campaign_id
    , qlog::diffCount as diffCount;

groupped = GROUP projected BY (team_id, campaign_id);

result = FOREACH groupped  GENERATE FLATTEN(group), SUM(projected.diffCount);

rmf $OUTDIR 
STORE result INTO '$OUTDIR' USING PigStorage('\t');
