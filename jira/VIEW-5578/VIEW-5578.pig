import '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

%default OUTDIR 'VIEW-5578/quality'
qlogs = LOAD_JOINED('/user/thresher/quality/logs/2018/08/06/*/impressions/*');

qlogs_sample = FILTER qlogs BY ( 
    extPlacementId == '224965791' 
    and extAdnetworkId == '926282' 
    and extCampaignId =='21386074'
    );
    
rmf $OUTDIR
STORE qlogs_sample INTO '$OUTDIR' USING PigStorage('\t');

%default OUTDIR 'VIEW-5578/aggdt3'
aggdt3 = LOAD_QUALITY_PREMART('/user/thresher/quality/scores/aggdt3/2018/08/06/*/*');

aggdt3_sample = FILTER aggdt3 BY ( 
    extPlacementId == '224965791' 
    and extAdnetworkId == 926282 
    and extCampaignId == '21386074'
    );
    
rmf $OUTDIR
STORE aggdt3_sample INTO '$OUTDIR' USING PigStorage('\t');
