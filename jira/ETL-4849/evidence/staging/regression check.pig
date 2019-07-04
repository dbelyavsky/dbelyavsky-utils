IMPORT 'ias_mart/aggregation_helper/pig/macros/load.macro';

qualityScoresAggdt3 = LOAD_QUALITY_PREMART('/user/thresher/quality/scores/aggdt3/2016/11/08/23/*');
-- qualityScoresAggdt3 = LOAD_QUALITY_PREMART('/user/etldev/quality/scores/aggdt3/2016/11/08/23/*');

qualityScoresAggdt3ByMRCAccredited = group qualityScoresAggdt3 by (mrc_accredited);

counts = FOREACH qualityScoresAggdt3ByMRCAccredited GENERATE group, COUNT(qualityScoresAggdt3) as count;

dump counts;

-- Results in ETLDEV for 2016/10/21/23
--  (0,358909)
--  (1,52227612)
-- Results in ETLDEV for 2016/10/21/01
--  (0,342838)
--  (1,43865182)
-- Results in ETLDEV for 2016/11/08 (with changes)
-- (0,1249628)
-- (1,53169139)

-- Results in THRESHER for 2016/11/08
-- (0,1249631)
-- (1,53168379)
