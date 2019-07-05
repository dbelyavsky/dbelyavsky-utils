-- scored = load '/user/etlstage/MCALScorer.scored/_temporary/1/_temporary/attempt_1503520280936_448935_r_000019_0/*' using PigStorage('\t');
-- scored = load '/user/etlstage/MCALScorer.scored/_temporary/1/_temporary/*/*' using PigStorage('\t');
scored = load '/user/etlstage/MCALScorer.scored/part*' using PigStorage('\t');

filtered = filter scored by $0 matches '.*appnexus.*';

rmf ETL-7489.verify.appnexus
store filtered into 'ETL-7489.verify.appnexus' using PigStorage('\t');
