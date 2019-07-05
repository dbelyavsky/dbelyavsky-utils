-- scores = load '/user/thresher/scoring/*/exploded/*' using PigStorage('\t');
scores = load 'ETL-7489.out/*' using PigStorage('\t');

filtered = filter scores by $3 matches 'com.dart([.].*|$)';

store filtered into 'ETL-7489.out2' using PigStorage('\t');
