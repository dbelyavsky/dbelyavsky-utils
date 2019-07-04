q = load 'quality/logs/2018/11/20/12/impressions/*' using PigStorage('\t');

q_filtered = filter q by $14 in (191122);

rmf OUTPUT
store q_filtered into 'OUTPUT' using PigStorage('\t');
