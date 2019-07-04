-- want to see what is the most common size of a key in aggdt3

data = LOAD 'quality.SAD-3258.PROD/scores/aggdt3/2017/07/04/12/quality_aggdt.*' USING PigStorage('\t');
data_key_sizes = FOREACH data GENERATE SIZE(TOTUPLE($0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32)) AS key_size;
group_by_size = GROUP data_key_sizes BY key_size;
count_occurences = FOREACH group_by_size GENERATE FLATTEN(group) AS key_size, COUNT(data_key_sizes) AS count;
occurences_ordered = ORDER count_occurences BY count;
rmf aggdt3_key_sizes
STORE occurences_ordered INTO 'aggdt3_key_sizes' USING PigStorage('\t');

--
data_keys = FOREACH data GENERATE $0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32;
rmf aggdt3_keys
STORE data_keys INTO 'aggdt3_keys' USING PigStorage('\t');
--

-- sort -n aggdt3_key_sizes.txt | uniq -c | sort -n -k 1 > aggdt3_key_sizes.sorted.txt
