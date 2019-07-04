%declare RESULTS_OUT 'verify.ETL-5520';

qlog_prod = load 'quality.PROD-139/logs/2017/05/*/*/impressions/impressions*' using PigStorage('\t');
qlog_branch = load 'quality.ETL-5520.2/logs/2017/05/*/*/impressions/impressions*' using PigStorage('\t');

prod_asid = foreach qlog_prod generate
    REGEX_EXTRACT($37, '\\Wid=([0-9a-zA-Z-]+)', 1) AS asId, $37 AS jsInfo,
    $52 AS auxInfo, $71 AS asIdMap, $86 as dtMinimizer, $50 as impressionScores, $41 as eventCount;

branch_asid = foreach qlog_branch generate
    REGEX_EXTRACT($37, '\\Wid=([0-9a-zA-Z-]+)', 1) AS asId, $37 AS jsInfo,
    $52 AS auxInfo, $71 AS asIdMap, $86 as dtMinimizer, $50 as impressionScores, $41 as eventCount;

joined = JOIN prod_asid BY asId, branch_asid BY asId;

joined_filtered = FILTER joined BY branch_asid::eventCount > 1;

rmf $RESULTS_OUT
store joined_filtered into '$RESULTS_OUT' using PigStorage('\t');

