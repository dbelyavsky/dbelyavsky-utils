prodData = LOAD 'quality.SAD-2762.PROD1/logs/2017/04/*/*/impressions/*' USING PigStorage('\t');
branchData = LOAD 'quality.SAD-2762.BRANCH2/logs/2017/04/*/*/impressions/*' USING PigStorage('\t');

prodHASUS = FOREACH prodData GENERATE REGEX_EXTRACT($50, '(hasus=[0-9]+)', 1) as hasusScore, $50 as impressionScores, $86 as dtMinimizer;
branchHASUS = FOREACH branchData GENERATE REGEX_EXTRACT($50, '(hasus=[0-9]+)', 1) as hasusScore,  $50 as impressionScores, $86 as dtMinimizer;

prodHASUSGroupped = GROUP prodHASUS BY hasusScore;
branchHASUSGroupped = GROUP branchHASUS BY hasusScore;

prodHASUSCount = FOREACH prodHASUSGroupped GENERATE group, COUNT(prodHASUS) as count;
branchHASUSCount = FOREACH branchHASUSGroupped GENERATE group, COUNT(branchHASUS) as count;

together = UNION prodHASUSCount, branchHASUSCount;

dump together;

/************************ RESULTS ********************************************
(hasus=0,239246068)
(hasus=1,42683)
(hasus=0,239245700)
(hasus=1,43051)

43051 - 42683 = 368
239245700 - 239246068 = -368
******************************************************************************/
