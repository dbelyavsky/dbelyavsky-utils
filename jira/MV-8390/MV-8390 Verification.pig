/**** MV-8390 verification *****/

IMPORT '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

%declare date_path '2017/08/17/12'
%declare aggdt_path 'scores/aggdt3/$date_path';
%declare BRANCH_AGGDT 'quality.MV-8390.branch/$aggdt_path/*';
%declare PROD_AGGDT 'quality.MV-8390.PROD/$aggdt_path/*';
%declare RESULTS_OUT 'verify.MV-8390/$date_path';

premart_branch = LOAD_QUALITY_PREMART('$BRANCH_AGGDT');
premart_prod = LOAD_QUALITY_PREMART('$PROD_AGGDT');

branch_by_mrc = GROUP premart_branch BY mrc_accredited;
prod_by_mrc = GROUP premart_prod BY mrc_accredited;

branch_count = FOREACH branch_by_mrc GENERATE 'BRANCH' as env, FLATTEN(group) as mrc_accredited, COUNT(premart_branch) as count;
prod_count = FOREACH prod_by_mrc GENERATE 'PROD' as env, FLATTEN(group) as mrc_accredited, COUNT(premart_prod) as count;

/***** results ***********
grunt> dump branch_count
(0,2728155)
(1,95310781)
--------------------------
grunt> dump prod_count
(0,2728155)
(1,95310781)
**************************/

results = UNION branch_count, prod_count;

rmf $RESULTS_OUT
store results into '$RESULTS_OUT' USING PigStorage('\t');
