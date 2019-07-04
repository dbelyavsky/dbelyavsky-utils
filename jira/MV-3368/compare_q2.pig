IMPORT 'ias_mart/aggregation_helper/pig/macros/load.macro';

aggdt_new = LOAD_QUALITY_PREMART('quality.NEW/scores/aggdt3/2016/11/09/*');
aggdt_new_fw = filter aggdt_new by lookupId > 0;
aggdt_new_group = GROUP aggdt_new_fw BY (lookupId, passbackId);
--aggdt_new_group_mrc = GROUP aggdt_new_fw BY (lookupId, passbackId, mrc_accredited);
--aggdt_new_count = FOREACH aggdt_new_group GENERATE FLATTEN(group), COUNT(aggdt_new_fw), SUM(aggdt_new_fw.imps);
--aggdt_new_count_mrc = FOREACH aggdt_new_group_mrc GENERATE FLATTEN(group), COUNT(aggdt_new_fw), SUM(aggdt_new_fw.imps);
aggdt_new_sums = FOREACH aggdt_new_group GENERATE FLATTEN(group), SUM(aggdt_new_fw.imps), SUM(aggdt_new_fw.iv1imps), SUM(aggdt_new_fw.iv1);
--store aggdt_new_count into 'regression/aggdt_count_fw.NEW' USING PigStorage('\t'); -- 152,448

aggdt_mstr = LOAD_QUALITY_PREMART('quality.MSTR/scores/aggdt3/2016/11/09/*');
aggdt_mstr_fw = filter aggdt_mstr by lookupId > 0;
aggdt_mstr_group = GROUP aggdt_mstr_fw BY (lookupId, passbackId);
--aggdt_mstr_group_mrc = GROUP aggdt_mstr_fw BY (lookupId, passbackId, mrc_accredited);
--aggdt_mstr_count = FOREACH aggdt_mstr_group GENERATE FLATTEN(group), COUNT(aggdt_mstr_fw), SUM(aggdt_mstr_fw.imps);
--aggdt_mstr_count_mrc = FOREACH aggdt_mstr_group_mrc GENERATE FLATTEN(group), COUNT(aggdt_mstr_fw), SUM(aggdt_mstr_fw.imps);
aggdt_mstr_sums = FOREACH aggdt_mstr_group GENERATE FLATTEN(group), SUM(aggdt_mstr_fw.imps), SUM(aggdt_mstr_fw.iv1imps), SUM(aggdt_mstr_fw.iv1);
--store aggdt_mstr_count into 'regression/aggdt_count_fw.MSTR' USING PigStorage('\t'); -- 152,448

--counts_new = join aggdt_new_count by (group::lookupId, group::passbackId), aggdt_mstr_count by (group::lookupId, group::passbackId);
--counts_new_mrc = join aggdt_new_count_mrc by (group::lookupId, group::passbackId, group::mrc_accredited), aggdt_mstr_count_mrc by (group::lookupId, group::passbackId, group::mrc_accredited);
--count_new_diffs = filter counts_new by $2 != $6 OR $3 != $7;
--store count_new_diffs into 'regression/count_new_diffs.1' using PigStorage('\t');

--count_new_diffs_mrc = filter counts_new_mrc by $3 != $8 OR $4 != $9;
-- store count_new_diffs_mrc into 'regression/count_new_diffs_mrc.1' using PigStorage('\t');

aggdt_join = JOIN aggdt_new_sums BY (group::lookupId, group::passbackId), aggdt_mstr_sums BY (group::lookupId, group::passbackId);
aggdt_diffs = FILTER aggdit_join BY $2 != $7 OR $3 != $8 OR $4 != $9
STORE aggdt_diffs INTO 'regression/aggdt_diffs.1' USING PigStorage('\t'); 
