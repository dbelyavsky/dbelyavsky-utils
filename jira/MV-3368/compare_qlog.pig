-- join on asid
-- look for inView scores which differ.

IMPORT 'ias_mart/aggregation_helper/pig/macros/load.macro';
/* found the campaign wich differs
	select
		MSTR.agency_id,
		MSTR.campaign_id,
		MSTR.publisher_id,
		MSTR.placement_id,
		MSTR.mrc_accredited,
	    ROUND(SUM(IF(MSTR.Q_DATA_IMPS > MSTR.IMPS, ((MSTR.IN_VIEW_PASSED_IMPS+MSTR.IN_VIEW_FLAGGED_IMPS)/(MSTR.Q_DATA_IMPS/MSTR.IMPS))
	    	, MSTR.IN_VIEW_PASSED_IMPS+MSTR.IN_VIEW_FLAGGED_IMPS))) as inViewImps1,
	    ROUND(SUM(IF(MSTR.Q_DATA_IMPS > MSTR.IMPS, ((MSTR.NOT_IN_VIEW_PASSED_IMPS+MSTR.NOT_IN_VIEW_FLAGGED_IMPS)/(MSTR.Q_DATA_IMPS/MSTR.IMPS))
	    	, MSTR.NOT_IN_VIEW_PASSED_IMPS+MSTR.NOT_IN_VIEW_FLAGGED_IMPS))) as outOfViewImps1,
	    SUM(MSTR.SUSPICIOUS_PASSED_IMPS+MSTR.SUSPICIOUS_FLAGGED_IMPS) as suspiciousImps1,
	    ROUND(SUM(IF(NEW.Q_DATA_IMPS > NEW.IMPS, ((NEW.IN_VIEW_PASSED_IMPS+NEW.IN_VIEW_FLAGGED_IMPS)/(NEW.Q_DATA_IMPS/NEW.IMPS))
	    	, NEW.IN_VIEW_PASSED_IMPS+NEW.IN_VIEW_FLAGGED_IMPS))) as inViewImps2,
	    ROUND(SUM(IF(NEW.Q_DATA_IMPS > NEW.IMPS, ((NEW.NOT_IN_VIEW_PASSED_IMPS+NEW.NOT_IN_VIEW_FLAGGED_IMPS)/(NEW.Q_DATA_IMPS/NEW.IMPS))
	    	, NEW.NOT_IN_VIEW_PASSED_IMPS+NEW.NOT_IN_VIEW_FLAGGED_IMPS))) as outOfViewImps2,
	    SUM(NEW.SUSPICIOUS_PASSED_IMPS+NEW.SUSPICIOUS_FLAGGED_IMPS) as suspiciousImps2
	from dbelyavsky_NEW_agg_agency_quality_V3 NEW
		LEFT JOIN dbelyavsky_MSTR_agg_agency_quality_V3 MSTR
			ON (MSTR.agency_id = NEW.agency_id
				AND MSTR.campaign_id = NEW.campaign_id
				AND MSTR.publisher_id = NEW.publisher_id
				AND MSTR.placement_id = NEW.placement_id
				AND MSTR.mrc_accredited = NEW.mrc_accredited)
	GROUP BY MSTR.agency_id, MSTR.campaign_id, MSTR.publisher_id, MSTR.placement_id, MSTR.mrc_accredited
	HAVING (inViewImps1 + outOfViewImps1 + suspiciousImps1) != (inViewImps2 + outOfViewImps2 + suspiciousImps2)

-- lookupId = 57876 derived from:
-- 		select * from ADV_ENTITY where agency_id = 582 and campaign_id = 48320
-- passbackId in (8972207, 8972207, 8972208, 8972209, 8972237, 8972238) derived from:
-- 		select id from PUB_ENTITY where publisher_id = 6742 and placement_id = 2440249
*/
qlog_new = LOAD_JOINED('quality.NEW/logs/2016/11/09/*/impressions/*');
branch = filter qlog_new by lookupId == 57876 AND passbackId in (8972207, 8972207, 8972208, 8972209, 8972237, 8972238);

qlog_mstr = LOAD_JOINED('quality.MSTR/logs/2016/11/09/*/impressions/*');
mstr = filter qlog_mstr by lookupId == 57876 AND passbackId in (8972207, 8972207, 8972208, 8972209, 8972237, 8972238);

new_group_all = group branch all;
mstr_group_all = group mstr all;

new_count_all = foreach new_group_all generate COUNT(branch);		-- 13720
mstr_count_all = foreach mstr_group_all generate COUNT(mstr);		-- 13720

-- mstr = LOAD_JOINED('qlog_sample_mstr/*');
-- branch = LOAD_JOINED('qlog_sample_new/*');

mstr_reduced = foreach mstr generate
	REGEX_EXTRACT(javascriptInfo, ' id=[^,]*', 0) as asid,
	REGEX_EXTRACT(scores, ' iv1=[^,]*', 0) as s_iv1,
	REGEX_EXTRACT(impressionScores, ' iv1=[^,]*', 0) as is_iv1,
	REGEX_EXTRACT(impressionScores, ' fiv1=[^,]*', 0) as is_fiv1,
	REGEX_EXTRACT(impressionScores, ' fiv2=[^,]*', 0) as is_fiv2,
	javascriptInfo as jsInfo,
	scores as scores,
	impressionScores as iScores;

branch_reduced = foreach branch generate
		REGEX_EXTRACT(javascriptInfo, ' id=[^,]*', 0) as asid,
		REGEX_EXTRACT(scores, ' iv1=[^,]*', 0) as s_iv1,
		REGEX_EXTRACT(impressionScores, ' iv1=[^,]*', 0) as is_iv1,
		REGEX_EXTRACT(impressionScores, ' fiv1=[^,]*', 0) as is_fiv1,
		REGEX_EXTRACT(impressionScores, ' fiv2=[^,]*', 0) as is_fiv2,
		javascriptInfo as jsInfo,
		scores as scores,
		impressionScores as iScores;

joined = join mstr_reduced by asid , branch_reduced by asid;
diffs = filter joined by
	mstr_reduced::s_iv1 != branch_reduced::s_iv1
	OR mstr_reduced::is_iv1 != branch_reduced::is_iv1
	OR mstr_reduced::is_fiv1 != branch_reduced::is_fiv1
	OR mstr_reduced::is_fiv2 != branch_reduced::is_fiv2;

----- FOUND NO DIFFS
