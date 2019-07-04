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

aggdt3_branch = LOAD_QUALITY_PREMART('quality.NEW/scores/aggdt3/2016/11/09/*');
aggdt3_mstr = LOAD_QUALITY_PREMART('quality.MSTR/scores/aggdt3/2016/11/09/*');

aggdt3_branch_filtered = filter aggdt3_branch by lookupId == 57876 and passbackId in (8972207, 8972207, 8972208, 8972209, 8972237, 8972238);
aggdt3_mstr_filtered = filter aggdt3_mstr by lookupId == 57876 and passbackId in (8972207, 8972207, 8972208, 8972209, 8972237, 8972238);

-- store aggdt3_branch_filtered into 'aggdt3_branch_filtered' using PigStorage('\t');       -- 2743
-- store aggdt3_mstr_filtered into 'aggdt3_mstr_filtered' using PigStorage('\t');           -- 2743

aggdt3_branch_projected = foreach aggdt3_branch_filtered GENERATE
    media_type_id,
    iv1imps,
    iv2imps,
    ov1imps,
    ov2imps,
    niv,
    fiv1imps,
    fiv2imps,
    nfiv1,
    nfiv2,
    viewability_measurement_trusted_imps,
    mrc_accredited
    ;

aggdt3_mstr_projected = foreach aggdt3_mstr_filtered GENERATE
    media_type_id,
    iv1imps,
    iv2imps,
    ov1imps,
    ov2imps,
    niv,
    fiv1imps,
    fiv2imps,
    nfiv1,
    nfiv2,
    viewability_measurement_trusted_imps,
    mrc_accredited
    ;

joined = aggdt3_branch_projected by (media_type_id), aggdt3_mstr_projected by (media_type_id)
