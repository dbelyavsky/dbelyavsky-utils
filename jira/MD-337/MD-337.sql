
select campaign_id, sum(ha_hidden_ads_total_imps), sum(imps), sum(sivt_imps)
from AGG_AGENCY_FRAUD
where CAMPAIGN_ID = 128829
group by campaign_id;

select campaign_id, sum(ha_hidden_ads_total_imps), sum(imps), sum(sivt_imps)
from dev_etlstage_analytics_20180605.AGG_AGENCY_FRAUD
where CAMPAIGN_ID = 128829
group by campaign_id;

select SUSPICIOUS_IMPS from dev_dbelyavsky_analytics_20180605.AGG_AGENCY_QUALITY_V3
where suspicious_imps > 0 and campaign_id = 150544
limit 10;

select SUSPICIOUS_IMPS from dev_etlstage_analytics_20180605.AGG_AGENCY_QUALITY_V3
where suspicious_imps > 0 and campaign_id = 150544
limit 10;

select MY_DATA.campaign_id
	, MY_DATA.sus as msus
	, REF_DATA.sus as rsus
	, FRAUD.ha as ha
	, MY_DATA.sus - REF_DATA.sus as diff
from
	(
		select campaign_id, sum(SUSPICIOUS_IMPS) as sus
		from dev_dbelyavsky_analytics_20180605.AGG_AGENCY_QUALITY_V3
		group by CAMPAIGN_ID
	) as MY_DATA,
	(
		select campaign_id, sum(SUSPICIOUS_IMPS) as sus
		from dev_etlstage_analytics_20180605.AGG_AGENCY_QUALITY_V3
		group by CAMPAIGN_ID
	) as REF_DATA,
	(
		select campaign_id, sum(ha_hidden_ads_total_imps) as ha
		from dev_dbelyavsky_analytics_20180605.AGG_AGENCY_FRAUD
		group by campaign_id
	) as FRAUD
where MY_DATA.campaign_id = REF_DATA.CAMPAIGN_ID
	and MY_DATA.sus != REF_DATA.sus
	and MY_DATA.campaign_id = FRAUD.campaign_id
	and FRAUD.ha != (MY_DATA.sus - REF_DATA.sus)

;

select campaign_id, id
from ADV_ENTITY where campaign_id  in (128829,148666,128130,147404,126427,159557)
;


select MY_DATA.campaign_id, MY_DATA.pctSus - REF_DATA.pctSus diff
from
	(select campaign_id
		, sum(imps) imps
		, sum(suspicious_imps) sus
		, (sum(suspicious_imps) / sum(imps)) * 100 pctSus
	from dev_dbelyavsky_analytics_20180605.AGG_AGENCY_QUALITY_V3
	group by campaign_id
	having sum(imps) > 10000
	order by sum(imps) desc
	) MY_DATA join
	(select campaign_id
		, sum(imps) imps
		, sum(suspicious_imps) sus
		, (sum(suspicious_imps) / sum(imps)) * 100 pctSus
	from dev_etlstage_analytics_20180605.AGG_AGENCY_QUALITY_V3
	group by campaign_id
	having sum(imps) > 10000
	order by sum(imps) desc
	) REF_DATA on MY_DATA.campaign_id = REF_DATA.campaign_id
where REF_DATA.pctSus != MY_DATA.pctSus
