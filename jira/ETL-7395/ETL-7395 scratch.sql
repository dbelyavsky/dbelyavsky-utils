drop table dbelyavsky_sje_fraud_feb_2018;

create table dbelyavsky_sje_fraud_feb_2018 (
	adv_entity_id bigint(11) not null
	, hit_date date not null
	, fraud_count bigint(11) not null
) ENGINE=BRIGHTHOUSE DEFAULT CHARSET=latin1 COLLATE=latin1_bin
;

truncate table dbelyavsky_sje_fraud_feb_2018;

LOAD DATA LOCAL INFILE '/Users/dbelyavsky/Downloads/data.tsv'
INTO TABLE test.dbelyavsky_sje_fraud_feb_2018
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
;

select a.team_id
	, a.campaign_id
	, d.hit_date
	, sum(f.imps) as 'Total IMPS'
	, sum(f.sivt_imps) as 'Total SIVT'
	, d.fraud_count as 'SJE Fraud Count'
	, 100 * d.fraud_count / sum(f.sivt_imps) as 'SJE Fraud % of SIVT'
	, sum(f.sivt_imps) - d.fraud_count 'Total SIVT without SJEs'
	, 100 * sum(f.sivt_imps) / sum(f.imps) 'Total SIVT %'
	, 100 * (sum(f.sivt_imps) - d.fraud_count) / sum(f.imps) 'Total SIVT % without SJEs'
from dbelyavsky_sje_fraud_feb_2018 d
	left join analytics.ADV_ENTITY a on a.id = d.adv_entity_id
	left join analytics.AGG_AGENCY_FRAUD f on f.campaign_id = a.campaign_id and f.hit_date = d.hit_date
where a.id = d.adv_entity_id and f.campaign_id = a.campaign_id
group by campaign_id, hit_date
having sum(f.imps) >= 10000
order by team_id, campaign_id, hit_date
;
