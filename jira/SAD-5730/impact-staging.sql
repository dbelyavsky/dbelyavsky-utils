create schema dbelyavsky_analytics;

create table dbelyavsky_analytics.AGG_AGENCY_CUSTOM_REPORT_PROD as
select * from analytics.AGG_AGENCY_CUSTOM_REPORT where 1 = 2
;

create table dbelyavsky_analytics.AGG_AGENCY_CUSTOM_REPORT_SAD5730 as
select * from analytics.AGG_AGENCY_CUSTOM_REPORT where 1 = 2
;

set @bh_dataformat='mysql';
set AUTOCOMMIT=0;
load data
    local infile '/Users/dbelyavsky/Downloads/SAD-5730/data.BRANCH.tsv'
        into table dbelyavsky_analytics.AGG_AGENCY_CUSTOM_REPORT_SAD5730;
;
commit;
load data
    local infile '/Users/dbelyavsky/Downloads/SAD-5730/data.PROD.tsv'
        into table dbelyavsky_analytics.AGG_AGENCY_CUSTOM_REPORT_PROD;
;
commit;

select count(*) from dbelyavsky_analytics.AGG_AGENCY_CUSTOM_REPORT_PROD
;
select count(*) from dbelyavsky_analytics.AGG_AGENCY_CUSTOM_REPORT_SAD5730
;

select p.CAMPAIGN_ID
     , sum(p.IMPS + p.GIVT_IMPS) as total_imps_prod
     , sum(b.IMPS + b.GIVT_IMPS) as total_imps_branch
     , sum(p.IMPS) as imps_prod
     , sum(b.IMPS) as imps_branch

     , sum(p.GIVT_IMPS) as givt_imps_prod
     , sum(b.GIVT_IMPS)as givt_imps_branch
     , ( sum(b.GIVT_IMPS - p.GIVT_IMPS) / sum(p.IMPS + p.GIVT_IMPS) ) * 100 as givt_pct_change_of_total

     , sum(p.SIVT_IMPS) as sivt_imps_prod
     , sum(b.SIVT_IMPS) as sivt_imps_branch
     , ( sum(b.SIVT_IMPS - p.SIVT_IMPS) / sum(p.IMPS + p.GIVT_IMPS) ) * 100 sivt_pct_change_of_total


     , sum(p.GROSS_IMPS) as GROSS_IMPS_prod
     , sum(b.GROSS_IMPS) as GROSS_IMPS_branch
     , ( sum(b.GROSS_IMPS - p.GROSS_IMPS) / sum(p.IMPS + p.GIVT_IMPS) ) * 100 as gross_imps_pct_change

     , sum(p.IN_VIEW_PASSED_IMPS) as IN_VIEW_PASSED_IMPS_prod
     , sum(b.IN_VIEW_PASSED_IMPS) as IN_VIEW_PASSED_IMPS_branch
     , ( sum(b.IN_VIEW_PASSED_IMPS - p.IN_VIEW_PASSED_IMPS) / sum(p.IMPS + p.GIVT_IMPS) ) * 100 as in_view_passed_imps_pct_change

     , sum(p.FAILED_IMPS) as FAILED_IMPS_prod
     , sum(b.FAILED_IMPS) as FAILED_IMPS_branch
     , ( sum(b.FAILED_IMPS - p.FAILED_IMPS) / sum(p.IMPS + p.GIVT_IMPS) ) * 100 as failed_imps_pct_change

     , sum(p.NOT_IN_VIEW_PASSED_IMPS) as NOT_IN_VIEW_PASSED_IMPS_prod
     , sum(b.NOT_IN_VIEW_PASSED_IMPS) as NOT_IN_VIEW_PASSED_IMPS_branch
     , ( sum(b.NOT_IN_VIEW_PASSED_IMPS - p.NOT_IN_VIEW_PASSED_IMPS) / sum(p.IMPS + p.GIVT_IMPS) ) * 100 as not_in_view_passed_imps_pct_change

     , sum(p.PASSED_IMPS) as PASSED_IMPS_prod
     , sum(b.PASSED_IMPS) as PASSED_IMPS_branch
     , ( sum(b.PASSED_IMPS - p.PASSED_IMPS) / sum(p.IMPS + p.GIVT_IMPS) ) * 100 as passed_imps_pct_change

     , sum(p.SUSPICIOUS_PASSED_IMPS) as SUSPICIOUS_PASSED_IMPS_prod
     , sum(b.SUSPICIOUS_PASSED_IMPS) as SUSPICIOUS_PASSED_IMPS_branch
     , ( sum(b.SUSPICIOUS_PASSED_IMPS - p.SUSPICIOUS_PASSED_IMPS) / sum(p.IMPS + p.GIVT_IMPS) ) * 100 as suspicious_passed_imps_pct_change

     , sum(p.BLOCKED_IMPS) as BLOCKED_IMPS_prod
     , sum(b.BLOCKED_IMPS) as BLOCKED_IMPS_branch
     , ( sum(b.BLOCKED_IMPS - p.BLOCKED_IMPS) / sum(p.IMPS + p.GIVT_IMPS) ) * 100 as blocked_imps_pct_change

     , sum(p.GIVT_UNBLOCKED_IMPS) as GIVT_UNBLOCKED_IMPS_prod
     , sum(b.GIVT_UNBLOCKED_IMPS) as GIVT_UNBLOCKED_IMPS_branch
     , ( sum(b.GIVT_UNBLOCKED_IMPS - p.GIVT_UNBLOCKED_IMPS) / sum(p.IMPS + p.GIVT_IMPS) ) * 100 as givt_unblocked_imps_pct_change

     , sum(p.VIEWABILITY_MEASUREMENT_TRUSTED_UNBLOCKED_IMPS) as VIEWABILITY_MEASUREMENT_TRUSTED_UNBLOCKED_IMPS_prod
     , sum(b.VIEWABILITY_MEASUREMENT_TRUSTED_UNBLOCKED_IMPS) as VIEWABILITY_MEASUREMENT_TRUSTED_UNBLOCKED_IMPS_branch
     , ( sum(b.VIEWABILITY_MEASUREMENT_TRUSTED_UNBLOCKED_IMPS - p.VIEWABILITY_MEASUREMENT_TRUSTED_UNBLOCKED_IMPS) / sum(p.IMPS + p.GIVT_IMPS) ) * 100 as viewability_measurement_trusted_unblocked_imps_pct_change

     , sum(p.FAILED_FRAUD_IMPS) as FAILED_FRAUD_IMPS_prod
     , sum(b.FAILED_FRAUD_IMPS) as FAILED_FRAUD_IMPS_branch
     , ( sum(b.FAILED_FRAUD_IMPS - p.FAILED_FRAUD_IMPS) / sum(p.IMPS + p.GIVT_IMPS) ) * 100 as failed_fraud_imps_pct_change

     , sum(p.BLOCKED_IMPS) as BLOCKING_IMPS_prod
     , sum(b.BLOCKED_IMPS) as BLOCKING_IMPS_branch
     , ( sum(b.BLOCKED_IMPS - p.BLOCKED_IMPS) / sum(p.IMPS + p.GIVT_IMPS) ) * 100 as blocking_imps_pct_change

from dbelyavsky_analytics.AGG_AGENCY_CUSTOM_REPORT_PROD as p
         join dbelyavsky_analytics.AGG_AGENCY_CUSTOM_REPORT_SAD5730 as b on p.CAMPAIGN_ID = b.CAMPAIGN_ID
group by CAMPAIGN_ID
order by total_imps_prod desc

;
