SELECT
    totalImps,
    fraudulentImps,
    fraudulentPct,
    nonFraudulentImps,
    nonFraudulentPct,
    nonHumanTrafficImps,
    nonHumanTrafficPct,
    nhtSittingDuckBotImps,
    nhtSittingDuckBotPct,
    nhtStandardBotImps,
    nhtStandardBotPct,
    nhtVolunteerBotImps,
    nhtVolunteerBotPct,
    nhtProfileBotImps,
    nhtProfileBotPct,
    nhtMaskedBotImps,
    nhtMaskedBotPct,
    nhtNomadicBotImps,
    nhtNomadicBotPct,
    nhtOtherBotImps,
    nhtOtherBotPct
    ,
        fraudulentUnblockedImps,
        fraudulentUnblockedPct,
        fraudulentBlockedImps,
        fraudulentBlockedPct,
        unblockedImps,
        unblockedPct,
        blockedImps,
        blockedPct,
        givtImps,
        givtPct,
        hiddenAdsImps,
        hiddenAdsPct,
        locationSpoofingImps,
        locationSpoofingPct,
        lsProxyServerImps,
        lsProxyServerPct,
        incentivizedBrowsingImps,
        incentivizedBrowsingPct,
        sivtImps,
        sivtPct,
        rviImps,
        rviPct,
        domainSpoofingImps,
        domainSpoofingPct

    , THIS_PERIOD.campaignId,THIS_PERIOD.campaignName
FROM
(
        SELECT
                    SUM(COALESCE(GROSS_IMPS, IMPS)) AS totalImps,
                    (SUM(SIVT_IMPS) + COALESCE(SUM(GIVT_IMPS), 0)) AS fraudulentImps,
                    ROUND(100 * (SUM(SIVT_IMPS) + COALESCE(SUM(GIVT_IMPS), 0))/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS fraudulentPct,
                    (SUM(COALESCE(GROSS_IMPS, IMPS)) - (SUM(SIVT_IMPS) + COALESCE(SUM(GIVT_IMPS), 0))) AS nonFraudulentImps,
                    ROUND(100 * (SUM(COALESCE(GROSS_IMPS, IMPS)) - (SUM(SIVT_IMPS) + COALESCE(SUM(GIVT_IMPS), 0)))/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS nonFraudulentPct,
                    ((SUM(SIVT_IMPS) + COALESCE(SUM(GIVT_IMPS), 0)) - SUM(COALESCE(BLOCKED_FRAUD_IMPS, 0))) AS fraudulentUnblockedImps,
                    ROUND(100 * ((SUM(SIVT_IMPS) + COALESCE(SUM(GIVT_IMPS), 0)) - SUM(COALESCE(BLOCKED_FRAUD_IMPS, 0)))/(SUM(SIVT_IMPS) + COALESCE(SUM(GIVT_IMPS), 0)), 2) AS fraudulentUnblockedPct,
                    IF(MIN(HIT_DATE) >= '2018-01-27'
                        , SUM(COALESCE(BLOCKED_FRAUD_IMPS, 0))
                	    , ((SUM(SIVT_IMPS) + COALESCE(SUM(GIVT_IMPS), 0)) - ((SUM(SIVT_IMPS) + COALESCE(SUM(GIVT_IMPS), 0)) - SUM(COALESCE(BLOCKED_FRAUD_IMPS, 0))))) AS fraudulentBlockedImps,
                	IF(MIN(HIT_DATE) >= '2018-01-27'
                        , ROUND(100 * SUM(COALESCE(BLOCKED_FRAUD_IMPS, 0))/(SUM(SIVT_IMPS) + COALESCE(SUM(GIVT_IMPS), 0)), 2)
                        , ROUND(100 * ((SUM(SIVT_IMPS) + COALESCE(SUM(GIVT_IMPS), 0)) - ((SUM(SIVT_IMPS) + COALESCE(SUM(GIVT_IMPS), 0)) - SUM(COALESCE(BLOCKED_FRAUD_IMPS, 0))))/(SUM(SIVT_IMPS) + COALESCE(SUM(GIVT_IMPS), 0)), 2)) AS fraudulentBlockedPct,
                    SUM(UNBLOCKED_IMPS) AS unblockedImps,
                    ROUND(100 * SUM(UNBLOCKED_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS unblockedPct,
                    (SUM(COALESCE(GROSS_IMPS, IMPS)) - SUM(UNBLOCKED_IMPS)) AS blockedImps,
                    ROUND(100 * (SUM(COALESCE(GROSS_IMPS, IMPS)) - SUM(UNBLOCKED_IMPS))/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS blockedPct,
                    SUM(GIVT_IMPS) AS givtImps,
                    ROUND(100 * SUM(GIVT_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS givtPct,
                    SUM(NHT_NON_HUMAN_TRAFFIC_TOTAL_IMPS) AS nonHumanTrafficImps,
                    ROUND(100 * SUM(NHT_NON_HUMAN_TRAFFIC_TOTAL_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS nonHumanTrafficPct,
                    SUM(NHT_SITTING_DUCK_BOT_IMPS) AS nhtSittingDuckBotImps,
                    ROUND(100 * SUM(NHT_SITTING_DUCK_BOT_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS nhtSittingDuckBotPct,
                    SUM(NHT_STANDARD_BOT_IMPS) AS nhtStandardBotImps,
                    ROUND(100 * SUM(NHT_STANDARD_BOT_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS nhtStandardBotPct,
                    SUM(NHT_VOLUNTEER_BOT_IMPS) AS nhtVolunteerBotImps,
                    ROUND(100 * SUM(NHT_VOLUNTEER_BOT_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS nhtVolunteerBotPct,
                    SUM(NHT_PROFILE_BOT_IMPS) AS nhtProfileBotImps,
                    ROUND(100 * SUM(NHT_PROFILE_BOT_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS nhtProfileBotPct,
                    SUM(NHT_MASKED_BOT_IMPS) AS nhtMaskedBotImps,
                    ROUND(100 * SUM(NHT_MASKED_BOT_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS nhtMaskedBotPct,
                    SUM(NHT_NOMADIC_BOT_IMPS) AS nhtNomadicBotImps,
                    ROUND(100 * SUM(NHT_NOMADIC_BOT_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS nhtNomadicBotPct,
                    SUM(NHT_OTHER_BOT_IMPS) AS nhtOtherBotImps,
                    ROUND(100 * SUM(NHT_OTHER_BOT_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS nhtOtherBotPct,
                    SUM(HA_HIDDEN_ADS_TOTAL_IMPS) AS hiddenAdsImps,
                    ROUND(100 * SUM(HA_HIDDEN_ADS_TOTAL_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS hiddenAdsPct,
                    SUM(LS_LOCATION_SPOOFING_TOTAL_IMPS) AS locationSpoofingImps,
                    ROUND(100 * SUM(LS_LOCATION_SPOOFING_TOTAL_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS locationSpoofingPct,
                    SUM(LS_PROXY_SERVER_IMPS) AS lsProxyServerImps,
                    ROUND(100 * SUM(LS_PROXY_SERVER_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS lsProxyServerPct,
                    SUM(IB_INCENTIVIZED_BROWSING_TOTAL_IMPS) AS incentivizedBrowsingImps,
                    ROUND(100 * SUM(IB_INCENTIVIZED_BROWSING_TOTAL_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS incentivizedBrowsingPct,
                    SUM(SIVT_IMPS) AS sivtImps,
                    ROUND(100 * SUM(SIVT_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS sivtPct,
                    SUM(RVI_IMPS) AS rviImps,
                    ROUND(100 * SUM(RVI_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS rviPct,
                    SUM(DS_DOMAIN_SPOOFING_TOTAL_IMPS) AS domainSpoofingImps,
                    ROUND(100 * SUM(DS_DOMAIN_SPOOFING_TOTAL_IMPS)/SUM(COALESCE(GROSS_IMPS, IMPS)), 2) AS domainSpoofingPct
            , CAMPAIGN_ID as campaignId, CAMPAIGN_NAME as campaignName
        FROM
            (
            SELECT
                 HIT_DATE,
                GROSS_IMPS,
                IMPS,
                SIVT_IMPS,
                GIVT_IMPS,
                (SIVT_PASSED_IMPS + SIVT_FLAGGED_IMPS + GIVT_IMPS) AS FRAUDULENT_UNBLOCKED_IMPS,
                (PASSED_IMPS + FLAGGED_IMPS + GIVT_IMPS) AS UNBLOCKED_IMPS,
                NHT_NON_HUMAN_TRAFFIC_TOTAL_IMPS,
                NHT_SITTING_DUCK_BOT_IMPS,
                NHT_STANDARD_BOT_IMPS,
                NHT_VOLUNTEER_BOT_IMPS,
                NHT_PROFILE_BOT_IMPS,
                NHT_MASKED_BOT_IMPS,
                NHT_NOMADIC_BOT_IMPS,
                NHT_OTHER_BOT_IMPS,
                HA_HIDDEN_ADS_TOTAL_IMPS,
                LS_LOCATION_SPOOFING_TOTAL_IMPS,
                LS_PROXY_SERVER_IMPS,
                IB_INCENTIVIZED_BROWSING_TOTAL_IMPS,
                RVI_IMPS,
                DS_DOMAIN_SPOOFING_TOTAL_IMPS,
                BLOCKED_FRAUD_IMPS
                , CAMPAIGN_ID, CAMPAIGN_NAME
            FROM
                AGG_AGENCY_FRAUD
            WHERE
                 (HIT_DATE >= '2018-01-28' AND HIT_DATE <= '2018-01-28')
                AND ( CAMPAIGN_ID IN (select distinct campaign_id
from (
	SELECT HIT_DATE
		, ADV.ID AS ADV_ENTITY_ID
		, ADV.campaign_id
		, CAMPAIGN_NAME
		, IF( IF(SUM(MONITORING_IMPS) !=0 AND SUM(MONITORING_IMPS)/SUM(IMPS)*100>95
				, 2
				, IF(SUM(BLOCKING_IMPS)!=0 AND SUM(BLOCKING_IMPS)/SUM(IMPS)*100>95
					, 1
					, 3
				)
			) = 1
			, 'Blocking'
			, IF(
				IF(SUM(MONITORING_IMPS)!=0 AND SUM(MONITORING_IMPS)/SUM(IMPS)*100>95
					, 2
					, IF(SUM(BLOCKING_IMPS) !=0 AND SUM(BLOCKING_IMPS)/SUM(IMPS)*100>95
						, 1
						, 3
					)
				) = 2
				, 'Monitoring'
				, 'Mixed'
			)
		) AS 'Campaign Blocking Status'
		, SUM(IMPS) as 'Total Impressions'
	FROM AGG_AGENCY_ACTION AGG
	LEFT JOIN ADV_ENTITY ADV
		ON ADV.CAMPAIGN_ID = AGG.CAMPAIGN_ID
	WHERE HIT_DATE = '2018-01-28' and ADV.status = 'Active'
	GROUP BY CAMPAIGN_NAME,HIT_DATE,ADV_ENTITY_ID
	HAVING SUM(BLOCKING_IMPS)= 0 and sum(imps) > 100000
	order by SUM(IMPS) desc
) campaigns)  )

            UNION ALL

            SELECT
                 HIT_DATE,
                (IMPS + GENERAL_INVALID_IMPS) AS GROSS_IMPS,
                IMPS,
                SUSPICIOUS_IMPS AS SIVT_IMPS,
                GENERAL_INVALID_IMPS AS GIVT_IMPS,
                (SUSPICIOUS_IMPS + GENERAL_INVALID_IMPS) AS FRAUDULENT_UNBLOCKED_IMPS,
                (IMPS + GENERAL_INVALID_IMPS) AS UNBLOCKED_IMPS,
                SUSPICIOUS_IMPS AS NHT_NON_HUMAN_TRAFFIC_TOTAL_IMPS,
                SITTING_DUCK_BOT_IMPS AS NHT_SITTING_DUCK_BOT_IMPS,
                STANDARD_BOT_IMPS AS NHT_STANDARD_BOT_IMPS,
                VOLUNTEER_BOT_IMPS AS NHT_VOLUNTEER_BOT_IMPS,
                PROFILE_BOT_IMPS AS NHT_PROFILE_BOT_IMPS,
                MASKED_BOT_IMPS AS NHT_MASKED_BOT_IMPS,
                NOMADIC_BOT_IMPS AS NHT_NOMADIC_BOT_IMPS,
                OTHER_BOT_IMPS AS NHT_OTHER_BOT_IMPS,
                NULL AS HA_HIDDEN_ADS_TOTAL_IMPS,
                NULL AS LS_LOCATION_SPOOFING_TOTAL_IMPS,
                NULL AS LS_PROXY_SERVER_IMPS,
                NULL AS IB_INCENTIVIZED_BROWSING_TOTAL_IMPS,
                NULL AS RVI_IMPS,
                NULL AS DS_DOMAIN_SPOOFING_TOTAL_IMPS,
                NULL AS BLOCKED_FRAUD_IMPS
                , CAMPAIGN_PM.CAMPAIGN_ID as CAMPAIGN_ID, CAMPAIGN.NAME as CAMPAIGN_NAME
            FROM
                AGG_PARTNER_MEASURED_VIEWABILITY VIEWABILITY_PM
                    JOIN (SELECT ID, MEASUREMENT_SOURCE_ID, NAME, CAMPAIGN_ID FROM PARTNER_MEASURED_CAMPAIGN where CAMPAIGN_ID > 0 ) CAMPAIGN_PM ON (VIEWABILITY_PM.PARTNER_MEASURED_CAMPAIGN_ID=CAMPAIGN_PM.ID AND
                               VIEWABILITY_PM.MEASUREMENT_SOURCE_ID = CAMPAIGN_PM.MEASUREMENT_SOURCE_ID)
                    JOIN CAMPAIGN ON CAMPAIGN_PM.CAMPAIGN_ID=CAMPAIGN.ID

            WHERE
                 (HIT_DATE >= '2018-01-28' AND HIT_DATE <= '2018-01-28')
                AND ( CAMPAIGN_PM.CAMPAIGN_ID IN (select distinct campaign_id
from (
	SELECT HIT_DATE
		, ADV.ID AS ADV_ENTITY_ID
		, ADV.campaign_id
		, CAMPAIGN_NAME
		, IF( IF(SUM(MONITORING_IMPS) !=0 AND SUM(MONITORING_IMPS)/SUM(IMPS)*100>95
				, 2
				, IF(SUM(BLOCKING_IMPS)!=0 AND SUM(BLOCKING_IMPS)/SUM(IMPS)*100>95
					, 1
					, 3
				)
			) = 1
			, 'Blocking'
			, IF(
				IF(SUM(MONITORING_IMPS)!=0 AND SUM(MONITORING_IMPS)/SUM(IMPS)*100>95
					, 2
					, IF(SUM(BLOCKING_IMPS) !=0 AND SUM(BLOCKING_IMPS)/SUM(IMPS)*100>95
						, 1
						, 3
					)
				) = 2
				, 'Monitoring'
				, 'Mixed'
			)
		) AS 'Campaign Blocking Status'
		, SUM(IMPS) as 'Total Impressions'
	FROM AGG_AGENCY_ACTION AGG
	LEFT JOIN ADV_ENTITY ADV
		ON ADV.CAMPAIGN_ID = AGG.CAMPAIGN_ID
	WHERE HIT_DATE = '2018-01-28' and ADV.status = 'Active'
	GROUP BY CAMPAIGN_NAME,HIT_DATE,ADV_ENTITY_ID
	HAVING SUM(BLOCKING_IMPS)= 0 and sum(imps) > 100000
	order by SUM(IMPS) desc
) campaigns) )

            UNION ALL

            SELECT
                 HIT_DATE,
                (IMPS + GENERAL_INVALID_IMPS - FACEBOOK_INVALID_IMPS) AS GROSS_IMPS,
                (IMPS - FACEBOOK_INVALID_IMPS) AS IMPS,
                SUSPICIOUS_IMPS AS SIVT_IMPS,
                GENERAL_INVALID_IMPS AS GIVT_IMPS,
                (SUSPICIOUS_IMPS + GENERAL_INVALID_IMPS) AS FRAUDULENT_UNBLOCKED_IMPS,
                (IMPS + GENERAL_INVALID_IMPS - FACEBOOK_INVALID_IMPS) AS UNBLOCKED_IMPS,
                SUSPICIOUS_IMPS AS NHT_NON_HUMAN_TRAFFIC_TOTAL_IMPS,
                NULL AS NHT_SITTING_DUCK_BOT_IMPS,
                NULL AS NHT_STANDARD_BOT_IMPS,
                NULL AS NHT_VOLUNTEER_BOT_IMPS,
                NULL AS NHT_PROFILE_BOT_IMPS,
                NULL AS NHT_MASKED_BOT_IMPS,
                NULL AS NHT_NOMADIC_BOT_IMPS,
                NULL AS NHT_OTHER_BOT_IMPS,
                NULL AS HA_HIDDEN_ADS_TOTAL_IMPS,
                NULL AS LS_LOCATION_SPOOFING_TOTAL_IMPS,
                NULL AS LS_PROXY_SERVER_IMPS,
                NULL AS IB_INCENTIVIZED_BROWSING_TOTAL_IMPS,
                NULL AS RVI_IMPS,
                NULL AS DS_DOMAIN_SPOOFING_TOTAL_IMPS,
                NULL AS BLOCKED_FRAUD_IMPS
                , CAMPAIGN_PM.CAMPAIGN_ID as CAMPAIGN_ID, CAMPAIGN.NAME as CAMPAIGN_NAME
            FROM
                AGG_FACEBOOK_VIEWABILITY
                    JOIN PARTNER_MEASURED_CAMPAIGN_MAPPING MAPPING ON (AGG_FACEBOOK_VIEWABILITY.MEASUREMENT_SOURCE_ID = MAPPING.MEASUREMENT_SOURCE_ID AND AGG_FACEBOOK_VIEWABILITY.AD_ID = MAPPING.EXT_MAPPING_ID)
                    JOIN (SELECT ID, MEASUREMENT_SOURCE_ID, NAME, CAMPAIGN_ID, STATUS FROM PARTNER_MEASURED_CAMPAIGN where CAMPAIGN_ID > 0 ) CAMPAIGN_PM ON (MAPPING.EXT_CAMPAIGN_ID=CAMPAIGN_PM.ID AND
                               AGG_FACEBOOK_VIEWABILITY.MEASUREMENT_SOURCE_ID = CAMPAIGN_PM.MEASUREMENT_SOURCE_ID)
                    JOIN CAMPAIGN ON CAMPAIGN_PM.CAMPAIGN_ID=CAMPAIGN.ID

            WHERE
                 (HIT_DATE >= '2018-01-28' AND HIT_DATE <= '2018-01-28')
                AND ( CAMPAIGN_PM.CAMPAIGN_ID IN (select distinct campaign_id
from (
	SELECT HIT_DATE
		, ADV.ID AS ADV_ENTITY_ID
		, ADV.campaign_id
		, CAMPAIGN_NAME
		, IF( IF(SUM(MONITORING_IMPS) !=0 AND SUM(MONITORING_IMPS)/SUM(IMPS)*100>95
				, 2
				, IF(SUM(BLOCKING_IMPS)!=0 AND SUM(BLOCKING_IMPS)/SUM(IMPS)*100>95
					, 1
					, 3
				)
			) = 1
			, 'Blocking'
			, IF(
				IF(SUM(MONITORING_IMPS)!=0 AND SUM(MONITORING_IMPS)/SUM(IMPS)*100>95
					, 2
					, IF(SUM(BLOCKING_IMPS) !=0 AND SUM(BLOCKING_IMPS)/SUM(IMPS)*100>95
						, 1
						, 3
					)
				) = 2
				, 'Monitoring'
				, 'Mixed'
			)
		) AS 'Campaign Blocking Status'
		, SUM(IMPS) as 'Total Impressions'
	FROM AGG_AGENCY_ACTION AGG
	LEFT JOIN ADV_ENTITY ADV
		ON ADV.CAMPAIGN_ID = AGG.CAMPAIGN_ID
	WHERE HIT_DATE = '2018-01-28' and ADV.status = 'Active'
	GROUP BY CAMPAIGN_NAME,HIT_DATE,ADV_ENTITY_ID
	HAVING SUM(BLOCKING_IMPS)= 0 and sum(imps) > 100000
	order by SUM(IMPS) desc
) campaigns))
            ) UNION_WITH_PM_DATA
        GROUP BY CAMPAIGN_ID
     HAVING totalImps >= 0
) THIS_PERIOD
where fraudulentBlockedImps > 0
ORDER BY totalImps DESC
