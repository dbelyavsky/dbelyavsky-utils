SELECT campaignId, campaignName,publisherId, publisherName,channelId, channelName,placementId, placementName,hitDate, startDate, endDate, hitMonth, hitYear,
    FORMAT(100*VVIC.inView1qPct*VVIC.scaleFactor, 2) as inView1qPct,
    FORMAT(100*VVIC.completed1qPct*VVIC.scaleFactor, 2) as completed1qPct,
    FORMAT(100*VVIC.inView2qPct*VVIC.scaleFactor, 2) as inView2qPct,
    FORMAT(100*VVIC.completed2qPct*VVIC.scaleFactor, 2) as completed2qPct,
    FORMAT(100*VVIC.inView3qPct*VVIC.scaleFactor, 2) as inView3qPct,
    FORMAT(100*VVIC.completed3qPct*VVIC.scaleFactor, 2) as completed3qPct,
    FORMAT(100*VVIC.inView4qPct*VVIC.scaleFactor, 2) as inView4qPct,
    FORMAT(100*VVIC.completed4qPct*VVIC.scaleFactor, 2) as completed4qPct,
    FORMAT(100*VVIC.inView4qPct100*VVIC.scaleFactor, 2) as inView4qPct100,
    FORMAT(100*VVIC.inView2qPct100*VVIC.scaleFactor, 2) as inView2qPct100,
    FORMAT(100*VVIC.neverStartedPct*VVIC.scaleFactor, 2) as neverStartedPct,
    FORMAT(100*VVIC.inViewPct*VVIC.scaleFactor, 2) as inViewPct,
    FORMAT(100*VVIC.outOfViewPct*VVIC.scaleFactor, 2) as outOfViewPct,
    FORMAT(100*VVIC.inViewPct100*VVIC.scaleFactor, 2) as inViewPct100,
    FORMAT(100*VVIC.mutedPct*VVIC.scaleFactor, 2) as mutedPct,
    FORMAT(100*VVIC.fullScreenPct*VVIC.scaleFactor, 2) as fullScreenPct,
    FORMAT(100*VVIC.clickThroughRate*VVIC.scaleFactor, 2) as clickThroughRate,
    VVIC.suspiciousPct,
    VVIC.suspiciousImps,
    VVIC.measuredImps,
    VVIC.totalVideoImpressions,
    VVIC.totalImpressions
FROM
    (
    SELECT EXT_CAMPAIGN_ID as campaignId, IF(EXT_CAMPAIGN_NAME IS NULL, EXT_CAMPAIGN_ID, EXT_CAMPAIGN_NAME) as campaignName,EXT_PUBLISHER_ID as publisherId, IF(EXT_PUBLISHER_NAME IS NULL, EXT_PUBLISHER_ID, EXT_PUBLISHER_NAME) as publisherName,EXT_CHANNEL_ID as channelId, IF(EXT_CHANNEL_NAME IS NULL, EXT_CHANNEL_ID, EXT_CHANNEL_NAME) as channelName,EXT_PLACEMENT_ID as placementId, IF(EXT_PLACEMENT_NAME IS NULL, EXT_PLACEMENT_ID, EXT_PLACEMENT_NAME) as placementName,CONCAT(MIN(DIM_TIME.DATE), '..', MAX(DIM_TIME.DATE)) as hitDate, DATE_SUB(DIM_TIME.`DATE`, INTERVAL (DIM_TIME.`DAY` -1) DAY) as startDate, LAST_DAY(DIM_TIME.`DATE`) as endDate, CONCAT(DIM_TIME.YEAR, '-', LPAD(DIM_TIME.MONTH,2,'0')) as hitMonth, DIM_TIME.YEAR as hitYear,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*IN_VIEW_1Q_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as inView1qPct,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*COMPLETED_1Q_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as completed1qPct,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*IN_VIEW_2Q_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as inView2qPct,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*COMPLETED_2Q_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as completed2qPct,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*IN_VIEW_3Q_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as inView3qPct,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*COMPLETED_3Q_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as completed3qPct,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*IN_VIEW_4Q_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as inView4qPct,
        FORMAT(100*(SUM(FULLY_IN_VIEW_AT_COMPLETION)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)), 2) as inView4qPct100,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*COMPLETED_4Q_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as completed4qPct,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*FULLY_IN_VIEW_2Q_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as inView2qPct100,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*NEVER_STARTED_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as neverStartedPct,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*IN_VIEW_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as inViewPct,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*NOT_IN_VIEW_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as outOfViewPct,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*FULLY_IN_VIEW_1S_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as inViewPct100,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*MUTED_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as mutedPct,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*FULL_SCREEN_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as fullScreenPct,
        FORMAT(SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*CLICK_THROUGH_PCT)/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2) as clickThroughRate,
        100*SUM(SUSPICIOUS_IMPS)/SUM(IMPS) as suspiciousPct,
        SUM(SUSPICIOUS_IMPS) as suspiciousImps,
        ROUND(SUM(IF(Q_DATA_IMPS > IMPS, ((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)/(Q_DATA_IMPS/IMPS)), IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS))) as measuredImps,
        SUM(IF(COALESCE(MEDIA_TYPE_ID, 1) IN (2, 3, 112, 122, 132, 222, 232), IMPS, 0)) as totalVideoImpressions,
        SUM(IMPS) as totalImpressions,
        (
            (1 - (SUM(SUSPICIOUS_IMPS)/SUM(IMPS))) / FORMAT(SUM((IN_VIEW_PCT+NOT_IN_VIEW_PCT)*(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS))/SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 2)
        ) as scaleFactor
    FROM (
      SELECT EXT_CAMPAIGN_ID, EXT_CAMPAIGN.NAME as EXT_CAMPAIGN_NAME,EXT_PUBLISHER_ID, EXT_PUBLISHER.NAME as EXT_PUBLISHER_NAME,EXT_CHANNEL_ID, EXT_CHANNEL.NAME as EXT_CHANNEL_NAME,EXT_PLACEMENT_ID, EXT_PLACEMENT.NAME as EXT_PLACEMENT_NAME,HIT_DATE,
        CLICK_THROUGH_PCT,
        COMPLETED_1Q_PCT,
        COMPLETED_2Q_PCT,
        COMPLETED_3Q_PCT,
        COMPLETED_4Q_PCT,
        FULLY_IN_VIEW_1S_PCT,
        FULLY_IN_VIEW_2Q_PCT,
        FULL_SCREEN_PCT,
        IMPS,
        IN_VIEW_1Q_PCT,
        IN_VIEW_2Q_PCT,
        IN_VIEW_3Q_PCT,
        IN_VIEW_4Q_PCT,
        IN_VIEW_IMPS,
        IN_VIEW_PCT,
        MUTED_PCT,
        NEVER_STARTED_PCT,
        NOT_IN_VIEW_IMPS,
        NOT_IN_VIEW_PCT,
        Q_DATA_IMPS,
        SUSPICIOUS_IMPS,
        MEDIA_TYPE_ID,
        FULLY_IN_VIEW_AT_COMPLETION
      FROM
                AGG_NETWORK_QUALITY_V3 NETWORK
            LEFT JOIN (SELECT ID, NAME FROM EXT_CAMPAIGN WHERE  AD_NETWORK_ID=10079 ) EXT_CAMPAIGN ON (NETWORK.EXT_CAMPAIGN_ID=EXT_CAMPAIGN.ID)
            LEFT JOIN (SELECT ID, NAME FROM EXT_PUBLISHER WHERE  AD_NETWORK_ID=10079 ) EXT_PUBLISHER ON (NETWORK.EXT_PUBLISHER_ID=EXT_PUBLISHER.ID)
            LEFT JOIN (SELECT ID, NAME FROM EXT_PLACEMENT WHERE  AD_NETWORK_ID=10079 ) EXT_PLACEMENT ON (NETWORK.EXT_PLACEMENT_ID=EXT_PLACEMENT.ID)
            LEFT JOIN (SELECT ID, NAME FROM EXT_CHANNEL WHERE  AD_NETWORK_ID=10079 ) EXT_CHANNEL ON (NETWORK.EXT_CHANNEL_ID=EXT_CHANNEL.ID)

        WHERE AD_NETWORK_ID = 10079
        AND (COALESCE(MEDIA_TYPE_ID, 1) IN (2, 3, 112, 122, 132, 222, 232) OR HIT_DATE < '2015-10-23')
        AND (hit_date >= '2018-12-15' AND hit_date <= '2019-04-05')
      UNION ALL
      SELECT EXT_CAMPAIGN.EXT_CAMPAIGN_ID as EXT_CAMPAIGN_ID, EXT_CAMPAIGN.NAME as EXT_CAMPAIGN_NAME,CONCAT('PM', CAST(NETWORK.MEASUREMENT_SOURCE_ID as CHAR(50))) as EXT_PUBLISHER_ID, PUBLISHER_PM.NAME as EXT_PUBLISHER_NAME,CAST(PARTNER_MEASURED_CHANNEL_ID as CHAR(50)) as EXT_CHANNEL_ID, CAST(PARTNER_MEASURED_CHANNEL_ID as CHAR(50)) as EXT_CHANNEL_NAME,CAST(PARTNER_MEASURED_PLACEMENT_ID as CHAR(50)) as EXT_PLACEMENT_ID, EXT_PLACEMENT.NAME as EXT_PLACEMENT_NAME,HIT_DATE,
        CLICK_THROUGH_IMPS * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as CLICK_THROUGH_PCT,
        COMPLETED_1Q_IMPS * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as COMPLETED_1Q_PCT,
        COMPLETED_2Q_IMPS * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as COMPLETED_2Q_PCT,
        COMPLETED_3Q_IMPS * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as COMPLETED_3Q_PCT,
        COMPLETED_4Q_IMPS * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as COMPLETED_4Q_PCT,
        FULL_AD_IN_VIEW_IMPS*100/(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as FULLY_IN_VIEW_1S_PCT,
        IF(NETWORK.MEASUREMENT_SOURCE_ID in (2,3,6,7), NULL, GROUPM_IN_VIEW_2Q_IMPS)
         * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)
            as FULLY_IN_VIEW_2Q_PCT,
        FULL_SCREEN_IMPS * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as FULL_SCREEN_PCT,
        IMPS as IMPS,
        IN_VIEW_1Q_IMPS * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as IN_VIEW_1Q_PCT,
        IN_VIEW_2Q_IMPS * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as IN_VIEW_2Q_PCT,
        IN_VIEW_3Q_IMPS * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as IN_VIEW_3Q_PCT,
        IN_VIEW_4Q_IMPS * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as IN_VIEW_4Q_PCT,
        IN_VIEW_IMPS as IN_VIEW_IMPS,
        IN_VIEW_IMPS * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as IN_VIEW_PCT,
        MUTED_IMPS * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as MUTED_PCT,
        NEVER_STARTED_IMPS * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as NEVER_STARTED_PCT,
        NOT_IN_VIEW_IMPS as NOT_IN_VIEW_IMPS,
        NOT_IN_VIEW_IMPS * 100 / (IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS) as NOT_IN_VIEW_PCT,
        IMPS as Q_DATA_IMPS,
        SUSPICIOUS_IMPS as SUSPICIOUS_IMPS,
        MEDIA_TYPE_ID,
        0 as FULLY_IN_VIEW_AT_COMPLETION
      FROM AGG_PARTNER_MEASURED_VIEWABILITY NETWORK
        JOIN (SELECT ID, MEASUREMENT_SOURCE_ID, AD_NETWORK_ID, EXT_CAMPAIGN_ID, NAME FROM PARTNER_MEASURED_CAMPAIGN where AD_NETWORK_ID = 10079 ) EXT_CAMPAIGN ON (NETWORK.PARTNER_MEASURED_CAMPAIGN_ID=EXT_CAMPAIGN.ID AND
                       NETWORK.MEASUREMENT_SOURCE_ID = EXT_CAMPAIGN.MEASUREMENT_SOURCE_ID)

            LEFT JOIN (SELECT ms.ID as MEASUREMENT_SOURCE_ID, pub.NAME as NAME, pe.PUBLISHER_ID as ID FROM MEASUREMENT_SOURCE ms LEFT JOIN PUB_ENTITY pe
                        ON ms.PUB_ENTITY_ID = pe.ID LEFT JOIN PUBLISHER pub on pe.PUBLISHER_ID = pub.ID) PUBLISHER_PM ON (NETWORK.MEASUREMENT_SOURCE_ID = PUBLISHER_PM.MEASUREMENT_SOURCE_ID)
            LEFT JOIN PARTNER_MEASURED_PLACEMENT EXT_PLACEMENT ON (NETWORK.PARTNER_MEASURED_PLACEMENT_ID=EXT_PLACEMENT.ID AND
                       NETWORK.MEASUREMENT_SOURCE_ID = EXT_PLACEMENT.MEASUREMENT_SOURCE_ID)

      WHERE (hit_date >= '2018-12-15' AND hit_date <= '2019-04-05')
        AND COALESCE(MEDIA_TYPE_ID, 1) IN (2, 3, 112, 122, 132, 222, 232)
    ) union_table
        LEFT JOIN DIM_TIME ON (HIT_DATE = DIM_TIME.Date)

    GROUP BY EXT_CAMPAIGN_ID,EXT_PUBLISHER_ID,EXT_CHANNEL_ID,EXT_PLACEMENT_ID,DIM_TIME.MONTH
    HAVING SUM(IMPS) >= 0
     LIMIT 200000 OFFSET 0
    ) VVIC
ORDER BY totalVideoImpressions desc
