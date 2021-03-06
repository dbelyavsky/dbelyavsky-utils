SELECT
	EXT_CAMPAIGN_ID as campaign_id, CASE WHEN EXT_CAMPAIGN.NAME IS NULL THEN EXT_CAMPAIGN_ID ELSE EXT_CAMPAIGN.NAME END as campaign_name,EXT_PUBLISHER_ID as publisher_id, CASE WHEN EXT_PUBLISHER.NAME IS NULL THEN EXT_PUBLISHER_ID ELSE EXT_PUBLISHER.NAME END as publisher_name,EXT_CHANNEL_ID as channel_id, CASE WHEN EXT_CHANNEL.NAME IS NULL THEN EXT_CHANNEL_ID ELSE EXT_CHANNEL.NAME END as channel_name,EXT_PLACEMENT_ID as placement_id, CASE WHEN EXT_PLACEMENT.NAME IS NULL THEN EXT_PLACEMENT_ID ELSE EXT_PLACEMENT.NAME END as placement_name,CONCAT(from_unixtime(unix_timestamp(MIN(DIM_TIME.DimDate)),'yyyy-MM-dd'), CONCAT('..', from_unixtime(unix_timestamp(MAX(DIM_TIME.DimDate)),'yyyy-MM-dd'))) as hit_date, from_unixtime(unix_timestamp(MIN(DIM_TIME.DimDate)),'yyyy-MM-dd') as start_date, from_unixtime(unix_timestamp(MAX(DIM_TIME.DimDate)),'yyyy-MM-dd') as end_date, DIM_TIME.MONTH as hit_month,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*IN_VIEW_1Q_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as in_view1q_pct,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*COMPLETED_1Q_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as completed1q_pct,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*IN_VIEW_2Q_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as in_view2q_pct,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*COMPLETED_2Q_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as completed2q_pct,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*IN_VIEW_3Q_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as in_view3q_pct,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*COMPLETED_3Q_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as completed3q_pct,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*IN_VIEW_4Q_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as in_view4q_pct,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*COMPLETED_4Q_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as completed4q_pct,
	ROUND(100
	    * 100*(SUM(FULLY_IN_VIEW_AT_COMPLETION)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0))
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as in_view4q_pct100,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*FULLY_IN_VIEW_2Q_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as in_view2q_pct100,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*NEVER_STARTED_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as never_started_pct,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*IN_VIEW_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as in_view_pct,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*NOT_IN_VIEW_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as out_of_view_pct,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*FULLY_IN_VIEW_1S_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as in_view_pct100,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*MUTED_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as muted_pct,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*FULL_SCREEN_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as full_screen_pct,
	ROUND(100
	    * SUM((IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS)*CLICK_THROUGH_PCT)/NULLIF(SUM(IN_VIEW_IMPS+NOT_IN_VIEW_IMPS+SUSPICIOUS_IMPS), 0)
	    * (1 - (SUM(SUSPICIOUS_IMPS) / NULLIF(SUM(IMPS),0))) / ROUND(SUM((IN_VIEW_PCT + NOT_IN_VIEW_PCT) * (IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS)) / NULLIF(SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS),0), 2), 2) as click_through_rate,
	100*SUM(SUSPICIOUS_IMPS)/NULLIF(SUM(IMPS),0) as suspicious_pct,
	SUM(SUSPICIOUS_IMPS) as suspicious_imps,
	SUM(IN_VIEW_IMPS + NOT_IN_VIEW_IMPS + SUSPICIOUS_IMPS) as measured_imps,
	SUM(CASE WHEN COALESCE(MEDIA_TYPE_ID, 1) IN (2, 3, 112, 122, 132, 222, 232) THEN IMPS ELSE 0 END ) as total_video_impressions,
	SUM(IMPS) as total_impressions

FROM AGG_NETWORK_QUALITY_V3 NETWORK
	    LEFT JOIN (SELECT ID, NAME FROM EXT_CAMPAIGN WHERE  AD_NETWORK_ID=10079 ) EXT_CAMPAIGN ON (NETWORK.EXT_CAMPAIGN_ID=EXT_CAMPAIGN.ID)
	    LEFT JOIN (SELECT ID, NAME FROM EXT_PUBLISHER WHERE  AD_NETWORK_ID=10079 ) EXT_PUBLISHER ON (NETWORK.EXT_PUBLISHER_ID=EXT_PUBLISHER.ID)
	    LEFT JOIN (SELECT ID, NAME FROM EXT_PLACEMENT WHERE  AD_NETWORK_ID=10079 ) EXT_PLACEMENT ON (NETWORK.EXT_PLACEMENT_ID=EXT_PLACEMENT.ID)
	    LEFT JOIN (SELECT ID, NAME FROM EXT_CHANNEL WHERE  AD_NETWORK_ID=10079 ) EXT_CHANNEL ON (NETWORK.EXT_CHANNEL_ID=EXT_CHANNEL.ID)

	    LEFT JOIN DIM_TIME ON (HIT_DATE = DIM_TIME.DimDate)

WHERE AD_NETWORK_ID = 10079
	AND (COALESCE(MEDIA_TYPE_ID, 1) IN (2, 3, 112, 122, 132, 222, 232))
	AND (hit_date >= '2018-12-15' AND hit_date <= '2019-04-05') AND (dt >= 20181215 AND dt <= 20190405)
GROUP BY EXT_CAMPAIGN_ID, EXT_CAMPAIGN.NAME,EXT_PUBLISHER_ID, EXT_PUBLISHER.NAME,EXT_CHANNEL_ID, EXT_CHANNEL.NAME,EXT_PLACEMENT_ID, EXT_PLACEMENT.NAME,DIM_TIME.MONTH
HAVING SUM(IMPS) >= 0
ORDER BY total_video_impressions desc
 LIMIT 200000 OFFSET 0 
