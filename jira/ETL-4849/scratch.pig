REGISTER /home/dbelyavsky/.m2/repository/com/integralads/hadoop_analytics/1.3/hadoop_analytics-1.3.jar;

adn_s05 = load '/user/thresher/mart/network/2016/09/12/20160913003501/filtered_cutdown_adn_s05' using PigStorage() as
(
    hit_date,
    hit_hour,
    partner_code,
    ad_network_id,
    ad_network_name,
    -- ext_publisher_id,
    -- ext_channel_id,
    -- ext_placement_id,
    -- ext_advertiser_id,
    -- ext_campaign_id,
    -- ext_plan_id,
    TRUNCATE_UNICODE(ext_publisher_id, 50) as ext_publisher_id,
    TRUNCATE_UNICODE(ext_channel_id, 50) as ext_channel_id,
    TRUNCATE_UNICODE(ext_placement_id, 50) as ext_placement_id,
    TRUNCATE_UNICODE(ext_advertiser_id, 50) as ext_advertiser_id,
    TRUNCATE_UNICODE(ext_campaign_id, 50) as ext_campaign_id,
    TRUNCATE_UNICODE(ext_plan_id, 50) as ext_plan_id,
    site,
    action,
    reason,
    visibility_sc,
    adult_sc,
    hate_sc,
    illegaldownloads_sc,
    dirtywords_sc,
    alcohol_sc,
    drug_sc,
    pri_iab_t1_name,
    sec_iab_t1_name,
    ter_iab_t1_name,
    pri_iab_t2_name,
    sec_iab_t2_name,
    ter_iab_t2_name,
    imps,
    violence_sc
);

agg_network_imps_pre = foreach adn_s05 generate
    hit_date,
    ad_network_id,
    ad_network_name,
    ext_publisher_id,
    ext_channel_id,
    ext_placement_id,
    ext_advertiser_id,
    ext_campaign_id,
    ext_plan_id,
    site,
    visibility_sc as visibilityScore,
    imps;

agg_network_imps_grouped = GROUP agg_network_imps_pre BY (
    hit_date,
    ad_network_id,
    ad_network_name,
    ext_publisher_id,
    ext_channel_id,
    ext_placement_id,
    ext_advertiser_id,
    ext_campaign_id,
    ext_plan_id,
    site
);

agg_network_imps = FOREACH agg_network_imps_grouped GENERATE
    flatten(group),
    com.adsafe.pigudfs.eval.WAMEAN(agg_network_imps_pre.(visibilityScore,imps)) as visibilityScore,
    SUM(agg_network_imps_pre.imps) as imps;

-- LOAD TRAQ/SAD DATA:
traq = load '/user/dbelyavsky/aggdt3/out.20161006103136.gz' USING PigStorage() as
(
    report_name:chararray,
    hit_date:chararray,
    hit_hour:int,
    lookupId:int,
    passbackId:int,
    ad_network_id:int,
    ext_publisher_id:chararray,
    ext_channel_id:chararray,
    ext_placement_id:chararray,
    ext_advertiser_id:chararray,
    ext_campaign_id:chararray,
    ext_plan_id:chararray,
    site:chararray,
    sitelet:chararray,
    nlCountry:chararray,
    nlFl:chararray,
    browser:chararray,
    browser_version:chararray,
    browser_friendly:chararray,
    -- various impression counts for weighting
    -- only using imps/sample_imps for now
    imps:int,
    imps_initial_state:int,
    imps_final_state:int,
    sample_imps:int,
    -- MRC-certified counting metrics
    in_view_1s_imps:int,
    not_in_view_1s_imps:int,
    suspicious_imps:int,
    unmeasured_imps:int,
    -- "extended" viewability metrics (rates)
    in_view_1s:float,
    not_in_view_1s:float,
    in_view_5s:float,
    in_view_15s:float,
    in_view_load:float,
    in_view_unload:float,
    never_in_view:float,
    -- timing metrics
    in_view_time:int,
    time_on_page:int,
    -- fold-relative metrics
    below_the_fold_imps:int,
    on_the_fold_imps:int,
    above_the_fold_imps:int,
    -- TRAQ
    raw_traq_score:float
);


-- ONLY LOOK AT AD_NETWORK DATA
filtered_traq = filter traq by ad_network_id > 0;

-- LOAD LOOKUP DATA
ad_network = load '/user/thresher/mart/network/2016/09/12/20160913003501/lookup.ad_network/part-*' USING PigStorage() as (id:int, name:chararray, partner_code:chararray);

-- ADD IN LOOKUP DATA TO traq
traq_join1 = JOIN filtered_traq by ad_network_id LEFT OUTER, ad_network by id USING 'skewed' ; -- PARALLEL $PARALLEL;

traq_network_pre_pre = foreach traq_join1 generate
    hit_date,
    hit_hour,
    ad_network_id,
    name as ad_network_name,
    ext_publisher_id,
    ext_channel_id,
    ext_placement_id,
    ext_advertiser_id,
    ext_campaign_id,
    ext_plan_id,
    site,
    (float)((float)suspicious_imps/(float)imps) as raw_sad_score,
    raw_traq_score,
    imps;


-- TRANSLATE 0.0-1.0 SAD/TRAQ SCORES TO 1-1000:
traq_network_pre = foreach traq_network_pre_pre generate
    hit_date,
    hit_hour,
    ad_network_id,
    ad_network_name,
    ext_publisher_id,
    ext_channel_id,
    ext_placement_id,
    ext_advertiser_id,
    ext_campaign_id,
    ext_plan_id,
    site,
    (raw_sad_score >= 0.8 ? 250 :
          (raw_sad_score >= 0.5 and raw_sad_score < 0.8 ? 400 :
            (raw_sad_score >= 0.3 and raw_sad_score < 0.5 ? 500 :
              (raw_sad_score >= 0.2 and raw_sad_score < 0.3 ? 600 :
                (raw_sad_score >= 0.1 and raw_sad_score < 0.2 ? 700 :
                  (raw_sad_score >= 0.05 and raw_sad_score < 0.1 ? 875 :
                    (raw_sad_score < 0.05 ? 1000 :
                      -1
                    )
                  )
                )
              )
            )
          )
        ) as sad_score,
        (raw_traq_score >= 0.7 ? 1000 :
          (raw_traq_score >= 0.6 and raw_traq_score < 0.7 ? 875 :
            (raw_traq_score >= 0.5 and raw_traq_score < 0.6 ? 750 :
              (raw_traq_score >= 0.35 and raw_traq_score < 0.5 ? 700 :
                (raw_traq_score >= 0.2 and raw_traq_score < 0.35 ? 600 :
                  (raw_traq_score >= 0.15 and raw_traq_score < 0.2 ? 500 :
                    (raw_traq_score >= 0.1 and raw_traq_score < 0.15 ? 400 :
                      (raw_traq_score < 0.1 ? 250 :
                        -1
                      )
                    )
                  )
                )
              )
            )
          )
        ) as traq_score,
        raw_sad_score,
        raw_traq_score,
        imps;

--
-- traq_network_pre_0traq = filter traq_network_pre by traq_score == 0;
-- store traq_network_pre_0traq into 'traq_network_pre_0traq' using PigStorage('\t');

-- SEGMENT 1-1000 SCORES INTO RISK BUCKETS:
traq_network_buckets = foreach traq_network_pre generate
    hit_date,
    ad_network_id,
    ad_network_name,
    ext_publisher_id,
    ext_channel_id,
    ext_placement_id,
    ext_advertiser_id,
    ext_campaign_id,
    ext_plan_id,
    site,
    (sad_score > 750?imps:0) as low_sad,
    (sad_score <= 750?(sad_score >500?imps:0):0) as moderate_sad,
    (sad_score <= 500?(sad_score >250?imps:0):0) as high_sad,
    (sad_score <= 250?(sad_score >0?imps:0):0) as very_high_sad,
    (sad_score < 0?imps:0) as unknown_sad,
    (traq_score > 750?imps:0) as very_high_traq,
    (traq_score <= 750?(traq_score >500?imps:0):0) as high_traq,
    (traq_score <= 500?(traq_score >250?imps:0):0) as moderate_traq,
    (traq_score <= 250?(traq_score >0?imps:0):0) as low_traq,
    (traq_score < 0?imps:0) as unknown_traq,
    sad_score,
    traq_score,
    raw_sad_score,
    raw_traq_score,
    imps;

-- store traq_network_buckets into 'traq_network_buckets' using PigStorage('\t');

agg_network_traq_site_grouped = GROUP traq_network_buckets BY (
    hit_date,
    ad_network_id,
    ad_network_name,
    ext_publisher_id,
    ext_channel_id,
    ext_placement_id,
    ext_advertiser_id,
    ext_campaign_id,
    ext_plan_id,
    site
);

agg_network_traq_site_pre = FOREACH agg_network_traq_site_grouped GENERATE
    flatten(group),
    SUM(traq_network_buckets.low_sad) as low_sad,
    SUM(traq_network_buckets.moderate_sad) as moderate_sad,
    SUM(traq_network_buckets.high_sad) as high_sad,
    SUM(traq_network_buckets.very_high_sad) as very_high_sad,
    SUM(traq_network_buckets.unknown_sad) as unknown_sad,
    SUM(traq_network_buckets.low_traq) as low_traq,
    SUM(traq_network_buckets.moderate_traq) as moderate_traq,
    SUM(traq_network_buckets.high_traq) as high_traq,
    SUM(traq_network_buckets.very_high_traq) as very_high_traq,
    SUM(traq_network_buckets.unknown_traq) as unknown_traq,
    com.adsafe.pigudfs.eval.WAMEAN(traq_network_buckets.(sad_score,imps)) as sad_score,
    com.adsafe.pigudfs.eval.WAMEAN(traq_network_buckets.(traq_score,imps)) as traq_score,
    SUM(traq_network_buckets.imps) as imps,
    com.adsafe.pigudfs.eval.WAMEAN(traq_network_buckets.(raw_sad_score,imps)) as raw_sad_score,
    com.adsafe.pigudfs.eval.WAMEAN(traq_network_buckets.(raw_traq_score,imps)) as raw_traq_score;

-- store agg_network_traq_site_pre into 'agg_network_traq_site_pre' using PigStorage('\t');
-- agg_network_traq_site_pre_0traq = filter agg_network_traq_site_pre by traq_score == 0;
-- store agg_network_traq_site_pre_0traq into 'agg_network_traq_site_pre_0traq' using PigStorage('\t');

--JOIN WITH S05 FOR VISIBILITY SCORES
agg_network_traq_site_join = JOIN
    agg_network_imps BY (hit_date, ad_network_id, ext_publisher_id, ext_channel_id, ext_placement_id, ext_advertiser_id, ext_campaign_id, ext_plan_id, site),
    agg_network_traq_site_pre BY (hit_date, ad_network_id, ext_publisher_id, ext_channel_id, ext_placement_id, ext_advertiser_id, ext_campaign_id, ext_plan_id, site);
/*
agg_network_imps_filtered = filter agg_network_imps by ad_network_id == '10285' and ext_placement_id == 'TWC/OANM_SEG_1070512-MP00322_P_728x90-300x250-320x';
store agg_network_imps_filtered into 'agg_network_imps_filtered' using PigStorage('\t');

store agg_network_traq_site_pre into 'agg_network_traq_site_pre' using PigStorage('\t');

agg_network_traq_site_join_filtered = filter agg_network_traq_site_join by agg_network_imps::group::ad_network_id == '10285' and agg_network_imps::group::ext_placement_id == 'TWC/OANM_SEG_1070512-MP00322_P_728x90-300x250-320x';
store agg_network_traq_site_join_filtered into 'agg_network_traq_site_join_filtered' using PigStorage('\t');
*/
agg_network_traq_site = FOREACH agg_network_traq_site_join GENERATE
    agg_network_imps::group::hit_date,
    agg_network_imps::group::ad_network_id,
    agg_network_imps::group::ad_network_name,
    agg_network_imps::group::ext_publisher_id,
    agg_network_imps::group::ext_channel_id,
    agg_network_imps::group::ext_placement_id,
    agg_network_imps::group::ext_advertiser_id,
    agg_network_imps::group::ext_campaign_id,
    agg_network_imps::group::ext_plan_id,
    agg_network_imps::group::site,
    (long) (agg_network_traq_site_pre::low_sad is null ? 0 : agg_network_traq_site_pre::low_sad),
    (long) (agg_network_traq_site_pre::moderate_sad is null ? 0 : agg_network_traq_site_pre::moderate_sad),
    (long) (agg_network_traq_site_pre::high_sad is null ? 0 : agg_network_traq_site_pre::high_sad),
    (long) (agg_network_traq_site_pre::very_high_sad is null ? 0 : agg_network_traq_site_pre::very_high_sad),
    (long) (agg_network_traq_site_pre::unknown_sad is null ? 0 : agg_network_traq_site_pre::unknown_sad),
    (long) (agg_network_traq_site_pre::low_traq is null ? 0 : agg_network_traq_site_pre::low_traq),
    (long) (agg_network_traq_site_pre::moderate_traq is null ? 0 : agg_network_traq_site_pre::moderate_traq),
    (long) (agg_network_traq_site_pre::high_traq is null ? 0 : agg_network_traq_site_pre::high_traq),
    (long) (agg_network_traq_site_pre::very_high_traq is null ? 0 : agg_network_traq_site_pre::very_high_traq),
    (long) (agg_network_traq_site_pre::unknown_traq is null ? 0 : agg_network_traq_site_pre::unknown_traq),
    (agg_network_traq_site_pre::sad_score is null ? 0 : agg_network_traq_site_pre::sad_score),
    (agg_network_traq_site_pre::traq_score is null ? 0 : agg_network_traq_site_pre::traq_score),
    (long) (agg_network_traq_site_pre::imps is null ? 0 : agg_network_traq_site_pre::imps),
    (agg_network_traq_site_pre::raw_sad_score is null ? 0 : agg_network_traq_site_pre::raw_sad_score),
    (agg_network_traq_site_pre::raw_traq_score is null ? 0 : agg_network_traq_site_pre::raw_traq_score),
    (long) agg_network_imps::visibilityScore;

-- ????
-- agg_network_traq_site_0traq = filter agg_network_traq_site by traq_score == 0;
-- store agg_network_traq_site_0traq into 'agg_network_traq_site_0traq' using PigStorage('\t');

-- THIS IS THE SITE LEVEL TABLE
remove_adsafe = FILTER agg_network_traq_site BY NOT(site MATCHES '.*adsafeprotected.*');
agg_network_traq_sorted = ORDER remove_adsafe BY ad_network_id, ext_campaign_id, site;
-- store agg_network_traq_sorted into '$OUTDIR/agg_network_traq_site' using PigStorage('\t');

-- PREPARE ANOTHER ONE AT THE EXT_IDS LEVEL
-- REGROUP adn_s05 AT EXT_IDS LEVEL:
agg_network_imps_grouped2 = GROUP agg_network_imps_pre BY (
    hit_date,
    ad_network_id,
    ad_network_name,
    ext_publisher_id,
    ext_channel_id,
    ext_placement_id,
    ext_advertiser_id,
    ext_campaign_id,
    ext_plan_id
);

agg_network_imps2 = FOREACH agg_network_imps_grouped2 GENERATE
    flatten(group),
    com.adsafe.pigudfs.eval.WAMEAN(agg_network_imps_pre.(visibilityScore,imps)) as visibilityScore,
    SUM(agg_network_imps_pre.imps) as imps;

-- REGROUP TRAQ DATA AT EXT_IDS LEVEL:
agg_network_traq2_grouped = GROUP traq_network_buckets BY (
    hit_date,
    ad_network_id,
    ad_network_name,
    ext_publisher_id,
    ext_channel_id,
    ext_placement_id,
    ext_advertiser_id,
    ext_campaign_id,
    ext_plan_id
);

agg_network_traq_pre = FOREACH agg_network_traq2_grouped GENERATE
    flatten(group),
    SUM(traq_network_buckets.low_sad) as low_sad,
    SUM(traq_network_buckets.moderate_sad) as moderate_sad,
    SUM(traq_network_buckets.high_sad) as high_sad,
    SUM(traq_network_buckets.very_high_sad) as very_high_sad,
    SUM(traq_network_buckets.unknown_sad) as unknown_sad,
    SUM(traq_network_buckets.low_traq) as low_traq,
    SUM(traq_network_buckets.moderate_traq) as moderate_traq,
    SUM(traq_network_buckets.high_traq) as high_traq,
    SUM(traq_network_buckets.very_high_traq) as very_high_traq,
    SUM(traq_network_buckets.unknown_traq) as unknown_traq,
    com.adsafe.pigudfs.eval.WAMEAN(traq_network_buckets.(sad_score,imps)) as sad_score,
    com.adsafe.pigudfs.eval.WAMEAN(traq_network_buckets.(traq_score,imps)) as traq_score,
    com.adsafe.pigudfs.eval.WAMEAN(traq_network_buckets.(raw_sad_score,imps)) as raw_sad_score,
    com.adsafe.pigudfs.eval.WAMEAN(traq_network_buckets.(raw_traq_score,imps)) as raw_traq_score,
    SUM(traq_network_buckets.imps) as sample_imps;
/*
store agg_network_traq_pre into 'agg_network_traq_pre' USING  PigStorage('\t');
agg_network_traq_pre_0traq = filter agg_network_traq_pre by traq_score == 0;
store agg_network_traq_pre_0traq into 'agg_network_traq_pre_0traq' USING  PigStorage('\t');

agg_network_imps2_filtered = filter agg_network_imps2 by ad_network_id == '10285' and ext_placement_id == 'TWC/OANM_SEG_1070512-MP00322_P_728x90-300x250-320x';
store agg_network_imps2_filtered into 'agg_network_imps2_filtered' using PigStorage('\t');
*/
agg_network_traq_join = JOIN
    agg_network_imps2 BY (hit_date, ad_network_id, ext_publisher_id, ext_channel_id, ext_placement_id, ext_advertiser_id, ext_campaign_id, ext_plan_id) LEFT OUTER,
    agg_network_traq_pre BY (hit_date, ad_network_id, ext_publisher_id, ext_channel_id, ext_placement_id, ext_advertiser_id, ext_campaign_id, ext_plan_id);
/*
store agg_network_traq_join into 'agg_network_traq_join' using PigStorage('\t');

agg_network_traq_join_0traq = filter agg_network_traq_join by agg_network_traq_pre::traq_score == 0 or agg_network_traq_pre::traq_score is null;
store agg_network_traq_join_0traq into 'agg_network_traq_join_0traq' USING PigStorage('\t');
*/
agg_network_traq = FOREACH agg_network_traq_join GENERATE
    agg_network_imps2::group::hit_date,
    agg_network_imps2::group::ad_network_id,
    agg_network_imps2::group::ad_network_name,
    agg_network_imps2::group::ext_publisher_id,
    agg_network_imps2::group::ext_channel_id,
    agg_network_imps2::group::ext_placement_id,
    agg_network_imps2::group::ext_advertiser_id,
    agg_network_imps2::group::ext_campaign_id,
    agg_network_imps2::group::ext_plan_id,
    (long) (agg_network_traq_pre::low_sad is null ? 0 : agg_network_traq_pre::low_sad),
    (long) (agg_network_traq_pre::moderate_sad is null ? 0 : agg_network_traq_pre::moderate_sad),
    (long) (agg_network_traq_pre::high_sad is null ? 0 : agg_network_traq_pre::high_sad),
    (long) (agg_network_traq_pre::very_high_sad is null ? 0 : agg_network_traq_pre::very_high_sad),
    (long) (agg_network_traq_pre::unknown_sad is null ? 0 : agg_network_traq_pre::unknown_sad),
    (long) (agg_network_traq_pre::low_traq is null ? 0 : agg_network_traq_pre::low_traq),
    (long) (agg_network_traq_pre::moderate_traq is null ? 0 : agg_network_traq_pre::moderate_traq),
    (long) (agg_network_traq_pre::high_traq is null ? 0 : agg_network_traq_pre::high_traq),
    (long) (agg_network_traq_pre::very_high_traq is null ? 0 : agg_network_traq_pre::very_high_traq),
    (long) (agg_network_traq_pre::unknown_traq is null ? 0 : agg_network_traq_pre::unknown_traq),
    (agg_network_traq_pre::sad_score is null ? 0 : agg_network_traq_pre::sad_score),
    (agg_network_traq_pre::traq_score is null ? 0 : agg_network_traq_pre::traq_score),
    (long) (agg_network_traq_pre::sample_imps is null ? 0 : agg_network_traq_pre::sample_imps),
    (long) agg_network_imps2::imps,
    (agg_network_traq_pre::raw_sad_score is null ? 0 : agg_network_traq_pre::raw_sad_score),
    (agg_network_traq_pre::raw_traq_score is null ? 0 : agg_network_traq_pre::raw_traq_score);

-- agg_network_traq_0traq = filter agg_network_traq by $21 == 0;
-- store agg_network_traq_0traq into 'agg_network_traq_0traq' USING PigStorage('\t');

store agg_network_traq into 'agg_network_traq' using PigStorage('\t');
