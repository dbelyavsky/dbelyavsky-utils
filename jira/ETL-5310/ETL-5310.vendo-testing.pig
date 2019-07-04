/*
pig -param EXT_IDS_DIR=/user/thresher/ad_network_ext_ids -param ext_ids_min_imp_threshold=1 -param camps_per_anid=500 -param placs_per_anid=1500 -param filter_anids='502|5425|8087|7522|7314|10001|10002|7443|8059|8265|8004|9937|8087|922334|7708|925172' -param channel_id_limit=3 -l /home/thresher/log/sty -param PARALLEL=1 -param OUTDIR=dbelyavsky/ext_ids_lookup -param BASEDIR="/home/thresher/ias_mart" -param MAPRED_CHILD_JAVA_OPTS="-Xmx512m" -param IO_SORT_MB="50" -f /home/thresher/ias_mart/pig/ext_ids_lookup.pig
*/

%default PARALLEL '8'
%default ext_ids_min_imp_threshold '100'
%default camps_per_anid '500'
%default placs_per_anid '1500'
%default channel_id_limit '1000'

SET default_parallel $PARALLEL;

-- load all unique ids for the last n months
unique_ext_ids_last_n_months = LOAD '$EXT_IDS_DIR/*' using PigStorage() as
(
    hit_date:chararray,
    ad_network_id:chararray,
    ext_publisher_id:chararray,
    ext_channel_id:chararray,
    ext_placement_id:chararray,
    ext_advertiser_id:chararray,
    ext_campaign_id:chararray,
    ext_plan_id:chararray,
    imps:long
);

-- filter out anIds we wont want for the UI
unique_ext_ids_last_n_months_minus_some_anids = FILTER unique_ext_ids_last_n_months BY NOT ad_network_id MATCHES '($filter_anids)';

-- we dont need date, ext_plan_id and imps in the final output, so remove those
unique_ext_ids_primary_keys = FOREACH (
                                   GROUP unique_ext_ids_last_n_months_minus_some_anids BY
                                   (ad_network_id, ext_publisher_id, ext_channel_id, ext_placement_id, ext_advertiser_id, ext_campaign_id)
            ) GENERATE
            FLATTEN(group),
            SUM(unique_ext_ids_last_n_months_minus_some_anids.imps) as imps;

-- select top 500 campaigns in the lastNmonths overall, per anId
anid_advid_campid_imps = FOREACH (
            GROUP unique_ext_ids_primary_keys BY
            (ad_network_id, ext_advertiser_id, ext_campaign_id)
        ) GENERATE
    FLATTEN(group),
    SUM(unique_ext_ids_primary_keys.imps) as imps;

anid_advid_campid_imps_minus_junk = FILTER anid_advid_campid_imps BY imps > $ext_ids_min_imp_threshold;

records_grouped_by_anid = GROUP anid_advid_campid_imps_minus_junk BY (ad_network_id);

top_n_campaigns = FOREACH records_grouped_by_anid {
        sorted_by_imps = ORDER anid_advid_campid_imps_minus_junk BY imps desc;
        top_n = LIMIT sorted_by_imps $camps_per_anid;
        GENERATE group, flatten(top_n);
};

-- prepare another bag with top 1500 placements in the lastNmonths, per anId.
-- but these 1500 placements should be from within the top 500 campaigns.
-- so filter out all non-top500 campaigns before preparing the top1500 placements bag
unique_ext_ids_for_top_n_campaigns_join = JOIN unique_ext_ids_primary_keys BY (ad_network_id, ext_advertiser_id, ext_campaign_id),
                    top_n_campaigns BY (ad_network_id, ext_advertiser_id, ext_campaign_id);

unique_ext_ids_for_top_n_campaigns = FOREACH unique_ext_ids_for_top_n_campaigns_join GENERATE
    unique_ext_ids_primary_keys::group::ad_network_id as ad_network_id,
    unique_ext_ids_primary_keys::group::ext_advertiser_id as ext_advertiser_id,
    unique_ext_ids_primary_keys::group::ext_campaign_id as ext_campaign_id,
    unique_ext_ids_primary_keys::group::ext_publisher_id as ext_publisher_id,
    unique_ext_ids_primary_keys::group::ext_channel_id as ext_channel_id,
    unique_ext_ids_primary_keys::group::ext_placement_id as ext_placement_id,
    unique_ext_ids_primary_keys::imps as imps;


-- now we have all ext_ids records only for the top500 campaigns.
-- prepare the top1500 placements bag
anid_advid_campid_placid_imps = FOREACH (
                        GROUP unique_ext_ids_for_top_n_campaigns BY
                        (ad_network_id, ext_advertiser_id, ext_campaign_id, ext_placement_id)
                ) GENERATE
        FLATTEN(group),
        SUM(unique_ext_ids_for_top_n_campaigns.imps) as imps;

anid_advid_campid_placid_imps_minus_junk = FILTER anid_advid_campid_placid_imps BY imps > $ext_ids_min_imp_threshold;

records_grouped_by_anid2 = GROUP anid_advid_campid_placid_imps_minus_junk BY (ad_network_id);

top_n_placements = FOREACH records_grouped_by_anid2 {
    sorted_by_imps = ORDER anid_advid_campid_placid_imps_minus_junk BY imps desc;
    top_n = LIMIT sorted_by_imps $placs_per_anid;
    GENERATE group, flatten(top_n);
};

-- now we have topN placements belonging to topN campaigns
-- join the two to narrow the results
top_camp_plac_join = JOIN unique_ext_ids_for_top_n_campaigns BY (ad_network_id, ext_advertiser_id, ext_campaign_id, ext_placement_id),
            top_n_placements BY (ad_network_id, ext_advertiser_id, ext_campaign_id, ext_placement_id);

-- project the join wtih imps to make later steps more readable
top_camp_plac_imps = FOREACH top_camp_plac_join GENERATE
    unique_ext_ids_for_top_n_campaigns::ad_network_id as ad_network_id,
    unique_ext_ids_for_top_n_campaigns::ext_advertiser_id as ext_advertiser_id,
    unique_ext_ids_for_top_n_campaigns::ext_campaign_id as ext_campaign_id,
    unique_ext_ids_for_top_n_campaigns::ext_publisher_id as ext_publisher_id,
    unique_ext_ids_for_top_n_campaigns::ext_channel_id as ext_channel_id,
    unique_ext_ids_for_top_n_campaigns::ext_placement_id as ext_placement_id,
    unique_ext_ids_for_top_n_campaigns::imps as imps;
/*
(7,2,3,8,1,6,38)
(7,2,3,8,2,6,30)
(7,2,3,8,3,6,38)
(7,2,3,8,4,6,5)
(7,2,3,8,5,6,10)
(7,2,3,8,6,6,27)
(7,2,3,8,7,6,9)
(7,2,3,8,8,6,8)
(7,2,3,8,9,6,38)
(7,2,3,8,10,6,10)
(7,2,3,8,11,6,11)
(7,2,3,8,12,6,12)
(7,2,3,8,13,6,13)
(7,2,3,8,15,6,17)
(7,2,3,8,20,6,23)
(7,2,3,8,23,6,1)
(7,2,3,8,24,6,1)
(7,2,3,8,25,6,1)
(7,2,3,8,26,6,1)
(7,2,3,8,27,6,1)
(7,2,3,8,28,6,1)
(7,2,3,8,29,6,1)
(7,2,3,8,30,6,1)
(7,2,3,8,31,6,1)
(7,2,3,8,32,6,1)
(7,2,3,8,33,6,4)
(7,2,3,8,34,6,1)
(7,2,3,8,35,6,1)
(7,2,3,8,36,6,1)
(7,2,3,8,37,6,1)
(7,2,3,8,38,6,1)
(7,2,3,8,39,6,1)
(7,2,3,8,40,6,1)
(7,2,3,8,44,6,4)
(7,2,3,8,51,6,2)
(7,2,3,8,55,6,5)
(7,2,3,8,66,6,7)
(7,2,3,8,77,6,7)
(7,2,3,8,888,6,8)
*/

----------------------************--------------------
-- prepare topN channelIDs bag
anid_advid_campid_placid_chanid_imps = FOREACH (
        GROUP top_camp_plac_imps by ( ad_network_id, ext_advertiser_id, ext_channel_id)
    ) GENERATE
        FLATTEN(group),
        SUM(top_camp_plac_imps.imps) as imps;
/*
anid_advid_campid_placid_chanid_imps: {
    group::ad_network_id: chararray,
    group::ext_advertiser_id: chararray,
    group::ext_channel_id: chararray,
    imps: long}

(7,2,1,38)
(7,2,2,30)
(7,2,3,38)
(7,2,4,5)
(7,2,5,10)
(7,2,6,27)
(7,2,7,9)
(7,2,8,8)
(7,2,9,38)
(7,2,10,10)
(7,2,11,11)
(7,2,12,12)
(7,2,13,13)
(7,2,15,17)
(7,2,20,23)
(7,2,23,1)
(7,2,24,1)
(7,2,25,1)
(7,2,26,1)
(7,2,27,1)
(7,2,28,1)
(7,2,29,1)
(7,2,30,1)
(7,2,31,1)
(7,2,32,1)
(7,2,33,4)
(7,2,34,1)
(7,2,35,1)
(7,2,36,1)
(7,2,37,1)
(7,2,38,1)
(7,2,39,1)
(7,2,40,1)
(7,2,44,4)
(7,2,51,2)
(7,2,55,5)
(7,2,66,7)
(7,2,77,7)
(7,2,888,8)
*/
records_grouped_by_anid3 = GROUP anid_advid_campid_placid_chanid_imps BY (ad_network_id);
/*
(7,{(7,2,1,38),(7,2,2,30),(7,2,3,38),(7,2,4,5),(7,2,5,10),(7,2,6,27),(7,2,7,9),(7,2,8,8),(7,2,9,38),(7,2,10,10),(7,2,11,11),(7,2,12,12),(7,2,13,13),(7,2,15,17),(7,2,20,23),(7,2,23,1),(7,2,24,1),(7,2,25,1),(7,2,26,1),(7,2,27,1),(7,2,28,1),(7,2,29,1),(7,2,30,1),(7,2,31,1),(7,2,32,1),(7,2,33,4),(7,2,34,1),(7,2,35,1),(7,2,36,1),(7,2,37,1),(7,2,38,1),(7,2,39,1),(7,2,40,1),(7,2,44,4),(7,2,51,2),(7,2,55,5),(7,2,66,7),(7,2,77,7),(7,2,888,8)})
*/
top_n_channels = FOREACH records_grouped_by_anid3 {
    sorted_by_imps = ORDER anid_advid_campid_placid_chanid_imps BY imps desc;
    top_n = LIMIT sorted_by_imps $channel_id_limit;
    GENERATE group, flatten(top_n);
};
/*
top_n_channels: {
    group: chararray,
    top_n::group::ad_network_id: chararray,
    top_n::group::ext_advertiser_id: chararray,
    top_n::group::ext_channel_id: chararray,
    top_n::imps: long}

(7,7,2,3,38)
(7,7,2,1,38)
(7,7,2,9,38)
*/
-- filter by topN channels
ext_ids_lookup_join = JOIN top_camp_plac_imps BY (ad_network_id, ext_advertiser_id, ext_channel_id),
    top_n_channels BY (ad_network_id, ext_advertiser_id, ext_channel_id);
/*
ext_ids_lookup_join: {
    top_camp_plac_imps::ad_network_id: chararray,
    top_camp_plac_imps::ext_advertiser_id: chararray,
    top_camp_plac_imps::ext_campaign_id: chararray,
    top_camp_plac_imps::ext_publisher_id: chararray,
    top_camp_plac_imps::ext_channel_id: chararray,
    top_camp_plac_imps::ext_placement_id: chararray,
    top_camp_plac_imps::imps: long,
    top_n_channels::group: chararray,
    top_n_channels::top_n::group::ad_network_id: chararray,
    top_n_channels::top_n::group::ext_advertiser_id: chararray,
    top_n_channels::top_n::group::ext_channel_id: chararray,
    top_n_channels::top_n::imps: long}

(7,2,3,8,1,6,38,7,7,2,1,38)
(7,2,3,8,3,6,38,7,7,2,3,38)
(7,2,3,8,9,6,38,7,7,2,9,38)
*/

-- now we have topN in topN placements belonging to topN campaigns
ext_ids_lookup = FOREACH ext_ids_lookup_join GENERATE
    top_camp_plac_imps::ad_network_id as ad_network_id,
    top_camp_plac_imps::ext_advertiser_id as ext_advertiser_id,
    top_camp_plac_imps::ext_campaign_id as ext_campaign_id,
    top_camp_plac_imps::ext_publisher_id as ext_publisher_id,
    top_camp_plac_imps::ext_channel_id as ext_channel_id,
    top_camp_plac_imps::ext_placement_id as ext_placement_id,
    'null' as ext_plan_id;
/*
(7,2,3,8,1,6,null)
(7,2,3,8,3,6,null)
(7,2,3,8,9,6,null)
*/
ext_ids_lookup_ordered =  ORDER ext_ids_lookup BY ad_network_id, ext_advertiser_id, ext_campaign_id, ext_publisher_id, ext_placement_id, ext_channel_id;

store ext_ids_lookup_ordered into '$OUTDIR/ext_ids_lookup' using PigStorage('\t');
