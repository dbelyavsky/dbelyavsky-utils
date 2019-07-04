-- pig -f ~/src/etlscripts/ias_mart/pig/ext_ids_lookup.pig -param EXT_IDS_DIR="/user/thresher/ad_network_ext_ids" -param ext_ids_min_imp_threshold=100 -param camps_per_anid=500 -param placs_per_anid=1500 -param filter_anids="502|5425|8087|7522|7314|10001|10002|7443|8059|8265|8004|9937|8087|922334|923831|7708|925172" -param OUTDIR=ETL-5310

%default EXT_IDS_DIR '/user/thresher/ad_network_ext_ids'
%default filter_anids '502|5425|8087|7522|7314|10001|10002|7443|8059|8265|8004|9937|8087|922334|923831|7708|925172'
%default OUTDIR 'ETL-5310'
rmf $OUTDIR
-- ----------------------------------
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

-- prepare topN channelIDs bag
anid_advid_campid_placid_chanid_imps = FOREACH (
        GROUP top_camp_plac_imps by ( ad_network_id, ext_advertiser_id, ext_channel_id)
    ) GENERATE
        FLATTEN(group),
        SUM(top_camp_plac_imps.imps) as imps;


/*
groupped_by_channelid: {
    group::ad_network_id: chararray,
    group::ext_advertiser_id: chararray,
    group::ext_channel_id: chararray,
    imps: long}
*/

records_grouped_by_anid3 = GROUP anid_advid_campid_placid_chanid_imps BY (ad_network_id);

/*
records_grouped_by_anid3: {
    group: chararray,
    anid_advid_campid_placid_chanid_imps: {(
        group::ad_network_id: chararray,
        group::ext_advertiser_id: chararray,
        group::ext_channel_id: chararray,
        imps: long
        )
    }
}
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
    top_n::imps: long
}
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
    top_n_channels::group::ad_network_id: chararray,
    top_n_channels::group::ext_advertiser_id: chararray,
    top_n_channels::group::ext_channel_id: chararray,
    top_n_channels::imps: long}
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

ext_ids_lookup_ordered =  ORDER ext_ids_lookup BY ad_network_id, ext_advertiser_id, ext_campaign_id, ext_publisher_id, ext_placement_id, ext_channel_id;

store ext_ids_lookup_ordered into '$OUTDIR/ext_ids_lookup' using PigStorage('\t');

/*
BEFORE
    grunt> data2 = load '/user/thresher/mart/network/2017/06/19/20170620004501/ext_ids_lookup/*' using PigStorage('\t');
    grunt> groupped2 = group data2 all;
    grunt> counted2 = foreach groupped2 generate COUNT(data2);
    grunt> dump counted2
    (943,611,924)

AFTER
    grunt> data = load 'ETL-5310/ext_ids_lookup/*' using PigStorage('\t');
    grunt> groupped_data = group data all;
    grunt> counted_data = foreach groupped_data generate COUNT(data);
    grunt> dump counted_data
    (207,098,466)
*/
