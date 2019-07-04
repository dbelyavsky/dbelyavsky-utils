-- BRANCH ETL-5310
%default ext_ids_lookup 'ETL-5310/ext_ids_lookup'
%default OUTDIR 'ETL-5310.verify'
----- PROD
-- %default ext_ids_lookup '/user/thresher/mart/network/2017/06/19/20170620004501/ext_ids_lookup'
-- %default OUTDIR 'ETL-5310.verify.PROD'

rmf $OUTDIR

data = LOAD '$ext_ids_lookup/part*' USING PigStorage('\t') AS (
	anid:chararray,
    advid:chararray,
    cmid:chararray,
    pubid:chararray,
    chid:chararray,
    placid:chararray,
    planid:long
);

data_groupped = GROUP data BY (anid, advid, chid);
unique_anid_chid = FOREACH data_groupped GENERATE FLATTEN(group);
groupped_by_anid = GROUP unique_anid_chid by (anid, advid);
count_chid_per_anid = FOREACH groupped_by_anid GENERATE FLATTEN(group), COUNT(unique_anid_chid) as cnt;
only_over_1000 = FILTER count_chid_per_anid BY cnt >= 1000;

STORE only_over_1000 INTO '$OUTDIR' USING PigStorage('\t');
