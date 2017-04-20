FORDATE="2016/11/09"
NAKED_DATE=$(echo "${FORDATE}" |tr -d '/' )
NAKED_DATETIME="${NAKED_DATE}000000"
THIS_DATE=$(date +%Y%m%d%H%M%S)
#DT="${THIS_DATE}"
DT=`date +"%Y%m%d_%H%M"`
#TAG="$(git rev-parse --abbrev-ref HEAD)${THIS_DATE}"
#TAG="$(echo $TAG|tr -d '-')"
TAG=NEW
PREMART="quality.${TAG}/scores/aggdt3/${FORDATE}/*/*"
OUTDIR="mart/agency/${FORDATE}/${NAKED_DATETIME}"
LOOKUPDIR="${OUTDIR}"

OUTDIR_SANITY_ID="sanity_${DT}"

#MYSQL_HOST="db-stage.nj01.303net.pvt"
#MYSQL_PORT="5029"
#MYSQL_USER="thresher"
#MYSQL_PASSWORD="thr3sh3r"
#MYSQL_DB="analytics"

MYSQL_HOST="p-etl01.nj01.303net.pvt"
MYSQL_PORT="5029"
MYSQL_USER="etlstage" 
MYSQL_PASSWORD="etL5tag3@#"
MYSQL_DB="analytics"

FINAL="${OUTDIR}/${TAG}"
BACKUP="day_marting_bak_${THIS_DATE}"

PIG_PARAMS="-p srbi1=0.17 -p srbi2=0.16 -p srbi3=0.15 -p sris1=0.154 -p sris2=0.11 -p sris3=0.125 -p srpb1=0.1 -p srpb2=0.16 -p srpb3=0.1 -p srei1=0.184 -p srei2=0.183 -p srei3=0.148 -p srvi1=0 -p srvi2=0.8 -p srvi3=1.0"

function mart() {
    hdfs dfs -mkdir -p ${BACKUP}
    hdfs dfs -mv "${OUTDIR}/agg_agency_quality_site_pre_V3" "${BACKUP}/"
    hdfs dfs -mv "${OUTDIR}/agg_agency_quality_pre_V3" "${BACKUP}/"
    hdfs dfs -mv "${OUTDIR}/agg_agency_quality_site_V3" "${BACKUP}/"
    hdfs dfs -mv "${OUTDIR}/agg_agency_quality_V3" "${BACKUP}/"
    hdfs dfs -mv "${OUTDIR}/agg_agency_quality_placement_billable_V3" "${BACKUP}/"

    hdfs dfs -rm -r "${OUTDIR}/agg_agency_quality_site_pre*"
    hdfs dfs -rm -r "${OUTDIR}/agg_agency_quality_pre*"

    pig -D mapred.job.priority=HIGH -p qlogs_premart="$PREMART" \
        -l "${HOME}/log/agg_agency_quality_v3.${DT}.log"  \
        -p PARALLEL=50 \
        -p LOOKUPDIR="$LOOKUPDIR" \
        -p OUTDIR="$OUTDIR" \
        -p BASEDIR="${HOME}/ias_mart" \
        -f ${HOME}/ias_mart/pig/agg_agency_quality_v3.pig ${PIG_PARAMS}

    if [ $? -ne 0 ]; then exit 1 ; fi
    echo "${THIS_DATE} pig over 1"

    hdfs dfs -rm -r "${OUTDIR}/agg_agency_quality_site_V3"
    hdfs dfs -rm -r "${OUTDIR}/agg_agency_quality_placement_billable_V3"
    pig -D mapred.job.priority=HIGH \
        -p qlogs_premart="$PREMART" \
        -p site_placement_scale_factors=scale_factors_for_agency/${FORDATE}/site_placement   \
        -p site_scale_factors=scale_factors_for_agency/${FORDATE}/site  \
        -p scale_factors=scale_factors/${FORDATE}/groupm_scale_factors \
        -l "${HOME}/log/agg_agency_quality_site_join_v3.${DT}.log" \
        -p PARALLEL=50 \
        -p LOOKUPDIR="$LOOKUPDIR" \
        -p OUTDIR="$OUTDIR"  \
        -p BASEDIR="${HOME}/ias_mart"   \
        -f ${HOME}/ias_mart/pig/agg_agency_quality_site_join_v3.pig ${PIG_PARAMS}
    if [ $? -ne 0 ]; then exit 1 ; fi
    echo "${THIS_DATE} pig over 2"

    hdfs dfs -rm -r "${OUTDIR}/agg_agency_quality_V3"
    pig -D mapred.job.priority=HIGH   \
        -p qlogs_premart="$PREMART"  \
        -l "${HOME}/log/agg_agency_quality_join_v3.${DT}.log" \
        -p PARALLEL=50 -p LOOKUPDIR="$LOOKUPDIR" \
        -p OUTDIR="$OUTDIR"  \
        -p BASEDIR="${HOME}/ias_mart"   \
        -f ${HOME}/ias_mart/pig/agg_agency_quality_join_v3.pig ${PIG_PARAMS}
    if [ $? -ne 0 ]; then exit 1 ; fi
    echo "${THIS_DATE} pig over 3"

    echo "Moving data to ${FINAL}"
    hdfs dfs -mkdir  -p "${FINAL}"
    hdfs dfs -mv "${OUTDIR}/agg_agency_quality_site_pre_V3" "${FINAL}/"
    hdfs dfs -mv "${OUTDIR}/agg_agency_quality_pre_V3" "${FINAL}/"
    hdfs dfs -mv "${OUTDIR}/agg_agency_quality_site_V3" "${FINAL}/"
    hdfs dfs -mv "${OUTDIR}/agg_agency_quality_V3" "${FINAL}/"
    hdfs dfs -mv "${OUTDIR}/agg_agency_quality_placement_billable_V3" "${FINAL}/"
}

function metadata() {
    for table in agg_agency_quality_V3 agg_agency_quality_site_V3; do 
        NEWTABLE="dbelyavsky_${TAG}_${table}"
        echo "------------------------------------------------------------------"
        echo "Creating table ${NEWTABLE} on ${MYSQL}"
        echo "echo  'create table ${NEWTABLE} as select * from ${table^^} LIMIT 0'  | mysql -h ${MYSQL_HOST}  -P ${MYSQL_PORT}  -u ${MYSQL_USER}  -p ${MYSQL_PASSWORD}   ${MYSQL_DB} "
        echo "------------------------------------------------------------------"
        echo "create table ${NEWTABLE} as select * from ${table^^} LIMIT 0" | mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DB}"
        METADATA_FILE="${HOME}/ib03dataload/todo/${NAKED_DATE}.${NAKED_DATE}${THIS_DATE}.ui.${NEWTABLE}"
        echo "Creating Metadata file: ${METADATA_FILE}"
        HDFS_LOC="${FINAL}/${table}/part*"
        echo "${HDFS_LOC}" > "${METADATA_FILE}"
        echo "hdfs_location:${HDFS_LOC}" >> "${METADATA_FILE}"
        echo "creation_time:$(date '+%Y%m%d %H:%M:%S')" >> "${METADATA_FILE}"
        echo "load_attempt:1" >> "${METADATA_FILE}"
        echo "Created Metadata file: ${METADATA_FILE}"
        cat "${METADATA_FILE}"
    done
}

function get_lookups() {
    hdfs dfs -mkdir -p  ${OUTDIR}/adv_join
    hadoop distcp -D mapreduce.map.memory.mb=2024 -log /tmp/${RANDOM}.log -m 200 \
        /user/thresher/mart/agency/${FORDATE}/*/adv_join/*  \
        ${OUTDIR}/adv_join

    hdfs dfs -mkdir -p ${OUTDIR}/pub_plac_join
    hadoop distcp -D mapreduce.map.memory.mb=2024 -log /tmp/${RANDOM}.log -m 200 \
        /user/thresher/mart/agency/${FORDATE}/*/pub_plac_join/* \
        ${OUTDIR}/pub_plac_join

    hdfs dfs -mkdir -p ${OUTDIR}/lookup.placement
    hadoop distcp -D mapreduce.map.memory.mb=2024 -log /tmp/${RANDOM}.log -m 200 \
        /user/thresher/mart/agency/${FORDATE}/*/lookup.placement/* \
        ${OUTDIR}/lookup.placement

    hdfs dfs -mkdir -p ${OUTDIR}/roadblock_lookup
    hadoop distcp -D mapreduce.map.memory.mb=2024 -log /tmp/${RANDOM}.log -m 200 \
        /user/thresher/mart/agency/${FORDATE}/*/roadblock_lookup/* \
        ${OUTDIR}/roadblock_lookup

    hadoop distcp -D mapreduce.map.memory.mb=2024 -log /tmp/${RANDOM}.log -m 200 \
        /user/thresher/scale_factors/${FORDATE}/groupm_scale_factors scale_factors/${FORDATE}


    hdfs dfs -mkdir -p ${OUTDIR}/agg_agency_rsa_V2
    hadoop distcp -D mapreduce.map.memory.mb=2024 -log /tmp/${RANDOM}.log -m 200 \
        /user/thresher/mart/agency/${FORDATE}/*/agg_agency_rsa_V2/* \
        ${OUTDIR}/agg_agency_rsa_V2

}

#time get_lookups()
#INITIAL="$(date)"
#time mart
#END="$(date)"
time metadata
#echo "TIMINGS $(git rev-parse --abbrev-ref HEAD) ${THIS_DATE} ${INITIAL} ${END}”
