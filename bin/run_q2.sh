#!/bin/bash

while getopts 'd:h:cfq:j:' opt; do
    case $opt in
    d)
        DATE=$OPTARG
        ;;
    h)
        HOUR=$OPTARG
        ;;
    c)
        COPY_DATA=true
        ;;
    f)
        COPY_FULL_DAY=true
        ;;
    q)
        QLOG=$OPTARG
        ;;
    j)
        JIRA=$OPTARG
        ;;
    *)
        echo "Usage: $0 [-d <DATE>] [-h <HOUR>] [-c] [-f] [-q <QLOG_PATH>] [-j <JIRA>]"
        ;;
    esac
done

#DATE=${DATE:-$(date '+%Y-%m-%d')}
DATE=${DATE:-2017-04-12}
HOUR=${HOUR:-12}
COPY_DATA=${COPY_DATA:-false}
COPY_FULL_DAY=${COPY_FULL_DAY:-false}
QLOG=${QLOG:-qlog}
JIRA=${JIRA:-NOJIRA}

PREVDATE=$(date --date "$DATE - 1 day" '+%Y-%m-%d')
NEXTDATE=$(date --date "$DATE + 1 day" '+%Y-%m-%d')

DATEPATH=$(date --date "$DATE" '+%Y/%m/%d')
PREVDATEPATH=$(date --date "$PREVDATE" '+%Y/%m/%d')
NEXTDATEPATH=$(date --date "$NEXTDATE" '+%Y/%m/%d')

DATESTAMP=$(date --date "$DATE" '+%Y%m%d')
PREVDATESTAMP=$(date --date "$PREVDATE" '+%Y%m%d')
NEXTDATESTAMP=$(date --date "$NEXTDATE" '+%Y%m%d')

#SRC_DATA_HOST="hdfs://p-hdpnn02.nj01.303net.pvt"

NOTIFY_LIST="${USER}@integralads.com"

DISTCP_CMD="hadoop distcp -D mapreduce.map.memory.mb=2024 -log /tmp/$0.distcp.${RANDOM}.log -m 600"

echo "Creatting required dirs"
for d in quality/progress/score/in_progress \
    quality/progress/distribute/archive \
    quality/progress/distribute/in_progress \
    quality/progress/distribute/todo \
    quality/progress/aggregate3/in_progress \
    quality/evidence/site \
    dt/todo 
do
    echo "    $d"
    hdfs dfs -mkdir -p $d
done

echo "Cleaning up any previous progress tracking"
hdfs dfs -rm -r -skipTrash -f \
	quality/progress/score/in_progress/* \
	quality/progress/distribute/archive/* \
	quality/progress/distribute/in_progress/* \
	quality/progress/distribute/todo/* \
	quality/progress/aggregate3/in_progress/*

# get the PLACEMENT data (this is static, only need to get it once)
echo "Refreshing quality/evidence/placement && quality/evidence/site_placement"
hdfs dfs -mkdir -p quality/evidence/placement quality/evidence/site_placement
hdfs dfs -cp -f /user/thresher/quality/evidence/placement/part-r-00000.gz quality/evidence/placement
hdfs dfs -cp -f /user/thresher/quality/evidence/site_placement/part-r-00000.gz quality/evidence/site_placement

# get the FRAUD EVIDENCE data
echo "Refreshing quality/evidence/fraud/${PREVDATEPATH}"
hdfs dfs -mkdir -p quality/evidence/fraud/${PREVDATEPATH}
hdfs dfs -cp -f ${SRC_DATA_HOST}/user/thresher/quality/evidence/fraud/${PREVDATEPATH}/* quality/evidence/fraud/${PREVDATEPATH}

# get the INTENDED DURATION data
echo "Refreshing video/intended_duration/${PREVDATEPATH} && video/intended_duration_cm/${PREVDATEPATH}"
hdfs dfs -mkdir -p video/intended_duration/${PREVDATEPATH} video/intended_duration_cm/${PREVDATEPATH}
hdfs dfs -cp -f ${SRC_DATA_HOST}/user/thresher/video/intended_duration/${PREVDATEPATH}/* video/intended_duration/${PREVDATEPATH}
hdfs dfs -cp -f ${SRC_DATA_HOST}/user/thresher/video/intended_duration_cm/${PREVDATEPATH}/* video/intended_duration_cm/${PREVDATEPATH}

[[ "${COPY_DATA}" == "true" ]] && {
    if [[ "${COPY_FULL_DAY}" == "true" ]]; 
    then
        echo "COPY_FULL_DAY=${COPY_FULL_DAY}"

        echo "STARTING COPY OF EVENT FOR $DATESTAMP"
        hdfs dfs -rm -f copy_files_list \
            && hdfs dfs -ls ${SRC_DATA_HOST}/user/thresher/event/${DATESTAMP}*/*${DATESTAMP}* \
                ${SRC_DATA_HOST}/user/thresher/event/${NEXTDATESTAMP}*/*${DATESTAMP}* \
                ${SRC_DATA_HOST}/user/thresher/event/${PREVDATESTAMP}23*/*${PREVDATESTAMP}23* \
            | grep -v ^Found | rev | cut -d'/' -f2 | rev | sort | uniq | awk "{print \"${SRC_DATA_HOST}/user/thresher/event/\"\$1}" \
            | hdfs dfs -put - copy_files_list \
            && echo "Found source dirs" && hdfs dfs -text copy_files_list | sed 's/^/    /g' \
            && ${DISTCP_CMD} -f copy_files_list event/

        echo "FINISHED COPY OF EVENT FOR $DATESTAMP"

        echo "STARTING COPY OF DT/RAW FOR $DATESTAMP"
        hdfs dfs -rm -f copy_files_list \
            && hdfs dfs -ls ${SRC_DATA_HOST}/user/thresher/dt/raw/${DATESTAMP}*/*${DATESTAMP}* \
                ${SRC_DATA_HOST}/user/thresher/dt/raw/${NEXTDATESTAMP}*/*${DATESTAMP}* \
                ${SRC_DATA_HOST}/user/thresher/dt/raw/${PREVDATESTAMP}23*/*${PREVDATESTAMP}23* \
            | grep -v ^Found | rev | cut -d'/' -f2 | rev | sort | uniq | uniq | awk "{print \"${SRC_DATA_HOST}/user/thresher/dt/raw/\"\$1}" \
            | hdfs dfs -put - copy_files_list \
            && echo "Found source dirs" && hdfs dfs -text copy_files_list | sed 's/^/    /g' \
            && ${DISTCP_CMD} -f copy_files_list dt/raw/

        echo "FINISHED COPY OF DT/RAW FOR $DATESTAMP"
    else
        echo "COPY_FULL_DAY=${COPY_FULL_DAY}"

        # get the DT data
        echo "STARTING COPY OF DT/RAW FOR $DATESTAMP, HOUR:${HOUR:-00}"
        hdfs dfs -mkdir -p dt/raw/${DATESTAMP}${HOUR:-00}0000
        hdfs dfs -rm -r dt/raw/${DATESTAMP}${HOUR:-00}0000
        ${DISTCP_CMD} ${SRC_DATA_HOST}/user/thresher/dt/raw/${DATESTAMP}*/*${DATESTAMP}${HOUR}* dt/raw/${DATESTAMP}${HOUR:-00}0000
        echo "FINISHED COPY OF DT/RAW FOR $DATESTAMP"

        echo "STARTING COPY OF EVENT FOR $DATESTAMP, HOUR:${HOUR:-00}"
        # get the FW data
        hdfs dfs -mkdir -p event/${DATESTAMP}${HOUR:-00}0000
        hdfs dfs -rm -r event/${DATESTAMP}${HOUR:-00}0000
        ${DISTCP_CMD} ${SRC_DATA_HOST}/user/thresher/event/${DATESTAMP}*/*${DATESTAMP}${HOUR}* event/${DATESTAMP}${HOUR:-00}0000

        echo "FINISHED COPY OF EVENT FOR $DATESTAMP"
    fi
}

#### RUN Q2 !!!!!!!
CMD="${HOME}/${QLOG}/bin/Q2.sh --force --date=$DATE --skip-dsagg  --skip-verify "
echo "${CMD}"
${CMD}
#### FINISHED Q2 !!!!!!

# backup the log file
LOGFILE=${HOME}/log/Q2_$(date +%Y%m).log
LOGFILE_NEW=${LOGFILE}.${JIRA}
[[ -f ${LOGFILE_NEW} ]] && {
    mv ${LOGFILE_NEW} ${LOGFILE_NEW}.$(date +%Y%m%d%H%M%S)
}
mv ${LOGFILE} ${LOGFILE_NEW}

# send completion notification with some stats
mail -s "Q2 completed under ${USER}@${HOSTNAME}, for ${JIRA}" ${NOTIFY_LIST} <<-EOF
    * Q2 completed under ${USER}@${HOSTNAME} 

    Tail of the log file bellow:

    $(tail ${LOGFILE_NEW})

    $(~/getStats.sh ${LOGFILE_NEW})

    script: ${0}
EOF
