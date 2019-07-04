#!/bin/sh

DATE="2018/10/13"
JOBID="20181014000000"

# Edit ias_mart/conf/dev/mart.conf to point to PROD data
# MART_HDP_INPUT_HOME_DIR="/user/thresher"
#sed -i 's/MART_HDP_INPUT_HOME_DIR=\"\/user\/\${USER}"/MART_HDP_INPUT_HOME_DIR=\"\/user\/thresher\"/' ~ias_mart/conf/dev/mart.conf

# Ensure target path exists locally
hdfs dfs -mkdir -p mart/{agency,network}/${DATE}

# copy all previously generated MART data for the date
hadoop distcp -log /tmp/distcp.${RANDOM}.log -m 400 /user/thresher/mart/agency/${DATE}/* mart/agency/${DATE}/
hadoop distcp -log /tmp/distcp.${RANDOM}.log -m 400 /user/thresher/mart/network/${DATE}/* mart/network/${DATE}/

# locally remove the report output that is being tested
hdfs dfs -rm -r -skipTrash mart/agency/${DATE}/${JOBID}/agg_agency_custom*

# run the driver script
~/ias_mart/bin/agency_mart_platform2.sh --date=${DATE} --jobid=${JOBID}
