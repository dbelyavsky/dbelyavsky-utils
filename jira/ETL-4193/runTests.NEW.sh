#!/bin/bash

export PROCESS_BASEDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd) 

source ./common/bin/pipeline_client_stub.sh
source ./common/bin/common_source.sh
source ./aggregation_helper/bin/mysql_helper.NEW.sh

export LOGFILE=log.dat

DATE=$1

[[ -z $DATE ]] && {
	echo "Usage: $0 <YYYY/MM/DD>"
	exit 1
}

YEAR=`echo $DATE | awk -F '/' '{print $1}'`
MONTH=`echo $DATE | awk -F '/' '{print $2}'`
DAY=`echo $DATE | awk -F '/' '{print $3}'`

[[ -z $YEAR || -z $MONTH || -z $DAY ]] && {
	echo "Date $DATE is not valid, should be 'YYYY/MM/DD'"
	exit 1
}

#HDFS_PATH="/user/thresher/mart/agency/$YEAR/$MONTH/$DAY/*/agg_agency_action/part*"
HDFS_PATH="/user/thresher/mart/network/$YEAR/$MONTH/$DAY/*/agg_network_iab_v2/part*"
LOCAL_TMPDIR="/tmp"
EXPORT_FILE_NAME="etl4193.fifo"
#TABLE_NAME="dbelyavsky_AGG_AGENCY_ACTION"
TABLE_NAME="dbelyavsky_AGG_NETWORK_IAB_V2"
EXPORT_COLUMNS=""
#INFOBRIGHT_CONNECTION_STRING="mysql -hdb-stage.nj01.303net.pvt -P5029 -uthresher -pthr3sh3r test"
INFOBRIGHT_CONNECTION_STRING="mysql -hinfobright04.nj01.303net.pvt -P5029 -uthresher -pthr3sh3r test"
UTF8_FL=""
alert_on_error=""

echo exportToMysqlOverwrite "${HDFS_PATH}" "$LOCAL_TMPDIR/${EXPORT_FILE_NAME}" "${TABLE_NAME}" "${EXPORT_COLUMNS}" "$INFOBRIGHT_CONNECTION_STRING" "${UTF8_FL}" "${alert_on_error}"
ret_code=$?

#log "completed with ret_code[$ret_code]"
