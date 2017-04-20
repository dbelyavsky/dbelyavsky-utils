#!/bin/bash

DATE="2016/11/09"
RUNTIME=ib03

#MYSQL_HOST="p-etl01.nj01.303net.pvt"
#MYSQL_PORT="5029"
#MYSQL_USER="etlstage" 
#MYSQL_PASSWORD="etL5tag3@#"
#MYSQL_DB="analytics"
#DB_CONNECTION="mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT}  -u ${MYSQL_USER}  -p${MYSQL_PASSWORD} ${MYSQL_DB}"

${HOME}/ib_dataload/bin/ib_dataload.sh --runtime=${RUNTIME} --date=${DATE} --process-all-input --infobright-connection-string="${DB_CONNECTION}"
