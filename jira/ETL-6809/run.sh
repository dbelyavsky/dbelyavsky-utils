#!/bin/bash

# prod running on p-etl16
# [20171108--13:30:09] (2567) Command: hadoop jar ../lib/pm-jas.jar com/integralads/etl/partnermeasured/jas_generator/mapreduce/JasGeneratorMRLauncher "-Dmapreduce.compress.map.output=true" "-Dmapreduce.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec" "-Dmapreduce.output.compress=true" "-Dmapreduce.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec" "-Dmapreduce.output.fileoutputformat.compress=true" "-Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec" --date=20171108 --start-time=1000 --end-time=1100 --input-dirs=/user/thresher/partner_measured/raw_logs/youtube/archive/2017/11/08/09/30,/user/thresher/partner_measured/raw_logs/youtube/archive/2017/11/08/10/00,/user/thresher/partner_measured/raw_logs/youtube/archive/2017/11/08/10/30,/user/thresher/partner_measured/raw_logs/youtube/archive/2017/11/08/11/00,/user/thresher/partner_measured/raw_logs/youtube/archive/2017/11/08/11/30 --output-dir=/user/thresher/partner_measured/jas_logs/youtube --local-confdir=/home/thresher/pm-jas-controller-youtube/conf/prod

ARGS="  -Dmapreduce.compress.map.output=true" 
ARGS+=" -Dmapreduce.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec" 
ARGS+=" -Dmapreduce.output.compress=true" 
ARGS+=" -Dmapreduce.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec" 
ARGS+=" -Dmapreduce.output.fileoutputformat.compress=true" 
ARGS+=" -Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec"

JAR=${HOME}/src/etl-pm-jas/target/pm-jas-0.0.89.jar
DATE=20171108
STARTTIME=1000
ENDTIME=1100
DATE_PATH=$(date --date ${DATE} +%Y/%m/%d)
BASE_INPUT_DIR="/user/thresher/partner_measured/raw_logs/youtube/archive/${DATE_PATH}"
INPUT_DIRS="${BASE_INPUT_DIR}/09/30,${BASE_INPUT_DIR}/10/00,${BASE_INPUT_DIR}/10/30,${BASE_INPUT_DIR}/11/00,${BASE_INPUT_DIR}/11/30"
OUTPUT_DIR="partner_measured/jas_logs/youtube"
CONFDIR="/home/dbelyavsky/src/etl-pm-jas-controller/runtime/youtube/conf/dev"

hdfs dfs -mkdir -p ${OUTPUT_DIR}

hadoop jar ${JAR} com/integralads/etl/partnermeasured/jas_generator/mapreduce/JasGeneratorMRLauncher \
    ${ARGS} \
    --date=${DATE} --start-time=${STARTTIME} --end-time=${ENDTIME} \
    --input-dirs=${INPUT_DIRS} \
    --output-dir=${OUTPUT_DIR} \
    --local-confdir=${CONFDIR}
