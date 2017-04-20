#!/bin/sh

DATE='2016/12/04'

TS_DIR=${HOME}/src/etl-trafficscope-script

cd ${TS_DIR}

./bin/run.sh traffic_scope_script.py --config ${TS_DIR}/conf/dev/traffic_scope.json --date ${DATE} 
