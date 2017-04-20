#!/bin/bash

echo "$(date) Q2 driver script [$0]" | mail -s "Q2 driver starting BRANCH" dbelyavsky@integralads.com
${HOME}/run_q2.sh -d '2017-04-16' -q 'qlog-STAG-381-SNAPSHOT' -j 'SAD-2762'
hdfs dfs -mv quality quality.SAD-2762.BRANCH1

#echo "$(date) Q2 driver script [$0]" | mail -s "Q2 driver starting PROD" dbelyavsky@integralads.com
#${HOME}/run_q2.sh -d '2017-04-16' -q 'qlog-1.0.126-NEXUS' -j 'SAD-2762'
#hdfs dfs -mv quality quality.SAD-2762.PROD1

echo "$(date) Q2 driver script [$0]" | mail -s "Q2 driver finished" dbelyavsky@integralads.com
