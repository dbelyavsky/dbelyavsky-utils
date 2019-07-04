#!/bin/sh

hdfs_base='/user/thresher/quality/logs'
jira='SAD-5561'
year='2019'
month='04'

pig_script="${jira}.pig"

available_days=$(hdfs dfs -ls -d ${hdfs_base}/${year}/${month}/* | awk '{print $8}' | cut -d'/' -f 8)

hdfs dfs -mkdir -p status/${jira}

for day in ${available_days}; do
    for hour in $(seq -w 0 23); do
        echo "---------------------------------------------------------------------"
        outfile="${jira}/${year}/${month}/${day}/${hour}/part*"
        hdfs dfs -test -f ${outfile}
        [[ $? == 0 ]] && {
            echo "$(date) Output file [${outfile}] already exists, skipping"
        }  || {
            echo "$(date) Now will run for ${month}/${day}, hour:${hour}"
            pig -f ${pig_script} -param HDFS_BASE=${hdfs_base} -param JIRA=${jira} -param YEAR=${year} -param MONTH=${month} -param DAY=${day} -param HOUR=${hour}
        }
    done
done

# generate the final results
#pig -f ${jira}-final.pig
