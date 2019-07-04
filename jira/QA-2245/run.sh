#!/bin/bash

servers="app45dal app46dal app47dal app48dal app49dal app50dal app51dal app52dal"

day_date="$(date +'%Y/%m/%d')"

#fetching the last 2 batches from events and the todo dir
dirs_to_check="$(hdfs dfs -ls -d /user/thresher/event/${today_date}/*/*/ /user/thresher/fw_log/todo /user/thresher/misc/pubmatic_fw_log/todo | tail -n4 | rev | cut -d' ' -f1 | rev | tr '\n' ' ')"

# fetching list of recently collected files in hdfs
files_collected="$(hdfs dfs -ls ${dirs_to_check})"
echo "checking on the following dirs: ${dirs_to_check}"
for server in ${servers}; do
    # short hostname of the server that would be in the log files name
    server_short=$(echo $server | cut -d'.' -f1)
    echo "files collected from ${server} : $(echo "${files_collected}" | grep "${server_short}\." | wc -l)"
    ## echo "${files_collected}" | grep "${server_short}\."
done
