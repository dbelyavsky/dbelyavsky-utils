#!/bin/bash

logFile=$1

declare -a PATTERNS=(\
   "Waiting on \w+ job" \
   "CPU time spent" \
   "Launched \w+ tasks" \
   "Job job_[0-9_]+ completed successfully" \
   "GC time elapsed" \
)

function resetOutputVars {
    launchTime=""
    url=""
    jobType=""
    launchedMapTasks=""
    launchedReduceTasks=""
    gsTimeElapsed=""
    cpuTimeSpent=""
}

function printOutputRecord {
    echo "${launchTime},${url},${jobType},${launchedMapTasks},${launchedReduceTasks},${gsTimeElapsed},${cpuTimeSpent}"
}

# Print Header
echo ""
echo "Launch Time,url,JobType,Launched map tasks,Launched reduce tasks,GC time elapsed (ms),CPU time spent (ms)"

FILTER_STRING=""
for pattern in "${PATTERNS[@]}"; do
    if [[ -z ${FILTER_STRING} ]] ; then
        FILTER_STRING="${pattern}"
    else
        FILTER_STRING="${FILTER_STRING}|${pattern}"
    fi
done 

stats=$(echo ""; echo "")
stats+=$(grep -E "${FILTER_STRING}" $logFile)

while read -r aLine
do
    case $aLine in
    *"Waiting on"*|*"completed successfully"* ) 
        printOutputRecord
        resetOutputVars
        appId=$(echo "${aLine}" | grep -Eo 'job[0-9A-Za-z_]+' | sed 's/job_/application_/g')
        url=$(grep "${appId}" ${logFile} | grep 'url to track')
        launchTime=$(echo ${url} | cut -d ' ' -f 1,2)
        url=$(echo ${url} | grep -Eo 'http.+$')
        jobType=$(echo $aLine | cut -d: -f3 | cut -d. -f2 | sed 's/Cli//i')
        ;;
    *"CPU time spent"*)
        cpuTimeSpent=$(echo $aLine | cut -d '=' -f 2)
        ;;
    *"Launched map tasks"* )
        launchedMapTasks=$(echo $aLine | cut -d '=' -f 2)
        ;;
    *"Launched reduce tasks"* )
        launchedReduceTasks=$(echo $aLine | cut -d '=' -f 2)
        ;;
    *"GC time elapsed"* )
        gsTimeElapsed=$(echo $aLine | cut -d '=' -f 2)
        ;;
    esac
done < <(echo "${stats}")
printOutputRecord
