#!/bin/bash

remote_files="TESTING-NO-FILE-LIST"
date_now=$(date)
ETL_ENVIRONMENT="prod"
IAB_BOT_NOTIFICATION_LIST="dbelyavsky@integralads.com"
PROCESS_BASEDIR=$HOME/watchdog""
WOODHOUSE_DOMAIN="woodhouse.303net.pvt"
JIRA_HANDLER_MAIL="jira-sad@integralads.com"

function get_email_body() {
    echo "Remote Files: [$1]"
}

# extracted from etlscripts/watchdog/bin/check_iab_bot_file_update.sh

email_body=$(get_email_body "${remote_files}")
SUMMARY="New IAB bots & valid browsers file has been released on ${date_now}"
echo "${email_body}" | mutt -s "$SUMMARY" -- ${IAB_BOT_NOTIFICATION_LIST}
etl_environment=${ETL_ENVIRONMENT:-$(get_etl_environment)}
if [[ "${etl_environment}" = "prod" ]]; then
    descr=$(echo "${email_body}" | python ${PROCESS_BASEDIR}/bin/to_json.py)
    tmpfile=$(mktemp)
    if [[ -f "${tmpfile}" ]] ; then
        echo '{"summary":"'${SUMMARY}'", "desc":'${descr}', "board":"SAD", "label":"etl-triage", "reporter":"dhanush"}' >$tmpfile
        #log "INFO: create_json=$(cat $tmpfile)"
        #log "INFO: WOODHOUSE_DOMAIN=$WOODHOUSE_DOMAIN"
        #log "curl -X POST -d @${tmpfile} -H \"Content-Type: application/json\" http://${WOODHOUSE_DOMAIN}/jira/create"
        curl -X POST \
        -d @${tmpfile} \
        -H "Content-Type: application/json" \
        http://${WOODHOUSE_DOMAIN}/jira/create
        EXIT_CODE=$?
        if [[ ${EXIT_CODE} -gt 0 ]] ; then
            ##checkError 1 WARNING "curl completed with exit code ${EXIT_CODE}. Therefore, creation of Jira card via email."
            echo "${email_body}" | mutt -s "$SUMMARY" -- ${JIRA_HANDLER_MAIL}
        fi
        rm -f ${tmpfile}
    fi

    #checkError 1 WARNING "Can't create tmpfile for json. Therefore, creation of Jira card via email."
    echo "${email_body}" | mutt -s "$SUMMARY" -- ${JIRA_HANDLER_MAIL}
fi