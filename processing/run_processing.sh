#!/bin/bash

declare -r BOLD="\033[1m"
declare -r RESET="\033[0m"
declare -r RED="\033[31m"
declare -r LIGHT_BLUE='\033[1;34m'
declare -r GREEN='\033[32m'

declare -r RUN_CONTROL_DATE_PATH='/tech/RUN_CONTROL_DATE.dat'

function error() {
    echo -e "${RED}${BOLD}${*}${RESET}" >&2
}

function success() {
    echo -e "${GREEN}${*}${RESET}" >&1
}

function help() {
    echo -e "${BOLD}Create RUN_CONTROL_DATE tech file. ${RESET}"
    echo -e "${BOLD}Usage${RESET}: ./run_processing.sh [csv|json|rake] [PATH TO JAR]"
}

function main {
    hdfs dfs -test -e $RUN_CONTROL_DATE_PATH
    if [ $? != 0 ] ; then
        error "File with RUN_CONTROL_DATE doesn not exist"
        exit 1
    fi
    RUN_CONTROL_DATE=`hdfs dfs -cat $RUN_CONTROL_DATE_PATH`
    case $1 in
        csv)
            success "Starting csv clean-up map reduce job..."
            hadoop jar $2 CsvCleanUp ${RUN_CONTROL_DATE}
        ;;
        json)
            success "Starting article-info.json Clean-up job..."
            spark-submit --class "ArticleInfoCleanUp" --master yarn --deploy-mode client $2 ${RUN_CONTROL_DATE}
        ;;
        rake)
            success "Starting RAKE processing job..."
            spark-submit --class "RakeKeyWordsProcessing" --master yarn --deploy-mode client $2 ${RUN_CONTROL_DATE}
        ;;
        country)
            success "Starting country mapping job..."
            spark-submit --class "CountryMapper" --master yarn --deploy-mode client $2 ${RUN_CONTROL_DATE}
        ;;
    esac
}

if [ $# -lt 2 ]
then
    help
    exit 0
fi
if ! [[ $1 =~ ^(csv|json|rake|country)$ ]]
then
    help
    exit 0
fi

main $1 $2