#!/bin/bash

# Script for executing csvcleanup jar on hadoop cluster

declare -r BOLD="\033[1m"
declare -r RESET="\033[0m"
declare -r RED="\033[31m"
declare -r LIGHT_BLUE='\033[1;34m'
declare -r GREEN='\033[32m'

declare -r RUN_CONTROL_DATE_PATH='/tech/RUN_CONTROL_DATE.dat'
declare -r JAR_PATH='articleinfo-cleanup-1.0-SNAPSHOT-jar-with-dependencies.jar'
declare -r JAR_CLASS='ArticleInfoCleanUp'

function error() {
    echo -e "${RED}${BOLD}${*}${RESET}" >&2
}

function success() {
    echo -e "${GREEN}${*}${RESET}" >&1
}


function main(){
    hdfs dfs -test -e $RUN_CONTROL_DATE_PATH
    if [ $? != 0 ] ; then
        error "File with RUN_CONTROL_DATE doesn not exist"
    else
        RUN_CONTROL_DATE=`hdfs dfs -cat $RUN_CONTROL_DATE_PATH`
        success "Starting article-info.json Clean-up job..."
        spark-submit --class "${JAR_CLASS}" --master yarn --deploy-mode client ${JAR_PATH} ${RUN_CONTROL_DATE}
    fi
}

main
