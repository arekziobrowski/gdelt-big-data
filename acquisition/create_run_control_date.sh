#!/bin/bash

# Script for generating a RUN_CONTROL_DATE tech file.

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
    echo -e "${BOLD}Usage${RESET}: ./create_run_control_date.sh"
}

function main() {
    hdfs dfs -test -e $RUN_CONTROL_DATE_PATH
    if [ $? != 0 ] ; then
        echo "[$(date)] Creating $RUN_CONTROL_DATE_PATH" >&2
        printf "$(date '+%Y%m%d' -d 'yesterday')" | hdfs dfs -appendToFile - $RUN_CONTROL_DATE_PATH
    else
        hdfs dfs -rm $RUN_CONTROL_DATE_PATH
        printf "$(date '+%Y%m%d' -d 'yesterday')" | hdfs dfs -appendToFile - $RUN_CONTROL_DATE_PATH
    fi
    success "[$(date)] Created $RUN_CONTROL_DATE_PATH!"
}

while getopts ":h" OPT; do
    case ${OPT} in
        h)
            help
            exit 0
        ;;
        \?)
            error "Invalid option: -${OPTARG}"
            exit 1
        ;;
    esac
done

main
