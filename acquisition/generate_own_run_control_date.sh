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
    echo -e "${BOLD}Create dummy data. ${RESET}"
    echo -e "${BOLD}Usage${RESET}: ./generate_dummy.sh"
}

function main() {
    hdfs dfs -test -e $RUN_CONTROL_DATE_PATH
    if [ $? != 0 ] ; then
        echo "[$(date)] Creating $RUN_CONTROL_DATE_PATH" >&2
        printf "$INPUT_DATE" | hdfs dfs -appendToFile - $RUN_CONTROL_DATE_PATH
    else
        hdfs dfs -rm $RUN_CONTROL_DATE_PATH
        printf "$INPUT_DATE" | hdfs dfs -appendToFile - $RUN_CONTROL_DATE_PATH
    fi
    success "[$(date)] Created $RUN_CONTROL_DATE_PATH!"
}

while getopts ":hd:" OPT; do
    case ${OPT} in
        h)
            help
            exit 0
        ;;
        d)
            INPUT_DATE=${OPTARG}
        ;;
        \?)
            error "Invalid option: -${OPTARG}"
            exit 1
        ;;
    esac
done

if [ -z $INPUT_DATE ] ; then
    error "No input date. Use -d flag."
    exit 1
fi

main
