#!/bin/bash

# Script for unloading a table from the database.

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
    echo -e "${GREEN}${*}${RESET}"
}

function help() {
    echo -e "${BOLD}Unload a table from the database. ${RESET}"
    echo -e "${BOLD}Usage${RESET}: ./create_run_control_date.sh"
    echo -e "${BOLD}\t-t${RESET}\t table name"
}

# Unloads a table from database
# Params:
#   - $1 - table name
function unload() {
    if [ -z $OUTPUT_PATH ] ; then
        error "No output path set!"
        exit 1
    fi
    sqoop import \
        --connect jdbc:mysql://maria-gdelt:3306/bd_gdelt \
        --username=root \
        --password=root \
        --table=$1 \
        --as-avrodatafile \
        --delete-target-dir \
        --target-dir=$OUTPUT_PATH/$1

    success "[$(date)] Unloaded $OUTPUT_PATH/$1" >&2
}

function main() {
    hdfs dfs -test -e $RUN_CONTROL_DATE_PATH
    if [ $? != 0 ] ; then
        error "[$(date)] No RUN_CONTROL_DATE.dat found!"
        exit 1
    fi

    declare -r RUN_CONTROL_DATE=$(hdfs dfs -cat $RUN_CONTROL_DATE_PATH)
    declare -r OUTPUT_PATH="/data/db/$RUN_CONTROL_DATE"

    hdfs dfs -test -d $OUTPUT_PATH
    if [ $? != 0 ] ; then
        echo "[$(date)] Creating $OUTPUT_PATH" >&2
        hdfs dfs -mkdir $OUTPUT_PATH
    fi

    unload 'color_metadata'
}

while getopts ":ht:" OPT; do
    case ${OPT} in
        h)
            help
            exit 0
        ;;
        t)
            TABLE_TO_UNLOAD=${OPTARG}
        ;;
        \?)
            error "Invalid option: -${OPTARG}"
            exit 1
        ;;
    esac
done

if [ -z $TABLE_TO_UNLOAD ] ; then
    error "Please specify table to unload by using -t flag."
    exit 1
fi

main

