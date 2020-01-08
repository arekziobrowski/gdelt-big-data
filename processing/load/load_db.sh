#!/bin/bash

# Script for loading a table to the database.

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
    echo -e "${BOLD}Load a table to the database. ${RESET}"
    echo -e "${BOLD}Usage${RESET}: ./load_db.sh"
    echo -e "${BOLD}\t-t${RESET}\t table name"
    echo -e "${BOLD}\t-f${RESET}\t hdfs file to be loaded"
}

# Loads a table to the database
# Params:
#   - $1 - file to be loaded
#   - $2 - table name
function load() {
    sqoop export \
        --connect jdbc:mysql://maria-gdelt:3306/bd_gdelt \
        --username=root \
        --password=root \
        --table=$2 \
        --export-dir="$1" \
        --input-fields-terminated-by '\t' \
        --lines-terminated-by '\n'

    if [ $? != 0 ] ; then
        error "[$(date)] Cannot load $1 to $2!"
        exit 1
    fi

    success "[$(date)] Loaded $1 to $2" >&2
}

function main() {
    hdfs dfs -test -e $RUN_CONTROL_DATE_PATH
    if [ $? != 0 ] ; then
        error "[$(date)] No RUN_CONTROL_DATE.dat found!"
        exit 1
    fi

    declare -r RUN_CONTROL_DATE=$(hdfs dfs -cat $RUN_CONTROL_DATE_PATH)

    echo "[$(date)] Loading $FILE_TO_LOAD to $TABLE_TO_LOAD..."

    load "$FILE_TO_LOAD" "$TABLE_TO_LOAD"
}

while getopts ":ht:f:" OPT; do
    case ${OPT} in
        h)
            help
            exit 0
        ;;
        t)
            TABLE_TO_LOAD=${OPTARG}
        ;;
        f)
            FILE_TO_LOAD=${OPTARG}
        ;;
        \?)
            error "Invalid option: -${OPTARG}"
            exit 1
        ;;
    esac
done

if [ -z $TABLE_TO_LOAD ] ; then
    error "Please specify table to load by using -t flag."
    exit 1
fi

if [ -z $FILE_TO_LOAD ] ; then
    error "Please specify file to load by using -f flag."
    exit 1
fi

main \

