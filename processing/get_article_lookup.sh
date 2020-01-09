#!/bin/bash

# Script for unloading an article lookup.

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
    echo -e "${BOLD}Unload an article lookup from the database. ${RESET}"
    echo -e "${BOLD}Usage${RESET}: ./get_article_looup.sh"
}

function main() {
    hdfs dfs -test -e $RUN_CONTROL_DATE_PATH
    if [ $? != 0 ] ; then
        error "[$(date)] No RUN_CONTROL_DATE.dat found!"
        exit 1
    fi

    declare -r RUN_CONTROL_DATE=$(hdfs dfs -cat $RUN_CONTROL_DATE_PATH)

    sqoop import \
        --connect jdbc:mysql://maria-gdelt:3306/bd_gdelt \
        --username=root \
        --password=root \
        --table=article \
        --columns "id,url" \
        --fields-terminated-by '\t' \
        --delete-target-dir \
        --target-dir="/etl/staging/load/$RUN_CONTROL_DATE/article_lookup.dat"

}

main

