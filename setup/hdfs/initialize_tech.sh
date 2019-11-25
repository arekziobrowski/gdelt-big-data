#!/bin/bash

# Script for creating /tech HDFS directory structure

declare -r BOLD="\033[1m"
declare -r RESET="\033[0m"
declare -r RED="\033[31m"
declare -r LIGHT_BLUE='\033[1;34m'
declare -r GREEN='\033[32m'

declare -r EXTRACTION_PATH='/tech/extraction'
declare -r TRANSFORMATION_PATH='/tech/transformation'
declare -r LOAD_PATH='/tech/load'

function error() {
    echo -e "${RED}${BOLD}${*}${RESET}" >&2
}

function success() {
    echo -e "${GREEN}${*}${RESET}" >&1
}

function help() {
    echo -e "${BOLD}Create /tech HDFS directory structure. ${RESET}"
    echo -e "${BOLD}Usage${RESET}: ./initialize_tech"
    echo -e "\t-r\tReplace the existing directories"
    echo -e "\t-h\tPrints this help message"
}

function create_directory() {
    if [ ! -z "$REPLACE" ] ; then
        hdfs dfs -rm -R $1
    fi

    hdfs dfs -test -d $1
    if [ $? != 0 ] ; then
        echo "Creating $1"
        hdfs dfs -mkdir -p $1
    else
        error "Directory $1 already exists!"
    fi
}

function main() {
    create_directory $EXTRACTION_PATH/log
    create_directory $EXTRACTION_PATH/checkpoint
    create_directory $TRANSFORMATION_PATH/log
    create_directory $LOAD_PATH/log
    success 'Finished!'
}

while getopts ":hr" OPT; do
    case ${OPT} in
        h)
            help
            exit 0
        ;;
        r)
            REPLACE=0
        ;;
        \?)
            error "Invalid option: -${OPTARG}"
            exit 1
        ;;
    esac
done

main

