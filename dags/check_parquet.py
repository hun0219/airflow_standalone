#!/bin/bash

#YYYYMMDD=$1
DONE_PATH_FILE=$1
#ehco "check"
#DONE_PATH=~/data/done/import/${YYYYMMDD}
#DONE_PATH_FILE="${DONE_PATH}/_DONE"

#파일 존재 여부 확인
if [ -e "$DONE_PATH_FILE" ]; then
    figlet "Let's move on"
    exit 0
else
    echo "I'll be back => $DONE_PATH_FILE"
    exit 1
fi