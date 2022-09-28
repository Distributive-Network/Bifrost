#!/bin/bash

NOCOLOR='\033[0m'
RED='\033[0;31m'
GREEN='\033[0;32m'

cd ./tests/

EXIT_COUNTER=0
COUNTER=0
NUM_TESTS=$(ls -l ./*.test.py | wc -l)

echo -e "${NOCOLOR}Will now run ${NUM_TESTS} tests."

for file in ./*.test.py;
do
  COUNTER=$(( $COUNTER + 1 ))
  echo -e "${NOCOLOR}${COUNTER}/${NUM_TESTS} - Beginning test of ${file}"
  python3 $file
  EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]
  then
    EXIT_COUNTER=$(( $EXIT_COUNTER + 1 ))
    echo -e "${RED}${COUNTER}/${NUM_TESTS} - Failed running test of ${file}${NOCOLOR}"
  else
    echo -e "${GREEN}${COUNTER}/${NUM_TESTS} - Succeeded running test of ${file}${NOCOLOR}"
  fi
done

if [ $EXIT_COUNTER -ne 0 ]
then
  echo -e "${RED}${EXIT_COUNTER}/${NUM_TESTS} have failed.${NOCOLOR}"
else
  echo -e "${GREEN}${EXIT_COUNTER}/${NUM_TESTS} have failed.${NOCOLOR}"
fi


if [ $EXIT_COUNTER -ne 0 ]
then
  exit 1
else
  exit 0
fi
