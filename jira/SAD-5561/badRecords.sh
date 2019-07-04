#!/bin/bash

grep -P -B 30 'bad records number' /home/thresher/log/event-agg_201904.log \
    | sed 's/\[.*\]//g' \
    | grep -E 'updateLastHitDate_|bad records number [0-9]+|^--$' \
    | tr -d '\n' \
    | sed 's/--/\n/g' \
    | sed -E 's/.* updateLastHitDate_([0-9]+) .* bad records number ([0-9]+) .*/\1,\2/g' \
    | awk '{sum += $17} END {print sum}'

