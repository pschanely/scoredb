#!/bin/bash

#for ZIPFILE in /mnt/census1990/census_1990/1990_PUMS_A/*.zip ; do
#    unzip -c $ZIPFILE
#done | grep '^P' >census1990_people.dat

INPUT=census1990_people.dat
OUTPUT=census1990_people.csv

COLUMNS="
age
children
depart_for_work
traveltime_to_work
weekly_work_hours
last_week_work_hours
carpool_riders
income
poverty_percentage
sex
weight
military_service_years
"

(printf 'id'
 for COL in $COLUMNS ; do
     printf ',%s' "$COL"
 done
 printf '\n'                ) >$OUTPUT

awk '{
age                    = substr($0,15,2);
children               = substr($0,89,2);
depart_for_work        = substr($0,105,4);
traveltime_to_work     = substr($0,109,2);
weekly_work_hours      = substr($0,125,2);
last_week_work_hours   = substr($0,93,2);
carpool_riders         = substr($0,104,1);
income                 = substr($0,133,6);
poverty_percentage     = substr($0,41,3);
sex                    = substr($0,11,1);
weight                 = substr($0,18,4);
military_service_years = substr($0,83,2);

children = (children == "00") ? 0 : int(children) - 1;

printf("r%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
       NR,
       age,
       children,
       depart_for_work,
       traveltime_to_work,
       weekly_work_hours,
       last_week_work_hours,
       carpool_riders,
       income,
       poverty_percentage,
       sex,
       weight,
       military_service_years)
} ' <$INPUT >>$OUTPUT
