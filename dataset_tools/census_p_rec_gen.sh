#!/bin/bash

#for ZIPFILE in /mnt/census1990/census_1990/1990_PUMS_A/*.zip ; do
#    unzip -c $ZIPFILE
#done | grep '^P' >census1990_people.dat

INPUT=census1990_people.dat
OUTPUT=census1990_people.csv

# for this dataset, gawk output is different than mawk or nawk, for 7 records (out of millions)
AWK=${AWK:-awk}

COLUMNS="
age
children
depart_for_work
traveltime_to_work
weekly_work_hours
last_week_work_hours
carpool_riders
income
wages
poverty_percentage
sex
military_service_years
"

(printf 'id'
 for COL in $COLUMNS ; do
     printf ',%s' "$COL"
 done
 printf '\n'                ) >$OUTPUT

$AWK '{
children = substr($0,89,2);
children = (children == "00") ? 0 : int(children) - 1;

printf("r%d,", NR);
printf("%s,",  substr($0, 15,2));  # age
printf("%s,",  children        );  # children
printf("%s,",  substr($0,105,4));  # depart_for_work
printf("%s,",  substr($0,109,2));  # traveltime_to_work
printf("%s,",  substr($0,125,2));  # weekly_work_hours
printf("%s,",  substr($0, 93,2));  # last_week_work_hours
printf("%s,",  substr($0,104,1));  # carpool_riders
printf("%s,",  substr($0,133,6));  # income
printf("%s,",  substr($0,139,6));  # wages
printf("%s,",  substr($0, 41,3));  # poverty_percentage
printf("%s,",  substr($0, 11,1));  # sex
printf("%s\n", substr($0, 83,2));  # military_service_years
} ' <$INPUT >>$OUTPUT
