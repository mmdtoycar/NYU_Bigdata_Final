#!/bin/bash
if [ $# -ne 1 ]
then
	echo "usage: bash run_script [cleandataset_path]"
	exit 1
fi
rm -rf output
mkdir output
for file in `ls script/ | grep .py`; do
	rm -rf ${file%???}.csv
	spark-submit script/$file $1
	/usr/bin/hadoop fs -put ${file%???}.csv /
	rm -rf ${file%???}.csv
	/usr/bin/hadoop fs -getmerge ／${file%???}.csv output/${file%???}.csv
done
