#!/bin/sh

zdname=./sample.d/zips.d
opname=./sample.d/out.parquet

geninput(){
	echo generating input files...

	mkdir -p "${zdname}"

	echo hw00 > ./sample.d/hw00.txt
	echo hw01 > ./sample.d/hw01.txt
	echo hw02 > ./sample.d/hw02.txt

	echo hw10 > ./sample.d/hw10.txt
	echo hw11 > ./sample.d/hw11.txt
	echo hw12 > ./sample.d/hw12.txt

	ls ./sample.d/hw0?.txt | zip -@ -o ./sample.d/zips.d/hw0.zip
	ls ./sample.d/hw1?.txt | zip -@ -o ./sample.d/zips.d/hw1.zip
}

test -f ./sample.d/zips.d/hw0.zip || geninput
test -f ./sample.d/zips.d/hw1.zip || geninput

./zips2meta2parquet \
	--zips-dir "${zdname}" \
	--output-parquet-filename "${opname}"
