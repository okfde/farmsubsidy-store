export DATA_ROOT ?= `pwd`/data
export DUCKDB_PATH ?= `pwd`/farmsubsidy.duckdb
export LOG_LEVEL ?= info

all: init download clean import


init:
	fscli db init --recreate

download:
	mkdir -p $(DATA_ROOT)/src
	wget -4 -P $(DATA_ROOT)/src/ -r -l1 -H -nd -N -np -A "gz" -e robots=off https://data.farmsubsidy.org/latest/

clean:
	mkdir -p $(DATA_ROOT)/cleaned
	parallel "fscli clean -i {} --ignore-errors | gzip > $(DATA_ROOT)/cleaned/{/.}.cleaned.csv.gz" ::: $(DATA_ROOT)/src/*.gz

import: init
	find $(DATA_ROOT)/cleaned -type f -name "*.cleaned.csv.gz" -exec fscli db import -i {} \;
	fscli db index

ftm: clean
	mkdir -p $(DATA_ROOT)/ftm
	parallel "gunzip -c {} | ftm map-csv mapping.yml | ftm store write -d farmsubsidy" ::: $(DATA_ROOT)/cleaned/*.gz
	ftm store iterate -d farmsubsidy > $(DATA_ROOT)/ftm/farmsubsidy.ftm.ijson
