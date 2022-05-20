export DRIVER ?= clickhouse
export DATA_ROOT ?= `pwd`/data
export LOG_LEVEL ?= info

all: init download clean import

install:
	pip install -e .

init:
	fscli db init --recreate

download:
	mkdir -p $(DATA_ROOT)/src/latest
	wget -4 -P $(DATA_ROOT)/src/latest/ -r -l1 -H -nd -N -np -A "gz" -e robots=off https://data.farmsubsidy.org/latest/
	mkdir -p $(DATA_ROOT)/src/flat
	wget -4 -P $(DATA_ROOT)/src/flat/ -r -l2 -H -nd -N -np -A "gz" -e robots=off https://data.farmsubsidy.org/Flat/

clean:
	mkdir -p $(DATA_ROOT)/cleaned
	parallel "fscli clean -i {} --ignore-errors | gzip > $(DATA_ROOT)/cleaned/{/.}.cleaned.csv.gz" ::: $(DATA_ROOT)/src/latest/*.gz
	# some manual fixes
	fscli clean -i data/src/latest/lt_2016.csv.gz --currency=LTL | gzip > data/cleaned/lt_2016.csv.cleaned.csv.gz
	fscli clean -i data/src/latest/lt_2015.csv.gz --currency=LTL | gzip > data/cleaned/lt_2015.csv.cleaned.csv.gz
	fscli clean -i data/src/latest/pl_2015.csv.gz --currency=EUR | gzip > data/cleaned/pl_2015.csv.cleaned.csv.gz
	# parallel "fscli clean -i {} -h recipient_name,recipient_street1,recipient_street2,recipient_postcode,recipient_city,amount,amount_original,scheme,year,country --ignore-errors | gzip > $(DATA_ROOT)/cleaned_flat/{/.}.cleaned.csv.gz" ::: $(DATA_ROOT)/src/flat/*.gz

import: init
	@if [ "$(DRIVER)" = "clickhouse" ]; then \
  		parallel fscli db import --ignore-errors -i {} ::: $(DATA_ROOT)/cleaned/*.cleaned.csv.gz ; \
	else \
  		find $(DATA_ROOT)/cleaned -type f -name "*.cleaned.csv.gz" -exec fscli db import -i {} \; ; \
	fi


ftm: clean
	mkdir -p $(DATA_ROOT)/ftm
	parallel "gunzip -c {} | ftm map-csv mapping.yml | ftm store write -d farmsubsidy" ::: $(DATA_ROOT)/cleaned/*.gz
	ftm store iterate -d farmsubsidy > $(DATA_ROOT)/ftm/farmsubsidy.ftm.ijson

clickhouse-server:
	docker run -p 8123:8123 -p 9000:9000 --ulimit nofile=262144:262144 clickhouse/clickhouse-server

install.dev:
	pip install coverage nose moto pytest pytest-cov black flake8 isort ipdb

test:
	pytest -s --cov=farmsubsidy_store --cov-report term-missing
