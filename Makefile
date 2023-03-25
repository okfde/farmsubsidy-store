export DRIVER ?= clickhouse
export DATA_ROOT ?= `pwd`/data
export LOG_LEVEL ?= info
export COMPOSE ?= docker-compose.yml
export DATA_DOMAIN = https://$(DATA_BASIC_AUTH)@data.farmsubsidy.org

all: init download clean import

install:
	pip install -e .

init:
	fscli db init --recreate

download:
	mkdir -p $(DATA_ROOT)/src/latest
	wget -4 -P $(DATA_ROOT)/src/latest/ -r -l1 -H -nd -N -np -A "gz" -e robots=off $(DATA_DOMAIN)/latest/
	mkdir -p $(DATA_ROOT)/src/flat
	wget -4 -P $(DATA_ROOT)/src/flat/ -r -l2 -H -nd -N -np -A "gz" -e robots=off $(DATA_DOMAIN)/Flat/

clean:
	mkdir -p $(DATA_ROOT)/cleaned
	parallel "fscli clean -i {} --ignore-errors | gzip > $(DATA_ROOT)/cleaned/{/.}.cleaned.csv.gz" ::: $(DATA_ROOT)/src/latest/*.csv.gz
	# some manual fixes
	fscli clean -i $(DATA_ROOT)/src/latest/lt_2016.csv.gz --currency=LTL | gzip > $(DATA_ROOT)/cleaned/lt_2016.csv.cleaned.csv.gz
	fscli clean -i $(DATA_ROOT)/src/latest/lt_2015.csv.gz --currency=LTL | gzip > $(DATA_ROOT)/cleaned/lt_2015.csv.cleaned.csv.gz
	fscli clean -i $(DATA_ROOT)/src/latest/pl_2015.csv.gz --currency=EUR | gzip > $(DATA_ROOT)/cleaned/pl_2015.csv.cleaned.csv.gz
	# OLD DATA
	# parallel "fscli clean -i {} -h recipient_name,recipient_street1,recipient_street2,recipient_postcode,recipient_city,amount,amount_original,scheme,year,country --ignore-errors | gzip > $(DATA_ROOT)/cleaned_flat/{/.}.cleaned.csv.gz" ::: $(DATA_ROOT)/src/flat/*.gz

download_cleaned:
	mkdir -p $(DATA_ROOT)/cleaned
	wget -4 -P $(DATA_ROOT)/cleaned/ -r -l1 -H -nd -N -np -A "gz" -e robots=off $(DATA_DOMAIN)/cleaned/

import: init
	@if [ "$(DRIVER)" = "clickhouse" ]; then \
  		parallel fscli db import --ignore-errors -i {} ::: $(DATA_ROOT)/cleaned/*.cleaned.csv.gz ; \
	else \
  		find $(DATA_ROOT)/cleaned -type f -name "*.cleaned.csv.gz" -exec fscli db import -i {} \; ; \
	fi


ftm:  # clean
	mkdir -p $(DATA_ROOT)/ftm
	parallel "gunzip -c {} | ftm map-csv mapping.yml | ftm store write -d farmsubsidy" ::: $(DATA_ROOT)/cleaned/*.gz
	ftm store iterate -d farmsubsidy > $(DATA_ROOT)/ftm/farmsubsidy.ftm.ijson

clickhouse:  # for dev, doesn't persist data
	docker run -p 8123:8123 -p 9000:9000 --ulimit nofile=262144:262144 clickhouse/clickhouse-server

redis: # dev
	docker run -p 6379:6379 redis

api:  install.api  # for developement
	DEBUG=1 uvicorn farmsubsidy_store.api:app --reload

install.api:
	pip install uvicorn

install.dev:
	pip install coverage nose moto pytest pytest-cov black flake8 isort ipdb mypy bump2version

test: install.dev
	pip install types-python-jose
	pip install types-passlib
	pip install pandas-stubs
	pytest -s --cov=farmsubsidy_store --cov-report term-missing
	mypy farmsubsidy_store


docker:
	docker-compose -f $(COMPOSE) up -d

docker.%:
	docker-compose -f $(COMPOSE) run --rm api make $*
