FROM ghcr.io/simonwoerpel/ftm-geocode:main

RUN apt-get update && apt-get install -y parallel

COPY farmsubsidy_store /farmsubsidy/farmsubsidy_store
COPY setup.py /farmsubsidy/setup.py
COPY setup.cfg /farmsubsidy/setup.cfg
COPY VERSION /farmsubsidy/VERSION
COPY Makefile /farmsubsidy/Makefile

WORKDIR /farmsubsidy
RUN wget -O cache.db.gz https://s3.investigativedata.org/farmsubsidy/cache.db.gz
RUN gunzip cache.db.gz

RUN pip install -U pip setuptools
RUN pip install gunicorn uvicorn
RUN pip install -e ".[geo]"

ENV DEBUG=0
ENV PARALLEL=-j`nproc`

ENTRYPOINT ["gunicorn", "farmsubsidy_store.api:app", "--bind", "0.0.0.0:8000", "--worker-class", "uvicorn.workers.UvicornWorker"]
