FROM ghcr.io/simonwoerpel/ftm-geocode

RUN apt-get update && apt-get install -y parallel

COPY farmsubsidy_store /farmsubsidy/farmsubsidy_store
COPY setup.py /farmsubsidy/setup.py
COPY setup.cfg /farmsubsidy/setup.cfg
COPY VERSION /farmsubsidy/VERSION
COPY Makefile /farmsubsidy/Makefile

WORKDIR /farmsubsidy
RUN pip install -U pip setuptools
RUN pip install gunicorn uvicorn
RUN pip install -e ".[geo]"


# Run the green unicorn
CMD gunicorn -w 8 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:5000 --name farmsubsidy_gunicorn_api
