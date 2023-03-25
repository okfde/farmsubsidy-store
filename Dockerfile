FROM ghcr.io/simonwoerpel/ftm-geocode

RUN apt-get update && apt-get install -y parallel

COPY farmsubsidy_store /farmsubsidy/farmsubsidy_store
COPY setup.py /farmsubsidy/setup.py
COPY setup.cfg /farmsubsidy/setup.cfg
COPY VERSION /farmsubsidy/VERSION
COPY Makefile /farmsubsidy/Makefile

ENV DEBUG=0
ENV PARALLEL=-j`nproc`
WORKDIR /farmsubsidy
RUN pip install -U pip setuptools
RUN pip install gunicorn uvicorn
RUN pip install -e ".[geo]"


# Run the green unicorn with 1 worker (scale via docker then)
CMD ["uvicorn", "farmsubsidy_store.api:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8000"]
