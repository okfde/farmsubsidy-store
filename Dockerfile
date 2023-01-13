FROM python:3.11

RUN apt-get update && apt-get install -y parallel

RUN apt-get update && apt-get install -y parallel

COPY farmsubsidy_store /app/farmsubsidy_store
COPY setup.py /app/setup.py
COPY setup.cfg /app/setup.cfg
COPY VERSION /app/VERSION
COPY Makefile /app/Makefile

WORKDIR /app
RUN pip install -U pip setuptools
RUN pip install gunicorn uvicorn
RUN pip install -e .


# Run the green unicorn
CMD gunicorn -w 8 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:5000 --name farmsubsidy_gunicorn_api
