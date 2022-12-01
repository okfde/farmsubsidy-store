FROM python:3.10

RUN apt-get update && apt-get install parallel

COPY farmsubsidy_store /app/farmsubsidy_store
COPY setup.py /app/setup.py
COPY README.md /app/README.md

WORKDIR /app
RUN pip install gunicorn uvicorn
RUN pip install -e .


# Run the green unicorn
CMD gunicorn -w 8 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:5000 --name farmsubsidy_gunicorn_api
