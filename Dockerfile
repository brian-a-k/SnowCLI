FROM python:3.8.13-slim

# output dir for airflow xcom values
RUN mkdir -p /airflow/xcom

# copy & install snow code base
COPY . /root/dpe-snow/
WORKDIR /root/dpe-snow/
RUN pip install .

# set working dir
WORKDIR /root
