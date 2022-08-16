FROM mcr.microsoft.com/planetary-computer/python:2022.8.1.0
RUN /srv/conda/envs/notebook/bin/python3 -m pip install cdsapi
COPY etl.py /code/etl.py
WORKDIR /code
