# Based on: https://github.com/ibm-functions/runtime-python/tree/master/python3.6

FROM python:3.8-slim-buster

ENV FLASK_PROXY_PORT 8080

RUN apt-get update && apt-get upgrade -y

RUN     apt-get install -y \
        build-essential \
        gcc \
        grass \
	    grass-dev \        
	    libc-dev \
        libxslt-dev \
        libxml2-dev \
        libffi-dev \
        libssl-dev \
        zip \
        unzip \
        vim \
        libgdal-dev \
        gdal-bin \
        && rm -rf /var/lib/apt/lists/*

RUN apt-cache search linux-headers-generic

RUN export CPLUS_INCLUDE_PATH=/usr/include/gdal
RUN export C_INCLUDE_PATH=/usr/include/gdal

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip setuptools six && pip install --no-cache-dir -r requirements.txt

# create action working directory
RUN mkdir -p /action

RUN mkdir -p /actionProxy
ADD https://raw.githubusercontent.com/apache/incubator-openwhisk-runtime-docker/8b2e205c39d84ed5ede6b1b08cccf314a2b13105/core/actionProxy/actionproxy.py /actionProxy/actionproxy.py

RUN mkdir -p /pythonAction
ADD https://raw.githubusercontent.com/apache/incubator-openwhisk-runtime-python/3%401.0.3/core/pythonAction/pythonrunner.py /pythonAction/pythonrunner.py

RUN export CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

CMD ["/bin/bash", "-c", "cd /pythonAction && python -u pythonrunner.py"]
