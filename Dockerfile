FROM python:2-alpine

MAINTAINER Jim Hickey

RUN apk add --no-cache --update \
    bash \
    git \
    openssh-client \
    linux-headers \
    gcc \
    g++ \
    jpeg-dev \
    libffi-dev \
    libxml2-dev \
    libxslt-dev \
    openssl-dev \
    musl-dev \
    libev-dev \
    make \
    && rm -rf /var/cache/apk/*

COPY aerospike-custom-setup/setup.py /temp_build/setup.py
COPY aerospike-custom-setup/install_aerospike.sh /temp_build/install_aerospike.sh
RUN chmod +x /temp_build/install_aerospike.sh
RUN /temp_build/install_aerospike.sh
RUN rm -rf /temp_build

RUN mkdir -p /tmp/filebeat
COPY ./run.sh /
COPY ./run_unit_tests.sh /

ENTRYPOINT ["/run.sh"]

