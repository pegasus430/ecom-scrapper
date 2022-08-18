#!/usr/bin/env bash

# there is no built lib for alpine for python aerospike client which could be installed with pip
# it has to be built from source

C_CLIENT_VERSION=v3.1.1
PYTHON_CLIENT_VERISON=3.2.0

# Build aerospike C client
git clone https://github.com/aerospike/aerospike-client-c.git /temp_build/aerospike-client-c/
cd /temp_build/aerospike-client-c/ && git ckeckout tags/$C_CLIENT_VERSION
git submodule update --init
export EVENT_LIB=libev
make && make install

# Build aerospike Python client wrapper
git clone https://github.com/aerospike/aerospike-lua-core.git /temp_build/aerospike-lua-core/

export DOWNLOAD_C_CLIENT=0
export AEROSPIKE_C_HOME=/temp_build/aerospike-client-c
export AEROSPIKE_LUA_PATH=/temp_build/aerospike-lua-core/src

git clone https://github.com/aerospike/aerospike-client-python.git /temp_build/aerospike-client-python/
cd /temp_build/aerospike-client-python/ && git checkout tags/$PYTHON_CLIENT_VERISON
# aerospike's setup.py is buggy when run in docker (https://github.com/moby/moby/issues/9547)
# it also misses -DAS_USE_LIBEV flag in extra_compile_args, which enables libev, without which package won't link
mv /temp_build/setup.py /temp_build/aerospike-client-python/setup.py
cd /temp_build/aerospike-client-python/
git submodule update --init
python setup.py build --force && python setup.py install --force
