#!/bin/bash
set -e

UPDATE_PYTHON_INSTALLATION=false

ENV="$1"  # should be 'dev' or 'prd'

PYTHON_VERSION=3.9.17
PYTHON_VERSION_BASE=3.9
S3_BUCKET=datlinq-datalabs-${ENV}-jobs-python/great-expectations/artifacts/emr/


if $UPDATE_PYTHON_INSTALLATION; then

    # Replace old OpenSSL and add build utilities
    sudo yum -y remove openssl-devel* && \
    sudo yum -y install gcc openssl11-devel bzip2-devel libffi-devel tar gzip wget make expat-devel sqlite-devel

    echo "----------- Download Python -----------"
    wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz
    sudo tar xzvf Python-${PYTHON_VERSION}.tgz

    #echo "go inside python folder"
    cd Python-${PYTHON_VERSION}

    echo "----------- execute the .configure ---------"
    # We aim for similar `CONFIG_ARGS` that AL2 Python is built with
    sudo ./configure --enable-loadable-sqlite-extensions --with-lto --enable-optimizations --with-system-expat \
        --prefix=/usr/local/python${PYTHON_VERSION}

    # Install into /usr/local/python3.xx.x
    # Note that "make install" links /usr/local/pythonx.xx.x/bin/pythonx while "altinstall" does not
    echo "----------- make altinstall ---------"
    sudo make altinstall

    echo "----------- pip installs ---------"
    # Good practice to upgrade pip
    sudo /usr/local/python${PYTHON_VERSION}/bin/python${PYTHON_VERSION_BASE} -m pip install --upgrade pip

    sudo /usr/local/python${PYTHON_VERSION}/bin/python${PYTHON_VERSION_BASE} -m pip install numpy==1.24.3
    sudo /usr/local/python${PYTHON_VERSION}/bin/python${PYTHON_VERSION_BASE} -m pip install pandas==1.3.5
    sudo /usr/local/python${PYTHON_VERSION}/bin/python${PYTHON_VERSION_BASE} -m pip install great_expectations==0.16.15
    sudo /usr/local/python${PYTHON_VERSION}/bin/python${PYTHON_VERSION_BASE} -m pip install boto3==1.26.123

    echo "----------- Upload python to S3 ---------"
    cd /usr/local
    sudo tar czvf python${PYTHON_VERSION_BASE}.tar.gz python${PYTHON_VERSION}/
    aws s3 cp python${PYTHON_VERSION_BASE}.tar.gz s3://${S3_BUCKET}   ß

    echo "----------- Python installation updated ---------"

else

    echo "----------- Downloading Python from S3 ---------"
    PYTHON_ARCHIVE=s3://${S3_BUCKET}python${PYTHON_VERSION_BASE}.tar.gz\
    # Copy and extract the Python archive into `/usr/local`
    sudo aws s3 cp "${PYTHON_ARCHIVE}" /usr/local/
    cd /usr/local/
    sudo tar -xf "$(basename "${PYTHON_ARCHIVE}")"
    sudo rm "$(basename "${PYTHON_ARCHIVE}")"

    echo "----------- Python Downloaded ---------"

fi