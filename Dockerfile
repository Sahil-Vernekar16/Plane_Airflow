# Base image
FROM python:3.8-slim

LABEL maintainer="Sahil"

# Arguments
ARG AIRFLOW_VERSION=2.5.1
ARG PYTHON_VERSION=3.8
ARG AIRFLOW_HOME=/opt/airflow 

ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# Install dependencies and tools
RUN apt-get update -yqq && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --no-install-recommends \
    wget \
    libczmq-dev \
    curl \
    libssl-dev \
    git \
    inetutils-telnet \
    bind9utils freetds-dev \
    libkrb5-dev \
    libsasl2-dev \
    libffi-dev libpq-dev \
    freetds-bin build-essential \
    default-libmysqlclient-dev \
    apt-utils \
    rsync \
    zip \
    unzip \
    gcc \
    vim \
    locales \
    pkg-config \
    curl \
    gnupg2 \
    ca-certificates \
    unixodbc \
    && apt-get clean

# Install Microsoft ODBC Driver for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update -y && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Install Airflow
RUN wget https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.8.txt

RUN pip install --upgrade pip && \
    useradd -ms /bin/bash -d ${AIRFLOW_HOME} azurelib && \
    pip install apache-airflow[postgres]==${AIRFLOW_VERSION} --constraint /constraints-3.8.txt

# Install Airflow Providers
RUN pip install apache-airflow-providers-microsoft-mssql
RUN pip install apache-airflow-providers-http
RUN pip install 'apache-airflow[amazon]'
RUN pip install pyodbc
RUN pip install apache-airflow-providers-google
RUN pip install apache-airflow-providers-microsoft-azure

# Copy the airflow_init.sh from host to container (at path AIRFLOW_HOME)
COPY ./airflow_commands.sh ./airflow_commands.sh

# Set the airflow_init.sh file to be executable
RUN chmod +x ./airflow_commands.sh

# Set the owner of the files in AIRFLOW_HOME to the user airflow
RUN chown -R azurelib: ${AIRFLOW_HOME}

# Set the username to use
USER azurelib

# Set workdir (it's like a cd inside the container)
WORKDIR ${AIRFLOW_HOME}

# Create dags directory
RUN mkdir dags

# Expose ports (just to indicate that this container needs to map port)
EXPOSE 8080

# Execute the airflow_commands.sh
ENTRYPOINT [ "/airflow_commands.sh" ]
