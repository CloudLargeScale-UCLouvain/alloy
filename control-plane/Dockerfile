FROM python:3.10-slim

RUN apt-get update && \
  apt-get -y install default-jdk-headless build-essential && \
  rm -rf /var/lib/apt/lists/* && \
  ln -s "$(dirname $(dirname $(readlink -f $(which java))))" /opt/java

ENV JAVA_HOME=/opt/java

WORKDIR /opt

COPY ./requirements.txt .

RUN pip3 install --no-cache-dir -r /opt/requirements.txt

COPY . .

ENV JOBMANAGER_RPC_ADDRESS=control-plane-jobmanager-1
ENV EXECUTION_MODE=COMPILE
ENV DISABLE_OPERATOR_CHAINING=false
ENV NUM_SOURCES=2
ENV QUERY_FILENAME=q11.sql
ENV TABLES_FILENAME=tables.sql
ENV PARALLELISM=1

CMD ["python3", "/opt/control-plane.py"]