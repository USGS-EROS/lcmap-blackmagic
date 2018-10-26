FROM continuumio/miniconda3

RUN apt-get update && apt-get upgrade -y && apt-get install libev gcc -y 
RUN conda install python=3.6 pip numpy scikit-learn scipy cython pandas>=0.19.2 --yes

COPY setup.py setup.py
COPY blackmagic/ blackmagic

RUN pip install --upgrade pip lcmap-merlin==2.3.1 xgboost -e .

ENV CASSANDRA_HOST=$CASSANDRA_HOST \
    CASSANDRA_PORT=$CASSANDRA_PORT \
    CASSANDRA_USER=$CASSANDRA_USER \
    CASSANDRA_PASS=$CASSANDRA_PASS \
    CASSANDRA_KEYSPACE=$CASSANDRA_KEYSPACE \
    CASSANDRA_TIMEOUT=$CASSANDRA_TIMEOUT \
    CASSANDRA_CONSISTENCY=$CASSANDRA_CONSISTENCY \
    CASSANDRA_CONCURRENT_WRITES=$CASSANDRA_CONCURRENT_WRITES \
    CHIPMUNK_URL=$CHIPMUNK_URL \
    HTTP_PORT=$HTTP_PORT \
    WORKERS=$WORKERS

# uwsgi to actually run this in ops
ENTRYPOINT blackmagic

