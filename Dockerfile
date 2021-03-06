FROM continuumio/miniconda3

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install libev-dev gcc git make -y && \
    conda install python=3.7 pip mkl numpy scikit-learn scipy cython pandas>=0.19.2 --yes && \
    git clone https://github.com/ncopa/su-exec.git && \
    cd su-exec && make all && mv su-exec /usr/bin

RUN adduser --system --shell /bin/bash --uid 1000 --no-create-home lcmap
RUN mkdir /app
WORKDIR /app
COPY setup.py setup.py
COPY version.txt version.txt
COPY README.rst README.rst
COPY bin/blackmagic.sh blackmagic.sh
COPY blackmagic/ blackmagic
RUN pip install --upgrade pip -e .
ENV PYTHONWARNINGS="ignore"
ENTRYPOINT su-exec lcmap:1000 /app/blackmagic.sh

