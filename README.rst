.. image:: https://travis-ci.org/USGS-EROS/lcmap-blackmagic.svg?branch=develop
    :target: https://travis-ci.org/USGS-EROS/lcmap-blackmagic

================
lcmap-blackmagic
================
HTTP server that saves PyCCD & prediction output to Apache Cassandra


On DockerHub
------------

https://hub.docker.com/r/usgseros/lcmap-blackmagic/


On PyPi
-------
.. code-block:: bash

    pip install lcmap-blackmagic

    
Features
--------
* Exposes execution of PyCCD over HTTP
* Saves results to Apache Cassandra
* Highly tunable
* Available as Python package or Docker image


Example
-------

Start BlackMagic

.. code-block:: bash

    docker run -it \
               --rm \
               --net=host \
               --pid=host \
	       -e CASSANDRA_HOST=localhost \
	       -e CASSANDRA_PORT=9042 \
	       -e CASSANDRA_USER=cassandra \
	       -e CASSANDRA_PASS=cassandra \
	       -e CASSANDRA_KEYSPACE=some_keyspace \
	       -e CASSANDRA_TIMEOUT=600 \
	       -e CASSANDRA_CONSISTENCY=QUORUM \
	       -e CASSANDRA_CONCURRENT_WRITES=1 \
	       -e CHIPMUNK_URL=http://host:port/path \
	       -e CPUS=4 \
	       -e HTTP_PORT=5000 \
	       -e WORKERS=4 \
	       -e WORKER_TIMEOUT=12000 \
               usgseros/lcmap-blackmagic:1.0

	    
Send a request

.. code-block:: bash

    http --timeout=12000 POST http://localhost:5000/segment cx:=1556415.0 cy:=2366805.0

    
Tuning
------
Blackmagic has three primary controls that determine the nature of its parallism and concurrency: WORKERS, CPUS & CASSANDRA_CONCURRENT_WRITES.

WORKERS controls the number of HTTP listener processes (gunicorn workers).

CPUS controls the number of cores available to each WORKER.

CASSANDRA_CONCURRENT_WRITES controls the number of parallel cassandra writes for each WORKER.

In general, WORKERS determines the number of simultaneous HTTP requests that can be serviced.  CPUS & CASSANDRA_CONCURRENT_WRITES determine how quickly each individual request can be completed.

One deployment approach is to accept the highest number of HTTP requests in parallel. To do this, WORKERS would equal the number of cores available and CPUS/CASSANDRA_CONCURRENT_WRITES would equal 1.

Another approach is to process each request as quickly as possible.  To do this, WORKERS would equal 1 and CPUS would equal the number of cores available.  CASSANDRA_CONCURRENT_WRITES would be set to 1 unless the finished results are not being saved quickly enough (Observe memory utilization of the running server.  Steadily climbing memory utilization of the WORKER process(s) indicate the results are queuing and not being saved as quickly as they are being produced.)


Requirements
------------

* Python3 or Docker
* Network access to Cassandra
* Network access to Chipmunk
                       
Versioning
----------
lcmap-blackmagic follows semantic versioning: http://semver.org/

License
-------
This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to http://unlicense.org.
