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
* Automatic schema creation on startup
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
	       -e CASSANDRA_BATCH_SIZE=1000 \
	       -e CASSANDRA_HOST=localhost \
	       -e CASSANDRA_PORT=9042 \
	       -e CASSANDRA_USER=cassandra \
	       -e CASSANDRA_PASS=cassandra \
	       -e CASSANDRA_KEYSPACE=some_keyspace \
	       -e CASSANDRA_TIMEOUT=600 \
	       -e CASSANDRA_CONSISTENCY=ALL \
	       -e CHIPMUNK_URL=http://host:port/path \
	       -e CPUS_PER_WORKER=4 \
	       -e HTTP_PORT=5000 \
	       -e WORKERS=4 \
	       -e WORKER_TIMEOUT=12000 \
               usgseros/lcmap-blackmagic:1.0

	    
Send a request

.. code-block:: bash

    http --timeout=12000 POST http://localhost:5000/segment cx:=1556415.0 cy:=2366805.0 acquired=1980/2017

URLs
----
+------------------------+------------------------+------------------------------------+
| URL                    | Parameters             | Description                        |
+========================+========================+====================================+
| POST /segment          | cx, cy, acquired       | Save change detection segments     |
+------------------------+------------------------+------------------------------------+
| POST /tile             | tx, ty, date, chips    | Create and save xgboost model      |
| (not yet implemented)  |                        | chips/date at tile x and tile y    | 
+------------------------+------------------------+------------------------------------+
| POST /prediction       | cx, cy                 | Save xgboost predictions for       |
| (not yet implemented)  |                        | chip x (cx) and chip y (cy)        |
+------------------------+------------------------+------------------------------------+
| GET /health            | None                   | Determine health of server         |
+------------------------+------------------------+------------------------------------+

    
Tuning
------
Blackmagic has two primary controls that determine the nature of its parallelism and concurrency: ``WORKERS`` and ``CPUS_PER_WORKER``.

``WORKERS`` controls the number of HTTP listener processes (gunicorn workers) and thus, the number of simultaneous HTTP requests that can be serviced.

``CPUS_PER_WORKER`` controls the number of cores available to each ``WORKER``.


Deployment Examples
~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Many slow HTTP requests

    -e WORKERS=<number of cores available>
    -e CPUS_PER_WORKER=1

    # One fast HTTP request
    
    -e WORKERS=1
    -e CPUS_PER_WORKER=<number of cores available>

    
Requirements
------------

* Python3 or Docker
* Network access to Cassandra
* Network access to Chipmunk

HTTP Requests & Responses
-------------------------
.. code-block:: bash


    # /segment resource expects cx (chip x) and cy (chip y) as parameters
    # If parameters are missing /segment returns HTTP 400 with JSON message
		
    [user@machine bin]$ http --timeout 12000 POST http://localhost:9876/segment cx=1484415 
    HTTP/1.1 400 BAD REQUEST
    Connection: close
    Content-Length: 67
    Content-Type: application/json
    Date: Tue, 04 Dec 2018 14:59:21 GMT
    Server: gunicorn/19.9.0

    {
        "acquired": null,
        "cx": 1484415, 
        "cy": null,
        "msg": "cx, cy, and acquired are required parameters"
    }

    [user@machine]$ http --timeout 12000 POST http://localhost:9876/segment cy=1484415 
    HTTP/1.1 400 BAD REQUEST
    Connection: close
    Content-Length: 67
    Content-Type: application/json
    Date: Tue, 04 Dec 2018 14:59:26 GMT
    Server: gunicorn/19.9.0

    {
        "acquired": null,
        "cx": null, 
        "cy": 1484415,
        "msg": "cx, cy, and acquired are required parameters"
    }

    [user@machine bin]$ http --timeout 12000 POST http://localhost:9876/segment 
    HTTP/1.1 400 BAD REQUEST
    Connection: close
    Content-Length: 64
    Content-Type: application/json
    Date: Tue, 04 Dec 2018 14:59:29 GMT
    Server: gunicorn/19.9.0

    {
        "acquired": null,
        "cx": null, 
        "cy": null,
        "msg": "cx, cy, and acquired are required parameters"
    }

    # if no input data was available from Chipmunk for cx/cy & acquired date range,
    # /segment returns HTTP 400 with msg = "no input data"
    
    [user@machine bin]$ http --timeout 12000 POST http://localhost:9876/segment cx=1484415 cy=-99999999 acquired=1980-01-01/2017-12-31
    HTTP/1.1 400 BAD REQUEST
    Connection: close
    Content-Length: 52
    Content-Type: application/json
    Date: Tue, 04 Dec 2018 14:59:40 GMT
    Server: gunicorn/19.9.0

    {
    	"acquired": 1980-01-01/2017-12-31,
        "cx": 1484415, 
        "cy": -99999999,
        "msg": "no input data"
    }


    # Successful POST to /segment returns HTTP 200 and cx/cy as JSON
    
    [user@machine bin]$ http --timeout 12000 POST http://localhost:9876/segment cx=1484415 cy=2414805 acquired=1980/2017-12-31
    HTTP/1.1 200 OK
    Connection: close
    Content-Length: 28
    Content-Type: application/json
    Date: Tue, 04 Dec 2018 15:37:33 GMT
    Server: gunicorn/19.9.0

    {
        "acquired": 1980/2017-12-31,
        "cx": 1484415, 
        "cy": 2414805,
    }


    # Database errors reported with HTTP 500 and the first error that occurred, with request parameters as JSON
    
    [user@machine bin]$ http --timeout 1200 POST http://localhost:9876/segment cx=1484415 cy=2414805 acquired=1980/2017-12-31
    HTTP/1.1 500 INTERNAL SERVER ERROR
    Connection: close
    Content-Length: 89
    Content-Type: application/json
    Date: Thu, 31 Jan 2019 22:04:57 GMT
    Server: gunicorn/19.9.0
    
    {
        "acquired": "1980/2017-12-31", 
        "cx": "1484415", 
        "cy": "2414805", 
        "msg": "db connection error"
    }
    
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
