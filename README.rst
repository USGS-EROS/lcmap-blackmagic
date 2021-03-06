.. image:: https://travis-ci.org/USGS-EROS/lcmap-blackmagic.svg?branch=develop
    :target: https://travis-ci.org/USGS-EROS/lcmap-blackmagic

================
lcmap-blackmagic
================
HTTP server that saves land change segment, trained classifiers & land cover probabilities to S3/Ceph.  


What does it do?
----------------
* Uses PyCCD to detect land change segments
* Trains & persists XGBoost classifiers from change segments & NLCD reference data
* Applies classifiers to create land cover probabilities

  
Use 
----
.. code-block:: bash

    
    $ http --timeout=12000 POST http://localhost:9876/segment cx=1556415 cy=2366805 acquired=1980/2017

    $ http --timeout=12000 POST http://localhost:9876/tile tx=1484415 ty=2414805 acquired=1980/2017 date=2001-07-01 chips=[[1484415,2414805], [...]]

    $ http --timeout=12000 POST http://localhost:9876/prediction tx=1484415 ty=2414805 cx=1556415 cy=2366805 acquired=1982/2017 month=7 day=1 

    
URLs
----
+------------------------+------------------------+------------------------------------+
| URL                    | Parameters             | Description                        |
+========================+========================+====================================+
| POST /segment          | cx, cy, acquired       | Save change detection segments     |
+------------------------+------------------------+------------------------------------+
| POST /tile             | tx, ty, acquired,      | Create and save xgboost model      |
|                        | date, chips            | at tx,ty for chips in list for     |
|                        |                        | prediction date.  Acquired         |
|                        |                        | controls aux data inputs to        |
|                        |                        | training entries                   |
+------------------------+------------------------+------------------------------------+
| POST /prediction       | tx, ty, cx, cy         | Save xgboost predictions for       |
|                        | acquired, month, day   | cx,cy using model saved at tx,ty   |
|                        |                        | for segments in acquired range     |
|                        |                        | with prediction date of month-day  |
+------------------------+------------------------+------------------------------------+
| GET /health            | None                   | Determine health of server         |
+------------------------+------------------------+------------------------------------+


Requirements
------------
* lcmap-chipmunk running with ARD & NLCD data ingested
* Ability to run Docker containers
* An S3/Ceph bucket to save outputs
* HTTP traffic load balancer (optional)

References & Acknowledgements
-----------------------------
* Based on prototype code available under ``references``
  
Install & Run
-------------

From Dockerhub:

.. code-block:: bash

    docker run -it \
               --rm \
               --net=host \
               --pid=host \
	       -e S3_BUCKET=the-bucket-name \
	       -e S3_ACCESS_KEY=the-access-key \
	       -e S3_SECRET_KEY=the-secret-key \
	       -e S3_URL=http://host:port \
	       -e ARD_URL=http://host:port/path \
     	       -e AUX_URL=http://host:port/path \
	       -e CPUS_PER_WORKER=4 \
	       -e HTTP_PORT=5000 \
	       -e WORKERS=4 \
	       -e WORKER_TIMEOUT=12000 \
	       -e MAX_REQUESTS=1000 \
               usgseros/lcmap-blackmagic:1.0

From PyPI:

.. code-block:: bash

    $ pip install lcmap-blackmagic
    $ export S3_BUCKET=the-bucket-name
    $ export S3_ACCESS_KEY=the-access-key
    $ export S3_SECRET_KEY=the-secret-key
    $ export S3_URL=http://host:port
    $ export ARD_URL=http://host:port/path
    $ export AUX_URL=http://host:port/path
    $ export CPUS_PER_WORKER=4
    $ export HTTP_PORT=5000
    $ export WORKERS=4
    $ export WORKER_TIMEOUT=12000
    $ export MAX_REQUESTS=1000
    $ blackmagic.sh

    
From Github:

.. code-block:: bash
		
    $ git clone https://github.com/usgs-eros/lcmap-blackmagic
    $ cd lcmap-blackmagic
    $ conda create --name=blackmagic python=3.7
    $ source activate blackmagic
    $ pip install -e .[test]
    $ export S3_BUCKET=the-bucket-name
    $ export S3_ACCESS_KEY=the-access-key
    $ export S3_SECRET_KEY=the-secret-key
    $ export S3_URL=http://host:port
    $ export ARD_URL=http://host:port/path
    $ export AUX_URL=http://host:port/path
    $ export CPUS_PER_WORKER=4
    $ export HTTP_PORT=5000
    $ export WORKERS=4
    $ export WORKER_TIMEOUT=12000
    $ export MAX_REQUESTS=1000
    $ ./bin/blackmagic.sh

    
Tuning
------
Blackmagic has two primary controls that determine the nature of its parallelism and concurrency: ``WORKERS`` and ``CPUS_PER_WORKER``.

``WORKERS`` controls the number of HTTP listener processes (gunicorn workers) and thus, the number of simultaneous HTTP requests that can be serviced.

``CPUS_PER_WORKER`` controls the number of cores available to each ``WORKER``.

An additional parameter, MAX_REQUESTS, is available to help control the lifespace of each Gunicorn worker.
See http://docs.gunicorn.org/en/stable/settings.html.


Deployment Examples
~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Many slow HTTP requests

    -e WORKERS=<number of cores available>
    -e CPUS_PER_WORKER=1

    # One fast HTTP request
    
    -e WORKERS=1
    -e CPUS_PER_WORKER=<number of cores available>

    
HTTP Requests & Responses
-------------------------
.. code-block:: bash
		
    # /segment resource expects cx (chip x) and cy (chip y) as parameters
    # If parameters are missing /segment returns HTTP 400 with JSON message
		
    $ http --timeout 12000 POST http://localhost:9876/segment cx=1484415 
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

    $ http --timeout 12000 POST http://localhost:9876/segment cy=1484415 
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

    $ http --timeout 12000 POST http://localhost:9876/segment 
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
    
    $ http --timeout 12000 POST http://localhost:9876/segment cx=1484415 cy=-99999999 acquired=1980-01-01/2017-12-31
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
    
    $ http --timeout 12000 POST http://localhost:9876/segment cx=1484415 cy=2414805 acquired=1980/2017-12-31
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
    
    $ http --timeout 1200 POST http://localhost:9876/segment cx=1484415 cy=2414805 acquired=1980/2017-12-31
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

    
Testing
-------
Tests are available in the ``test/`` directory.  To properly test blackmagic
operations, input data and a local S3/Ceph instance are needed.

Input data originates from `lcmap-chipmunk <http://github.com/usgs-eros/lcmap-chipmunk>`_.
Follow the instructions to download, run and load test data onto your local machine.
lcmap-blackmagic requires ARD and AUX data from Chipmunk, so ingest both.

To support testing on external CICD servers, a reverse-proxy NGINX cache is set up
as a project dependency.  Test HTTP requests are sent to NGINX which then serves
lcmap-chipmunk data to the test code.  Responses are stored at ``deps/nginxcache``.
This allows responses to be replayed without lcmap-chipmunk running.

To run the tests:

.. code-block:: bash

    $ make tests    

To update test data held in NGINX cache (requires lcmap-chipmunk running at http://localhost:5656):

.. code-block:: bash
		
   $ make update-test-data

Tests run automatically on every pushed commit to GitHub.  Travis-CI builds will fail and no
Docker image will be pushed if tests do not pass.

See ``Makefile``, ``deps/docker-compose.yml``, ``deps/nginx.conf``, ``.travis.yml``.


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
