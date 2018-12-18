#!/usr/bin/env python3

from blackmagic import cfg
from blackmagic.blueprints.segment import segments
from blackmagic import db
from cassandra import ConsistencyLevel
from cytoolz import count
from cytoolz import excepts
from cytoolz import first
from cytoolz import get
from cytoolz import get_in
from cytoolz import partial
from cytoolz import second
from cytoolz import take
from datetime import date
from datetime import datetime
from flask import Flask
from flask import jsonify
from flask import request
from multiprocessing import Pool
from multiprocessing import Process
from multiprocessing import Manager

import json
import logging
import merlin
import os
import sys

logging.basicConfig(format='%(asctime)-15s %(name)-15s %(levelname)-8s - %(message)s', level=cfg['log_level'])
logging.getLogger('cassandra.connection').setLevel(logging.ERROR)
logging.getLogger('cassandra.cluster').setLevel(logging.ERROR)
logger = logging.getLogger('blackmagic.app')

db.setup(cfg)

app = Flask('blackmagic')


def savepredictions(preds):
    pass


def queue():
    return Manager().Queue()


def workers(cfg):
    return Pool(cfg['cpus_per_worker'])


def writers(cfg, q):
    w = [Process(name='cassandra-writer[{}]'.format(i),
                 target=db.writer,
                 kwargs={'cfg': cfg, 'q': q},
                 daemon=False)
         for i in range(cfg['cassandra_concurrent_writes'])]
    [writer.start() for writer in w]
    return w

        
@app.route('/health', methods=['GET'])
def health():
    return jsonify(True)


@app.route('/annual_prediction')
def prediction():
    return jsonify('annual_prediction')


@app.route('/tile')
def tile():
    r = request.json
    x = get('x', r, None)
    y = get('y', r, None)
    n = get('n', r, None)
    pass


@app.route('/segment', methods=['POST'])
def segment():

    r = request.json
    x = get('cx', r, None)
    y = get('cy', r, None)
    a = get('acquired', r, None)
    n = get('n', r, 10000)
    
    if (x is None or y is None or a is None):
        response = jsonify({'cx': x, 'cy': y, 'acquired': a, 'msg': 'cx, cy, and acquired are required parameters'})
        response.status_code = 400
        return response

    logger.info('POST /segment {x},{y},{a}'.format(x=x, y=y, a=a))

    try:
        timeseries = merlin.create(x=x,
                                   y=y,
                                   acquired=a,
                                   cfg=merlin.cfg.get(profile='chipmunk-ard',
                                                      env={'CHIPMUNK_URL': cfg['chipmunk_url']}))
    except Exception as ex:
        response = jsonify({'cx': x, 'cy': y, 'acquired': a, 'msg': ex})
        response.status_code = 400
        return response
    
    if count(timeseries) == 0:
        response = jsonify({'cx': x, 'cy': y, 'acquired': a, 'msg': 'no input data'})
        response.status_code = 400
        return response
    
    __queue   = None
    __writers = None
    __workers = None
    
    try:
        __queue   = queue()
        __writers = writers(cfg, __queue)
        __workers = workers(cfg)

        __workers.map(partial(changedetection.pipeline, q=__queue),
                      take(n, changedetection.delete_detections(timeseries)))

        return jsonify({'cx': x, 'cy': y, 'acquired': a})
    
    except Exception as e:
        logger.exception(e)
        raise e
    finally:

        logger.debug('stopping writers')
        [__queue.put('STOP_WRITER') for w in __writers]
        [w.terminate() for w in __writers]
        [w.join() for w in __writers]
        
        logger.debug('stopping workers')
        __workers.terminate()
        __workers.join()
       
