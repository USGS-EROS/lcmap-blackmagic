#!/usr/bin/env python3

from blackmagic import db
from cassandra import ConsistencyLevel
from cytoolz import count
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

import ccd
import json
import logging
import merlin
import os
import sys


cfg = {'cassandra_host': os.environ['CASSANDRA_HOST'],
       'cassandra_port': int(os.environ.get('CASSANDRA_PORT', 9042)),
       'cassandra_user': os.environ.get('CASSANDRA_USER', 'cassandra'),
       'cassandra_pass': os.environ.get('CASSANDRA_PASS', 'cassandra'),
       'cassandra_keyspace': os.environ['CASSANDRA_KEYSPACE'],
       'cassandra_timeout': float(os.environ.get('CASSANDRA_TIMEOUT', 600)),
       'cassandra_consistency': ConsistencyLevel.name_to_value[os.environ.get('CASSANDRA_CONSISTENCY', 'QUORUM')],
       'cassandra_concurrent_writes': int(os.environ.get('CASSANDRA_CONCURRENT_WRITES', 1)),
       'chipmunk_url': os.environ['CHIPMUNK_URL'],
       'log_level': logging.INFO,
       'cpus_per_worker': int(os.environ.get('CPUS_PER_WORKER', 1))}

logging.basicConfig(format='%(asctime)-15s %(name)-15s %(levelname)-8s - %(message)s', level=cfg['log_level'])
logging.getLogger('cassandra.connection').setLevel(logging.ERROR)
logging.getLogger('cassandra.cluster').setLevel(logging.ERROR)
logger = logging.getLogger('blackmagic.app')

db.setup(cfg)

app = Flask('blackmagic')

def saveccd(detection, q):
    q.put(db.insert_chip(cfg, detection))
    q.put(db.insert_pixel(cfg, detection))
    q.put(db.insert_segment(cfg, detection))
    return detection


def savepredictions(preds):
    pass


def default(change_models):
    # if there are no change models, append an empty one to
    # signify that ccd was run for the point, setting start_day and end_day to day 1

    return [{'start_day': 1, 'end_day': 1, 'break_day': 1}] if not change_models else change_models

    
def format(cx, cy, px, py, dates, ccdresult):
    logger.debug('formatting {},{},{},{}'.format(cx, cy, px, py))
    return [
             {'cx'     : int(cx),
              'cy'     : int(cy),
              'px'     : int(px),
              'py'     : int(py),
              'sday'   : date.fromordinal(get('start_day', cm)).isoformat(),
              'eday'   : date.fromordinal(get('end_day', cm)).isoformat(),
              'bday'   : date.fromordinal(get('break_day', cm, None)).isoformat(),
              'chprob' : get('change_probability', cm, None),
              'curqa'  : get('curve_qa', cm, None),
              'blmag'  : get_in(['blue', 'magnitude'], cm, None),
              'grmag'  : get_in(['green', 'magnitude'], cm, None),
              'remag'  : get_in(['red', 'magnitude'], cm, None),
              'nimag'  : get_in(['nir', 'magnitude'], cm, None),
              's1mag'  : get_in(['swir1', 'magnitude'], cm, None),
              's2mag'  : get_in(['swir2', 'magnitude'], cm, None),
              'thmag'  : get_in(['thermal', 'magnitude'], cm, None),
              'blrmse' : get_in(['blue', 'rmse'], cm, None),
              'grrmse' : get_in(['green', 'rmse'], cm, None),
              'rermse' : get_in(['red', 'rmse'], cm, None),
              'nirmse' : get_in(['nir', 'rmse'], cm, None),
              's1rmse' : get_in(['swir1', 'rmse'], cm, None),
              's2rmse' : get_in(['swir2', 'rmse'], cm, None),
              'thrmse' : get_in(['thermal', 'rmse'], cm, None),
              'blcoef' : list(get_in(['blue', 'coefficients'], cm, None)),
              'grcoef' : list(get_in(['green', 'coefficients'], cm, None)),
              'recoef' : list(get_in(['red', 'coefficients'], cm, None)),
              'nicoef' : list(get_in(['nir', 'coefficients'], cm, None)),
              's1coef' : list(get_in(['swir1', 'coefficients'], cm, None)),
              's2coef' : list(get_in(['swir2', 'coefficients'], cm, None)),
              'thcoef' : list(get_in(['thermal', 'coefficients'], cm, None)),
              'blint'  : get_in(['blue', 'intercept'], cm, None),
              'grint'  : get_in(['green', 'intercept'], cm, None),
              'reint'  : get_in(['red', 'intercept'], cm, None),
              'niint'  : get_in(['nir', 'intercept'], cm, None),
              's1int'  : get_in(['swir1', 'intercept'], cm, None),
              's2int'  : get_in(['swir2', 'intercept'], cm, None),
              'thint'  : get_in(['thermal', 'intercept'], cm, None),
              'dates'  : [date.fromordinal(o).isoformat() for o in dates],
              'mask'   : get('processing_mask', ccdresult, None)}
             for cm in default(get('change_models', ccdresult, None))]


def detect(timeseries):
   
    cx, cy, px, py = first(timeseries)

    return format(cx=cx,
                  cy=cy,
                  px=px,
                  py=py,
                  dates=get('dates', second(timeseries)),
                  ccdresult=ccd.detect(**second(timeseries)))


def pipeline(x, q):
    return count(map(partial(saveccd, q=q), detect(x)))


def delete_detections(timeseries):
    cx, cy, _, _ = first(first(timeseries))
    try:
        x = int(cx)
        y = int(cy)
        _ = db.execute(cfg, db.delete_chip(cfg, x, y))
        _ = db.execute(cfg, db.delete_pixel(cfg, x, y))
        _ = db.execute(cfg, db.delete_segment(cfg, x, y))
        
    except Exception as e:
        logger.exception('Exception deleting partition for x:{cx} y:{cy}'.format(cx=x, cy=y))

    return timeseries


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


@app.route('/segment', methods=['POST'])
def segment():

    r = request.json
    x = r['cx']
    y = r['cy']
    n = get('n', r, 10000)

    a='0001-01-01/{}'.format(date.today().isoformat())
    
    logger.info('POST /segment {x},{y}'.format(x=x, y=y))

    timeseries = partial(merlin.create,
                         x=x,
                         y=y,
                         acquired=a,
                         cfg=merlin.cfg.get(profile='chipmunk-ard',
                                            env={'CHIPMUNK_URL': cfg['chipmunk_url']}))
    
    __queue   = None
    __writers = None
    __workers = None
    
    try:
        __queue   = queue()
        __writers = writers(cfg, __queue)
        __workers = workers(cfg)

        __workers.map(partial(pipeline, q=__queue),
                      take(n, delete_detections(timeseries())))

        return jsonify({'cx': x, 'cy': y})
    
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
       
