#!/usr/bin/env python3

from blackmagic import db
from cassandra import ConsistencyLevel
from cytoolz import assoc
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
from merlin.functions import flatten
from multiprocessing import Pool
from multiprocessing import Process
from multiprocessing import Manager

import ccd
import json
import logging
import merlin
import os
import sys


cfg = {'cassandra_batch_size': int(os.environ.get('CASSSANDRA_BATCH_SIZE', 1000)),
       'cassandra_host': str(os.environ['CASSANDRA_HOST']).split(','),
       'cassandra_port': int(os.environ.get('CASSANDRA_PORT', 9042)),
       'cassandra_user': os.environ.get('CASSANDRA_USER', 'cassandra'),
       'cassandra_pass': os.environ.get('CASSANDRA_PASS', 'cassandra'),
       'cassandra_keyspace': os.environ['CASSANDRA_KEYSPACE'],
       'cassandra_timeout': float(os.environ.get('CASSANDRA_TIMEOUT', 600)),
       'cassandra_consistency': ConsistencyLevel.name_to_value[os.environ.get('CASSANDRA_CONSISTENCY', 'ALL')],
       'chipmunk_url': os.environ['CHIPMUNK_URL'],
       'log_level': logging.INFO,
       'cpus_per_worker': int(os.environ.get('CPUS_PER_WORKER', 1))}

logging.basicConfig(format='%(asctime)-15s %(name)-15s %(levelname)-8s - %(message)s', level=cfg['log_level'])
logging.getLogger('cassandra.connection').setLevel(logging.ERROR)
logging.getLogger('cassandra.cluster').setLevel(logging.ERROR)
logging.getLogger('cassandra.pool').setLevel(logging.ERROR)
logging.getLogger('cassandra.io').setLevel(logging.ERROR)
logging.getLogger('ccd.procedures').setLevel(logging.ERROR)
logging.getLogger('ccd.change').setLevel(logging.ERROR)
logging.getLogger('lcmap-pyccd').setLevel(logging.ERROR)
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
logger = logging.getLogger('blackmagic.app')

db.setup(cfg)

app = Flask('blackmagic')


def save_chip(detections):
    db.insert_chips(cfg, detections)
    return detections
    

def save_pixels(detections):
    db.insert_pixels(cfg, detections)
    return detections


def save_segments(detections):
    db.insert_segments(cfg, detections)
    return detections


def save_predictions(preds):
    pass


#def default(change_models):
    # if there are no change models, append an empty one to
    # signify that ccd was run for the point, setting start_day and end_day to day 1
    
#    default_value = [{'start_day': 1,
#                      'end_day': 1,
#                      'break_day': 1,
#                      'chprob': 0.0,
#                      'curqa': 0,
#                      'blmag': 0.0,
#                      'grmag': 0.0,
#                      'remag': 0.0,
#                      'nimag': 0.0,
#                      's1mag': 0.0,
#                      's2mag': 0.0,
#                      'thmag': 0.0,
#                      'blrmse': 0.0,
#                      'grrmse': 0.0,
#                      'rermse': 0.0,
#                      'nirmse': 0.0,
#                      's1rmse': 0.0,
#                      's2rmse': 0.0,
#                      'thrmse': 0.0,
#                      'blcoef': [],
#                      'grcoef': [],
#                      'recoef': [],
#                      'nicoef': [],
#                      's1coef': [],
#                      's2coef': [],
#                      'thcoef': [],
#                      'blint': 0.0,
#                      'grint': 0.0,
#                      'reint': 0.0,
#                      'niint': 0.0,
#                      's1int': 0.0,
#                      's2int': 0.0,
#                      'thint': 0.0}]

#    if not change_models or len(change_models) == 0:
#        return default_value
#    else:
#        return change_models


def defaults(cms):
    return [{}] if (not cms or len(cms) == 0) else cms

    
def coefficients(change_model, spectra):
    coefs = get_in([spectra, 'coefficients'], change_model)
    return list(coefs) if coefs else []


def format(cx, cy, px, py, dates, ccdresult):
    
    return [
             {'cx'     : int(cx),
              'cy'     : int(cy),
              'px'     : int(px),
              'py'     : int(py),
              'sday'   : date.fromordinal(get('start_day', cm, 1)).isoformat(),
              'eday'   : date.fromordinal(get('end_day', cm, 1)).isoformat(),
              'bday'   : date.fromordinal(get('break_day', cm, 1)).isoformat(),
              'chprob' : get('change_probability', cm, 0.0),
              'curqa'  : get('curve_qa', cm, 0),
              'blmag'  : get_in(['blue', 'magnitude'], cm, 0.0),
              'grmag'  : get_in(['green', 'magnitude'], cm, 0.0),
              'remag'  : get_in(['red', 'magnitude'], cm, 0.0),
              'nimag'  : get_in(['nir', 'magnitude'], cm, 0.0),
              's1mag'  : get_in(['swir1', 'magnitude'], cm, 0.0),
              's2mag'  : get_in(['swir2', 'magnitude'], cm, 0.0),
              'thmag'  : get_in(['thermal', 'magnitude'], cm, 0.0),
              'blrmse' : get_in(['blue', 'rmse'], cm, 0.0),
              'grrmse' : get_in(['green', 'rmse'], cm, 0.0),
              'rermse' : get_in(['red', 'rmse'], cm, 0.0),
              'nirmse' : get_in(['nir', 'rmse'], cm, 0.0),
              's1rmse' : get_in(['swir1', 'rmse'], cm, 0.0),
              's2rmse' : get_in(['swir2', 'rmse'], cm, 0.0),
              'thrmse' : get_in(['thermal', 'rmse'], cm, 0.0),
              'blcoef' : coefficients(cm, 'blue'),
              'grcoef' : coefficients(cm, 'green'),
              'recoef' : coefficients(cm, 'red'),
              'nicoef' : coefficients(cm, 'nir'),
              's1coef' : coefficients(cm, 'swir1'),
              's2coef' : coefficients(cm, 'swir2'),
              'thcoef' : coefficients(cm, 'thermal'),
              'blint'  : get_in(['blue', 'intercept'], cm, 0.0),
              'grint'  : get_in(['green', 'intercept'], cm, 0.0),
              'reint'  : get_in(['red', 'intercept'], cm, 0.0),
              'niint'  : get_in(['nir', 'intercept'], cm, 0.0),
              's1int'  : get_in(['swir1', 'intercept'], cm, 0.0),
              's2int'  : get_in(['swir2', 'intercept'], cm, 0.0),
              'thint'  : get_in(['thermal', 'intercept'], cm, 0.0),
              'dates'  : [date.fromordinal(o).isoformat() for o in dates],
              'mask'   : get('processing_mask', ccdresult)}
        
             for cm in defaults(get('change_models', ccdresult, [{},]))]


def detect(timeseries):
   
    cx, cy, px, py = first(timeseries)

    return format(cx=cx,
                  cy=cy,
                  px=px,
                  py=py,
                  dates=get('dates', second(timeseries)),
                  ccdresult=ccd.detect(**second(timeseries)))


def delete_detections(timeseries):
    cx, cy, _, _ = first(first(timeseries))
    try:
        x = int(cx)
        y = int(cy)
        db.execute_statements(cfg, [db.delete_chip(cfg, x, y),
                                    db.delete_pixel(cfg, x, y),
                                    db.delete_segment(cfg, x, y)])
    except Exception as e:
        logger.exception('Exception deleting partition for cx:{cx} cy:{cy}'.format(cx=x, cy=y))
        raise e
    return timeseries


def workers(cfg):
    return Pool(cfg['cpus_per_worker'])


def measure(name, start_time, cx, cy, acquired):
    e = datetime.now()
    d = {'cx': cx, 'cy': cy, 'acquired': acquired}
    d = assoc(d, '{name}_elapsed_seconds'.format(name=name), (e - start_time).total_seconds())
    logger.info(d)
    return d


@app.route('/health', methods=['GET'])
def health():
    return jsonify(True)


@app.route('/annual_prediction')
def prediction():
    return jsonify('annual_prediction')


@app.route('/segment', methods=['POST'])
def segment():
    segment_start = datetime.now()
    r = request.json
    x = get('cx', r, None)
    y = get('cy', r, None)
    a = get('acquired', r, None)
    n = int(get('n', r, 10000))
    
    if (x is None or y is None or a is None):
        response = jsonify({'cx': x, 'cy': y, 'acquired': a, 'msg': 'cx, cy, and acquired are required parameters'})
        response.status_code = 400
        return response

    logger.info('POST /segment {x},{y},{a}'.format(x=x, y=y, a=a))

    merlin_start = datetime.now()
    try:
        timeseries = merlin.create(x=x,
                                   y=y,
                                   acquired=a,
                                   cfg=merlin.cfg.get(profile='chipmunk-ard',
                                                      env={'CHIPMUNK_URL': cfg['chipmunk_url']}))
    except Exception as ex:
        measure('merlin_exception', merlin_start, x, y, a)
        logger.exception('Merlin exception in /segment:{}'.format(ex))
        response = jsonify({'cx': x, 'cy': y, 'acquired': a, 'msg': str(ex)})
        response.status_code = 500
        return response
    
    if count(timeseries) == 0:
        measure('merlin_no_input_data_exception', merlin_start, x, y, a)
        logger.warning('No input data for {cx},{cy},{a}'.format(cx=x, cy=y, a=a))
        response = jsonify({'cx': x, 'cy': y, 'acquired': a, 'msg': 'no input data'})
        response.status_code = 500
        return response
    
    measure('merlin', merlin_start, x, y, a)

    detection_start = None
    cassandra_start = None
    detections = None

    try:
        with workers(cfg) as __workers:
            detection_start = datetime.now()
            detections = list(flatten(__workers.map(detect, take(n, delete_detections(timeseries)))))
            measure('detection', detection_start, x, y, a)
    except Exception as ex:
        measure('detection_exception', detection_start, x, y, a)
        logger.exception("Detection exception in /segment:{}".format(ex))
        response = jsonify({'cx': x, 'cy': y, 'acquired': a, 'msg': str(ex)})
        response.status_code = 500
        return response
    
    try:    
        cassandra_start = datetime.now()
        save_segments(save_pixels(save_chip(detections)))
        measure('cassandra', cassandra_start, x, y, a)    
        return jsonify({'cx': x, 'cy': y, 'acquired': a})
    except Exception as ex:
        measure('cassandra_exception', cassandra_start, x, y, a)
        logger.exception("Cassandra exception in /segment:{}".format(ex))
        response = jsonify({'cx': x, 'cy': y, 'acquired': a, 'msg': str(ex)})
        response.status_code = 500
        return response
