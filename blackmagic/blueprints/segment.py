from blackmagic import skip_on_exception
from blackmagic import workers
from blackmagic.data import ceph
from cytoolz import assoc
from cytoolz import count
from cytoolz import do
from cytoolz import excepts
from cytoolz import first
from cytoolz import get
from cytoolz import get_in
from cytoolz import merge
from cytoolz import partial
from cytoolz import second
from cytoolz import take
from cytoolz import thread_first
from datetime import date
from datetime import datetime
from flask import Blueprint
from flask import jsonify
from flask import request
from functools import wraps
from merlin.functions import flatten

import blackmagic
import ccd
import logging
import merlin
import os
import sys

cfg     = merge(blackmagic.cfg, ceph.cfg)
logger  = logging.getLogger('blackmagic.segment')
segment = Blueprint('segment', __name__)
_ceph   = ceph.Ceph(cfg)
_ceph.start()

def save_chip(ctx, cfg):
    _ceph.insert_chip(ctx['detections'])
    return ctx
    

def save_pixels(ctx, cfg):
    _ceph.insert_pixels(ctx['detections'])
    return ctx


def save_segments(ctx, cfg):
    _ceph.insert_segments(ctx['detections'])
    return ctx


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
        
             for cm in defaults(get('change_models', ccdresult, None))]


def detect(timeseries):
   
    cx, cy, px, py = first(timeseries)

    return format(cx=cx,
                  cy=cy,
                  px=px,
                  py=py,
                  dates=get('dates', second(timeseries)),
                  ccdresult=ccd.detect(**second(timeseries)))

def measure(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        ctx = fn(*args, **kwargs)
        
        d = {'cx': get('cx', ctx, None),
             'cy': get('cy', ctx, None),
             'acquired': get('acquired', ctx, None)}
            
        logger.info(assoc(d,
                          '{name}_elapsed_seconds'.format(name=fn.__name__),
                          (datetime.now() - start).total_seconds()))            
        return ctx
    return wrapper


def log_request(ctx):

    cx = get('cx', ctx, None)
    cy = get('cy', ctx, None)
    a  = get('acquired', ctx, None)
    
    logger.info('POST /segment {cx}, {cy}, {a}'.format(cx=cx, cy=cy, a=a))
    
    return ctx


def parameters(r):
    cx       = get('cx', r, None)
    cy       = get('cy', r, None)
    acquired = get('acquired', r, None)

    test_pixel_count         = int(get('test_pixel_count', r, 10000))
    test_detection_exception = get('test_detection_exception', r, None)
    test_cassandra_exception = get('test_cassandra_exception', r, None)
    
    if (cx is None or cy is None or acquired is None):
        raise Exception('cx, cy and acquired are required parameters')
    else:
        return {'cx': int(cx),
                'cy': int(cy),
                'acquired': acquired,
                'test_pixel_count': test_pixel_count,
                'test_detection_exception': test_detection_exception,
                'test_cassandra_exception': test_cassandra_exception}


@skip_on_exception
@measure
def timeseries(ctx, cfg):
    
    return merge(ctx,
                 {'timeseries': merlin.create(x=ctx['cx'],
                                              y=ctx['cy'],
                                              acquired=ctx['acquired'],
                                              cfg=merlin.cfg.get(profile='chipmunk-ard',
                                                                 env={'CHIPMUNK_URL': cfg['ard_url']}))})


@skip_on_exception
def nodata(ctx, cfg):
    
    if len(ctx['timeseries']) == 0:
        raise Exception('No timeseries data available')
    else:
        return ctx
    

@skip_on_exception
@measure
def detection(ctx, cfg):

    with workers(cfg) as w:
        if get('test_detection_exception', ctx, None) is not None:
            return merge(ctx, exception(msg='test_detection_exception', http_status=500))
        else:
            return merge(ctx, {'detections': list(flatten(w.map(detect, take(ctx['test_pixel_count'], ctx['timeseries']))))})

    
@skip_on_exception
def delete(ctx, cfg):
    cx = int(get('cx', ctx))
    cy = int(get('cy', ctx))

    _ceph.delete_chip(cx, cy)
    _ceph.delete_pixels(cx, cy)
    _ceph.delete_segments(cx, cy)

    return ctx


@skip_on_exception
@measure
def save(ctx, cfg):
    
    if get('test_cassandra_exception', ctx, None) is not None:
        raise Exception('test_cassandra_exception')
    else:
        save_chip(ctx, cfg)
        save_pixels(ctx, cfg)
        save_segments(ctx, cfg)
        return ctx


def respond(ctx):
    
    body = {'cx': get('cx', ctx, None),
            'cy': get('cy', ctx, None),
            'acquired': get('acquired', ctx, None)}

    e = get('exception', ctx, None)
    
    if e:
        response = jsonify(assoc(body, 'exception', e))
    else:
        response = jsonify(body)

    response.status_code = get('http_status', ctx, 200)

    return response


def exception_handler(ctx, http_status, name, fn):
    try:
        return fn(ctx)
    except Exception as e:
        
        return do(logger.error, {'cx': get('cx', ctx, None),
                                 'cy': get('cy', ctx, None),
                                 'acquired': get('acquired', ctx, None),
                                 'exception': '{name} exception: {ex}'.format(name=name, ex=e),
                                 'http_status': http_status})

    
@segment.route('/segment', methods=['POST'])
def segments():

    return thread_first(request.json,
                        partial(exception_handler, http_status=500, name='log_request', fn=log_request),
                        partial(exception_handler, http_status=400, name='parameters', fn=parameters),
                        partial(exception_handler, http_status=500, name='timeseries', fn=partial(timeseries, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='nodata', fn=partial(nodata, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='detection', fn=partial(detection, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='delete', fn=partial(delete, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='save', fn=partial(save, cfg=cfg)),
                        respond)
