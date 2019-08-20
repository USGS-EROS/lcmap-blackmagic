from blackmagic import raise_on
from blackmagic import segaux
from blackmagic import skip_on_empty
from blackmagic import skip_on_exception
from blackmagic import workers
from blackmagic.data import ceph

from cytoolz import assoc
from cytoolz import count
from cytoolz import dissoc
from cytoolz import excepts
from cytoolz import do
from cytoolz import first
from cytoolz import get
from cytoolz import get_in
from cytoolz import groupby
from cytoolz import merge
from cytoolz import partial
from cytoolz import second
from cytoolz import thread_first
from datetime import datetime
from flask import Blueprint
from flask import jsonify
from flask import request
from functools import wraps
from merlin.functions import flatten
from operator import add
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential
from xgboost.core import Booster

import blackmagic
import logging
import merlin
import numpy
import xgboost as xgb

logger = logging.getLogger('blackmagic.prediction')
prediction = Blueprint('prediction', __name__)

cfg = merge(blackmagic.cfg, ceph.cfg)

_ceph = ceph.Ceph(cfg)
_ceph.start()

def log_request(ctx):
    '''Create log message for HTTP request'''

    tx = get('tx', ctx, None)
    ty = get('ty', ctx, None)
    cx = get('cx', ctx, None)
    cy = get('cy', ctx, None)
    m  = get('month', ctx, None)
    d  = get('day', ctx, None)
    a  = get('acquired', ctx, None)
    
    logger.info("POST /prediction {tx},{ty},{cx},{cy},{m},{d},{a}".format(tx=tx,
                                                                          ty=ty,
                                                                          cx=cx,
                                                                          cy=cy,
                                                                          m=m,
                                                                          d=d,
                                                                          a=a))
    return ctx


def exception_handler(ctx, http_status, name, fn):
    try:
        return fn(ctx)
    except Exception as e:        
        return do(logger.exception, {'tx': get('tx', ctx, None),
                                     'ty': get('ty', ctx, None),
                                     'cx': get('cx', ctx, None),
                                     'cy': get('cy', ctx, None),
                                     'month': get('month', ctx, None),
                                     'day':   get('day', ctx, None),
                                     'acquired': get('acquired', ctx, None),
                                     'exception': '{name} exception: {ex}'.format(name=name, ex=e),
                                     'http_status': http_status})


def reformat(segments):
    return map(segaux.prediction_format, segments)


def booster(cfg, model_bytes):
    return segaux.booster_from_bytes(model_bytes,
                                     {'nthread': get_in(['xgboost', 'parameters', 'nthread'], cfg)})


def extract_segments(ctx):
    return ctx['data']


def measure(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        ctx = fn(*args, **kwargs)
        
        d = {"tx":get("tx", ctx, None),
             "ty":get("ty", ctx, None),
             "cx":get("cx", ctx, None),
             "cy":get("cy", ctx, None),
             "acquired":get("acquired", ctx, None),
             "month":get("month", ctx, None),
             "day":get("day", ctx, None)}
            
        logger.info(assoc(d,
                          "{name}_elapsed_seconds".format(name=fn.__name__),
                          (datetime.now() - start).total_seconds()))            
        return ctx
    return wrapper


@retry(retry=retry_if_exception_type(Exception),
       stop=stop_after_attempt(10),
       reraise=True,
       wait=wait_random_exponential(multiplier=1, max=60))
def segments(ctx, cfg):
    '''Return segments stored in Cassandra'''

    return assoc(ctx, 'segments', _ceph.select_segments(ctx['cx'], ctx['cy']))


@raise_on('test_load_data_exception')
@skip_on_exception
@measure
def load_data(ctx, cfg):
    return assoc(ctx,
                 'data',
                 thread_first(ctx,
                              partial(segments, cfg=cfg),
                              partial(segaux.aux, cfg=cfg),
                              segaux.combine,
                              segaux.unload_segments,
                              segaux.unload_aux,
                              extract_segments,
                              partial(segaux.prediction_dates,
                                      month=get("month", ctx),
                                      day=get("day", ctx)),
                              segaux.average_reflectance,
                              reformat))


@raise_on('test_load_model_exception')
@skip_on_exception
@measure
def load_model(ctx):
    
    fn = excepts(Exception,
                 lambda _: bytes.fromhex(first(_ceph.select_tile(ctx['tx'], ctx['ty']))['model']))

    model = fn(None)

    if model is None:
        raise Exception("No model found for tx:{tx} and ty:{ty}".format(**ctx))
    else:
        return assoc(ctx, 'model_bytes', model)
    

@raise_on('test_group_data_exception')
@skip_on_exception
@measure
def group_data(ctx):
    grouper = lambda x: 'defaults' if x['sday'] == '0001-01-01' and x['eday'] == '0001-01-01' else 'data'
    groups  = groupby(grouper, ctx['data'])
    return merge(ctx,
                 {'data': get('data', groups, []),
                  'defaults': get('defaults', groups, [])})
    

@raise_on('test_matrix_exception')
@skip_on_exception
@measure
def matrix(ctx):
    
    return assoc(ctx,
                 'ndata',
                 numpy.array([d['independent'] for d in ctx['data']], dtype='float32'))

    
@raise_on('test_prediction_exception')
@skip_on_exception
@measure
def predictions(ctx, cfg):
    model = booster(cfg, get('model_bytes', ctx))
    probs = model.predict(xgb.DMatrix(ctx['ndata'])) if len(ctx['ndata']) > 0 else []
    preds = []
    
    for i,v in enumerate(probs):
        preds.append(assoc(ctx['data'][i], 'prob', v))

    return assoc(ctx, 'predictions', preds)

    #############################################################
    # relying on position in a collection is highly error prone,
    # but in this case we have to use xgboost parallelization for
    # performance so there's no other way to do it.
    #
    # Previous implementation used Python workers and resulted
    # in a chip runtime of approximately 970 seconds for
    # predictions over 1982-2017 for a single cx,cy.
    #
    #############################################################
    # Python worker method:
    #
    # def prediction_fn(segment, model):
    #     ind = segment['independent']
    #     ind = ind.reshape(1, -1) if ind.ndim < 2 else ind
    #
    #     return assoc(segment,
    #                  'prob',
    #                  model.predict(xgb.DMatrix(ind))[0])
    #
    #
    # def predict(segment, model_bytes):
    #     model = booster(model_bytes)    
    #     return prediction_fn(segment, model)
    #
    #
    # p = partial(predict, model_bytes=get('model_bytes', ctx))             
    # with workers(cfg) as w:
    #    return assoc(ctx,
    #                 'predictions',
    #                 list(filter(lambda x: x is not None,
    #                             w.map(p, ctx['data']))))
    #############################################################

                 
@raise_on('test_default_predictions_exception')
@skip_on_exception
@measure
def default_predictions(ctx):
    default = lambda x: assoc(x, 'prob', [])
    return assoc(ctx,
                 'predictions',
                 add(list(map(default, get('defaults', ctx, []))),
                     ctx['predictions']))

                 
@skip_on_exception
@measure
def parameters(r):
    '''Check HTTP request parameters'''
    
    tx       = get('tx', r, None)
    ty       = get('ty', r, None)
    acquired = get('acquired', r, None)
    cx       = get('cx', r, None)
    cy       = get('cy', r, None)
    month    = get('month', r, None)
    day      = get('day', r, None)
        
    if (tx is None or
        ty is None or
        acquired is None or
        cx is None or
        cy is None or
        month is None or
        day is None):
        raise Exception('tx, ty, cx, cy, acquired, month and day are required parameters')
    else:
        return {'tx': int(tx),
                'ty': int(ty),
                'acquired': acquired,
                'month': month,
                'day': day,
                'cx': int(cx),
                'cy': int(cy),
                'test_load_model_exception': get('test_load_model_exception', r, None),
                'test_load_data_exception': get('test_load_data_exception', r, None),
                'test_group_data_exception': get('test_group_data_exception', r, None),
                'test_matrix_exception': get('test_matrix_exception', r, None),
                'test_prediction_exception': get('test_prediction_exception', r, None),
                'test_default_predictions_exception': get('test_default_predictions_exception', r, None),
                'test_delete_exception': get('test_delete_exception', r, None),
                'test_save_exception': get('test_save_exception', r, None)}

        
@raise_on('test_delete_exception')
@skip_on_exception
@measure
def delete(ctx, cfg):                                                
    '''Delete existing predictions'''

    _ceph.delete_predictions(ctx['cx'], ctx['cy'])
    
    return ctx


@raise_on('test_save_exception')
@skip_on_exception
@measure
def save(ctx, cfg):                                                
    '''Saves predictions to Cassandra'''
    
    # save all new predictions
    #_ceph.insert_predictions(merlin.functions.denumpify(cx=ctx['cx'], cy=ctx['cy'], ctx['predictions']))
    _ceph.insert_predictions(merlin.functions.denumpify(ctx['predictions']))
                
    return ctx


def respond(ctx):
    '''Send the HTTP response'''

    body = {'tx': get('tx', ctx, None),
            'ty': get('ty', ctx, None),
            'acquired': get('acquired', ctx, None),
            'month': get('month', ctx, None),
            'day': get('day', ctx, None),
            'cx': get('cx', ctx, None),
            'cy': get('cy', ctx, None)}

    e = get('exception', ctx, None)
    
    if e:
        response = jsonify(assoc(body, 'exception', e))
    else:
        response = jsonify(body)

    response.status_code = get('http_status', ctx, 200)

    return response

                
@prediction.route('/prediction', methods=['POST'])        
def predictions_route():
    
    return thread_first(request.json,
                        partial(exception_handler, http_status=500, name='log_request', fn=log_request),
                        partial(exception_handler, http_status=400, name='parameters', fn=parameters),
                        partial(exception_handler, http_status=500, name='load_model', fn=load_model),
                        partial(exception_handler, http_status=500, name='load_data', fn=partial(load_data, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='group_data', fn=group_data),
                        partial(exception_handler, http_status=500, name='matrix', fn=matrix),
                        partial(exception_handler, http_status=500, name='predictions', fn=partial(predictions, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='default_predictions', fn=default_predictions),
                        partial(exception_handler, http_status=500, name='delete', fn=partial(delete, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='save', fn=partial(save, cfg=cfg)),
                        respond)
