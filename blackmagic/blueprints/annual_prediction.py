from blackmagic import cfg
from blackmagic import db
from blackmagic import raise_on
from blackmagic import segaux
from blackmagic import skip_on_empty
from blackmagic import skip_on_exception
from blackmagic import workers

from cytoolz import assoc
from cytoolz import count
from cytoolz import dissoc
from cytoolz import excepts
from cytoolz import do
from cytoolz import first
from cytoolz import get
from cytoolz import get_in
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
from xgboost.core import Booster

import logging
import merlin
import numpy
import xgboost as xgb

logger = logging.getLogger('blackmagic.annual_prediction')
annual_prediction = Blueprint('annual_prediction', __name__)


def log_request(ctx):
    '''Create log message for HTTP request'''

    tx = get('tx', ctx, None)
    ty = get('ty', ctx, None)
    m  = get('month', ctx, None)
    d  = get('day', ctx, None)
    a  = get('acquired', ctx, None)
    c  = get('chips', ctx, None)
    
    logger.info("POST /tile {x},{y},{a},{d},{c}".format(x=tx, y=ty, a=a, d=d, c=c))
        
    return ctx


def exception_handler(ctx, http_status, name, fn):
    try:
        return fn(ctx)
    except Exception as e:        
        return do(logger.exception, {'tx': get('tx', ctx, None),
                                     'ty': get('ty', ctx, None),
                                     'month': get('month', ctx, None),
                                     'day':   get('day', ctx, None),
                                     'acquired': get('acquired', ctx, None),
                                     'chips': len(get('chips', ctx, [])),
                                     'exception': '{name} exception: {ex}'.format(name=name, ex=e),
                                     'http_status': http_status})


def log_chip(segments):
    m = '{{"cx":{cx}, "cy":{cy}, "msg":"generating probabilities"}}'
    
    segments = list(segments)
    logger.info(m.format(**first(segments)))
    
    return segments

    
def prediction_fn(segment, model):
    ind = segment['independent']
    ind = ind.reshape(1, -1) if ind.ndim < 2 else ind

    return assoc(segment,
                 'prob',
                 model.predict(xgb.DMatrix(ind))[0])

    
def predict(segments, model):    
    return list(map(partial(prediction_fn, model=model), segments))


def reformat(segments):
    return map(segaux.prediction_format, segments)


def booster(model_bytes):
    return segaux.booster_from_bytes(model_bytes, {'nthread': 1})


def extract_segments(ctx):
    return ctx['data']


def prediction_pipeline(chip, model_bytes, month, day, acquired, cfg):
    
    return thread_first({'cx': first(chip),
                         'cy': second(chip),
                         'acquired': acquired,
                         'cluster': db.cluster(cfg)},
                        partial(segaux.segments, cfg=cfg),
                        partial(segaux.aux, cfg=cfg),
                        segaux.aux_filter,                        
                        segaux.combine,
                        segaux.unload_segments,
                        segaux.unload_aux,
                        extract_segments,
                        partial(segaux.prediction_dates, month=month, day=day),
                        segaux.average_reflectance,
                        reformat,
                        log_chip,                       
                        partial(predict, model=booster(model_bytes)))


def measure(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        ctx = fn(*args, **kwargs)
        
        d = {"tx":get("tx", ctx, None),
             "ty":get("ty", ctx, None),
             "acquired":get("acquired", ctx, None),
             "month":get("month", ctx, None),
             "day":get("day", ctx, None),
             "chips":"count:{}".format(count(get("chips", ctx, [])))}
            
        logger.info(assoc(d,
                          "{name}_elapsed_seconds".format(name=fn.__name__),
                          (datetime.now() - start).total_seconds()))            
        return ctx
    return wrapper


@raise_on('test_predictions_exception')
@skip_on_exception
@measure
def predictions(ctx, cfg):
    p = partial(prediction_pipeline,
                model_bytes=ctx['model_bytes'],
                month=ctx['month'],
                day=ctx['day'],
                acquired=ctx['acquired'],
                cfg=cfg)

    with workers(cfg) as w:
        logger.info('a')
        a = w.map(p, ctx['chips'])

        # filter out the Nones from the Nones...

        logger.info('b')
        b = filter(lambda x: x is not None, a)
                
        logger.info('c')
        c = flatten(b)

        logger.info('d')
        d = list(c)

        logger.info('e')
        return assoc(ctx, 'predictions', d)
    
        #return assoc(ctx, 'predictions', list(flatten(filter(lambda x: x is not None,
        #                                                     w.map(p, ctx['chips']))))


@skip_on_exception
@measure
def parameters(r):
    '''Check HTTP request parameters'''
    
    tx       = get('tx', r, None)
    ty       = get('ty', r, None)
    acquired = get('acquired', r, None)
    chips    = get('chips', r, None)
    month    = get('month', r, None)
    day      = get('day', r, None)
        
    if (tx is None or
        ty is None or
        acquired is None or
        chips is None or
        month is None or
        day is None):
        raise Exception('tx, ty, acquired, chips, month and day are required parameters')
    else:
        return {'tx': int(tx),
                'ty': int(ty),
                'acquired': acquired,
                'month': month,
                'day': day,
                'chips': list(map(lambda chip: (int(first(chip)), int(second(chip))), chips)),
                'test_data_exception': get('test_data_exception', r, None),
                'test_training_exception': get('test_training_exception', r, None),
                'test_delete_exception': get('test_delete_exception', r, None),
                'test_save_exception': get('test_save_exception', r, None)}

    
@skip_on_exception
def add_cluster(ctx, cfg):
    return assoc(ctx, 'cluster', db.cluster(cfg))


@skip_on_exception
@measure
def load_model(ctx, cfg):

    sess  = db.session(cfg, ctx['cluster']) 
    stmt  = db.select_tile(cfg, ctx['tx'], ctx['ty'])
    
    fn = excepts(StopIteration,
                 lambda session, statement: bytes.fromhex(first(session.execute(statement)).model))

    model = fn(sess, stmt)

    if model is None:
        raise Exception("No model found for tx:{tx} and ty:{ty}".format(**ctx))
    else:
        return assoc(ctx, 'model_bytes', model)

    
@raise_on('test_delete_exception')
@skip_on_exception
@measure
def delete(ctx, cfg):                                                
    '''Delete existing predictions'''

    # delete all existing predictions in the chips we just processed
    cxcy = set(map(lambda x: (x['cx'], x['cy'],), ctx['predictions']))
    deletions = list(map(lambda x: db.delete_annual_predictions(cfg, first(x), second(x)), cxcy))
    db.execute_statements(cfg, deletions)    

    return ctx


@raise_on('test_save_exception')
@skip_on_exception
@measure
def save(ctx, cfg):                                                
    '''Saves annual predictions to Cassandra'''

    # save all new predictions
    db.insert_annual_predictions(cfg, ctx['predictions'])
                
    return ctx


def respond(ctx):
    '''Send the HTTP response'''

    body = {'tx': get('tx', ctx, None),
            'ty': get('ty', ctx, None),
            'acquired': get('acquired', ctx, None),
            'month': get('month', ctx, None),
            'day': get('day', ctx, None),
            'chips': get('chips', ctx, None)}

    e = get('exception', ctx, None)
    
    if e:
        response = jsonify(assoc(body, 'exception', e))
    else:
        response = jsonify(body)

    response.status_code = get('http_status', ctx, 200)

    return response


# xgboost models are not thread safe nor are they sharable
# across processes because the Python object holds a
# reference to the underlying C XGBoost memory locations.

# This means that we should load the model from Cassandra
# one time, decode it from hex into bytes one time,
# but then instantiate a new XGBoost Classifier for
# each process that is going to run.

# In seperate processes, a model can be created,
# data can be loaded, prediction can occur and
# format into correct datastructures,
# then results can be collected from pool.map()
# for persistence into Cassandra using batches

# The models saved to Cassandra are around 100MB,
# so 100MB + space for the segments will be needed
# for each chip in memory.
                
@annual_prediction.route('/annual-prediction', methods=['POST'])        
def annual_predictions():
    
    return thread_first(request.json,
                        partial(exception_handler, http_status=500, name='log_request', fn=log_request),
                        partial(exception_handler, http_status=400, name='parameters', fn=parameters),
                        partial(exception_handler, http_status=500, name='add_cluster', fn=partial(add_cluster, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='load_model', fn=partial(load_model, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='predictions', fn=partial(predictions, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='delete', fn=partial(delete, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='save', fn=partial(save, cfg=cfg)),
                        respond)
