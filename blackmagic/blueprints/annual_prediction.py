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
    d  = get('date', ctx, None)
    c  = get('chips', ctx, None)
    
    logger.info("POST /tile {x},{y},{a},{d},{c}".format(x=tx, y=ty, a=a, d=d, c=c))
        
    return ctx


def exception_handler(ctx, http_status, name, fn):
    try:
        return fn(ctx)
    except Exception as e:        
        return do(logger.exception, {'tx': get('tx', ctx, None),
                                     'ty': get('ty', ctx, None),
                                     'date': get('date', ctx, None),
                                     'chips': get('chips', ctx, None),
                                     'exception': '{name} exception: {ex}'.format(name=name, ex=e),
                                     'http_status': http_status})
    
def prediction_function(data, model):
    return model.predict(xgb.DMatrix(data))[0]

    
def predict(ctx):
    model = create_model_from_bytes(ctx['model_bytes'])
    fn = partial(prediction_function, model=model)
    return assoc(ctx, 'predictions', map(fn, ctx['data']))


def pipeline(chip, tx, ty, date, acquired, cfg):

    ctx = {'tx': tx,
           'ty': ty,
           'cx': first(chip),
           'cy': second(chip),
           'date': date,
           'acquired': acquired,
           'cluster': cluster(cfg)}

    # {'cx': 0, 'cy': 0, 'date': '2001/07/01', aux:{}, segments:[], data:[]}

    return thread_first(ctx,
                        partial(segaux.segments, cfg=cfg),
                        partial(segaux.aux, cfg=cfg),
                        segaux.aux_filter,                        
                        segaux.combine,
                        segaux.unload_segments,
                        segaux.unload_aux,
                        segaux.prediction_format,
                        segaux.log_chip,
                        predict)
    
def measure(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        ctx = fn(*args, **kwargs)
        
        d = {"tx":get("tx", ctx, None),
             "ty":get("ty", ctx, None),
             "date":get("date", ctx, None),
             "chips":"count:{}".format(count(get("chips", ctx, [])))}
            
        logger.info(assoc(d,
                          "{name}_elapsed_seconds".format(name=fn.__name__),
                          (datetime.now() - start).total_seconds()))            
        return ctx
    return wrapper                 


@skip_on_exception
@measure
def parameters(r):
    '''Check HTTP request parameters'''
    
    tx       = get('tx', r, None)
    ty       = get('ty', r, None)
    acquired = get('acquired', r, None)
    chips    = get('chips', r, None)
    date     = get('date', r, None)
        
    if (tx is None or ty is None or acquired is None or chips is None or date is None):
        raise Exception('tx, ty, acquired, chips and date are required parameters')
    else:
        return {'tx': int(tx),
                'ty': int(ty),
                'date': date,
                'chips': list(map(lambda chip: (int(first(chip)), int(second(chip))), chips)),
                'test_data_exception': get('test_data_exception', r, None),
                'test_training_exception': get('test_training_exception', r, None),
                'test_cassandra_exception': get('test_cassandra_exception', r, None)}

            
@skip_on_exception
@raise_on('test_load_model_exception')
@measure
def load_model(ctx, cfg):

    sess  = db.session(cfg, ctx['cluster']) 
    stmt  = db.select_tile(cfg, ctx['tx'], ctx['ty'])
    model = get('model', first(sess.execute(stmt), None)
                
    return assoc(ctx, 'model_bytes', bytes.fromhex(model))
                 

@skip_on_exception
@raise_on('test_load_data_exception')
@measure
def load_data(ctx, cfg):
    '''Retrieve a tile's worth of segments to predict'''
    
    p = partial(pipeline,
                tx=ctx['tx'],
                ty=ctx['ty'],
                date=ctx['date'],
                acquired=ctx['acquired'],
                cfg=cfg)

    with workers(cfg) as w:
        return assoc(ctx, 'data', numpy.array(list(flatten(w.map(p, ctx['chips']))), dtype=numpy.float32))


@raise_on('test_training_exception')
@skip_on_exception
@skip_on_empty('independent')
@skip_on_empty('dependent')
@measure
def predict(ctx, cfg):
    '''Train an xgboost model'''
    return ctx
    

@raise_on('test_cassandra_exception')
@skip_on_exception
@skip_on_empty('model')
@measure
def save(ctx, cfg):                                                
    '''Saves an xgboost model to Cassandra for this tx & ty'''

    # TODO: this is going to need to be run for each chip like it is
    # in segment.

    # TODO: work out the data model
        
    db.insert_annual_prediction(cfg,
                                ctx['cx'],
                                ctx['cy'],
                                ctx['px'],
                                ctx['py'],
                                ctx['sday'],
                                ctx['eday'],
                                ctx['date'],
                                ctx['predictions'])
    return ctx


def respond(ctx):
    '''Send the HTTP response'''

    body = {'tx': get('tx', ctx, None),
            'ty': get('ty', ctx, None),
            'acquired': get('acquired', ctx, None),
            'date': get('date', ctx, None),
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
                
@tile.route('/annual-prediction', methods=['POST'])        
def annual_prediction():
    
    return thread_first(request.json,
                        partial(exception_handler, http_status=500, name='log_request', fn=log_request),
                        partial(exception_handler, http_status=400, name='parameters', fn=parameters),
                        partial(exception_handler, http_status=500, name='load_model', fn=load_model),
                        partial(exception_handler, http_status=500, name='load_data', fn=partial(load_data, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='predict', fn=partial(predict, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='save', fn=partial(save, cfg=cfg)),
                        respond)
