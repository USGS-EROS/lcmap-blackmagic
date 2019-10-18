from blackmagic import raise_on
from blackmagic import segaux
from blackmagic import skip_on_empty
from blackmagic import skip_on_exception
from blackmagic import workers
from blackmagic.data import ceph
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
from sklearn.model_selection import train_test_split
from operator import add
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential

import arrow
import blackmagic
import logging
import json
import numpy
import xgboost as xgb

logger = logging.getLogger('blackmagic.tile')
tile = Blueprint('tile', __name__)

cfg = merge(blackmagic.cfg, ceph.cfg)


def log_request(ctx):
    '''Create log message for HTTP request'''

    tx = get('tx', ctx, None)
    ty = get('ty', ctx, None)
    a  = get('acquired', ctx, None)
    d  = get('date', ctx, None)
    c  = get('chips', ctx, None)
    
    logger.info("POST /tile {x},{y},{a},{d},{c}".format(x=tx, y=ty, a=a, d=d, c=c))
        
    return ctx


def watchlist(training_data, eval_data):
    return [(training_data, 'train'), (eval_data, 'eval')]


def add_average_reflectance(ctx):
    return assoc(ctx, 'data', segaux.average_reflectance(ctx['data']))


@retry(retry=retry_if_exception_type(Exception),
       stop=stop_after_attempt(10),
       reraise=True,
       wait=wait_random_exponential(multiplier=1, max=60))
def segments(ctx, cfg):
    '''Return saved segments'''
    
    with ceph.connect(cfg) as c:     
        return assoc(ctx, 'segments', c.select_segments(ctx['cx'], ctx['cy']))


def segments_filter(ctx):
    '''Yield segments that span the supplied date'''

    d = arrow.get(ctx['date']).datetime

    return assoc(ctx,
                 'segments',
                 list(filter(lambda s: d >= arrow.get(s['sday']).datetime and d <= arrow.get(s['eday']).datetime,
                             ctx['segments'])))


def pipeline(chip, tx, ty, date, acquired, cfg):

    ctx = {'tx': tx,
           'ty': ty,
           'cx': first(chip),
           'cy': second(chip),
           'date': date,
           'acquired': acquired}

    return thread_first(ctx,
                        partial(segments, cfg=cfg),
                        segments_filter,
                        partial(segaux.aux, cfg=cfg),
                        segaux.aux_filter,                        
                        segaux.combine,                        
                        segaux.unload_segments,
                        segaux.unload_aux,
                        segaux.add_training_dates,
                        add_average_reflectance,
                        segaux.training_format,
                        #segaux.log_chip,
                        segaux.exit_pipeline)


def exception_handler(ctx, http_status, name, fn):
    try:
        return fn(ctx)
    except Exception as e:
        d = {'tx': get('tx', ctx, None),
             'ty': get('ty', ctx, None),
             'acquired': get('acquired', ctx, None),
             'date': get('date', ctx, None),
             'chips': get('chips', ctx, None),
             'exception': '{name} exception: {ex}'.format(name=name, ex=e),
             'http_status': http_status}

        logger.exception(json.dumps(assoc(d,
                                          'chips',
                                          count(get('chips', ctx, [])))))
        return d

    
def measure(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        ctx = fn(*args, **kwargs)
        
        d = {"tx":get("tx", ctx, None),
             "ty":get("ty", ctx, None),
             "date":get("date", ctx, None),
             "acquired":get("acquired", ctx, None),
             "chips":count(get("chips", ctx, []))}
                        
        logger.info(json.dumps(assoc(d,
                                     "{name}_elapsed_seconds".format(name=fn.__name__),
                                     (datetime.now() - start).total_seconds())))

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
                'acquired': acquired,
                'date': date,
                'chips': list(map(lambda chip: (int(first(chip)), int(second(chip))), chips)),
                'test_data_exception': get('test_data_exception', r, None),
                'test_training_exception': get('test_training_exception', r, None),
                'test_save_exception': get('test_save_exception', r, None)}

@skip_on_exception
@raise_on('test_data_exception')
@measure
def data(ctx, cfg):
    '''Retrieve training data for all chips in parallel'''
    
    p = partial(pipeline,
                tx=ctx['tx'],
                ty=ctx['ty'],
                date=ctx['date'],
                acquired=ctx['acquired'],
                cfg=cfg)
   
    with workers(cfg) as w:
        return assoc(ctx, 'data', numpy.array(list(flatten(w.map(p, ctx['chips']))), dtype=numpy.float32))

    
@skip_on_exception
@measure
def statistics(ctx):
    '''Count label occurences
       
       associates 'statistics' into ctx
       sample 'statistics' value: Counter({4.0: 4255, 0.0: 3651, 3.0: 1746, 5.0: 348})
    '''

    dimension = ctx['data'][::-1, ::ctx['data'].shape[1]]
    dep = dimension.flatten()
    vals, cnts = numpy.unique(dep, return_counts=True)
    prct = cnts / numpy.sum(cnts)
    dimension = None
    dep = None
    cnts = None
    del dimension
    del dep
    del cnts
    return assoc(ctx, 'statistics', (vals, prct))


@skip_on_exception
@measure
def randomize(ctx, cfg):
    '''Randomize the order of training data'''

    r = numpy.random.RandomState().permutation(ctx['data'])
    del ctx['data']
    
    return assoc(ctx, 'data', r)


@skip_on_exception
@measure
def split_data(ctx):

    independent = segaux.independent(ctx['data'])
    dependent   = segaux.dependent(ctx['data'])
    ctx['data'] = None
    del ctx['data']
    
    return merge(ctx, {'independent': independent, 'dependent': dependent})
    

@skip_on_exception
@measure
def sample(ctx, cfg):
    '''Return leveled data sample based on label values'''

    # See xg-train-annualized.py in lcmap-science/classification as reference.

    class_values, percent = ctx['statistics']
    
    # Adjust the target counts that we are wanting based on the percentage
    # that each one represents in the base data set.
    adj_counts = numpy.ceil(cfg['xgboost']['target_samples'] * percent)
    adj_counts[adj_counts > cfg['xgboost']['class_max']] = cfg['xgboost']['class_max']
    adj_counts[adj_counts < cfg['xgboost']['class_min']] = cfg['xgboost']['class_min']

    selected_indices = []
    for cls, count in zip(class_values, adj_counts):
        # Index locations of values
        indices = numpy.where(ctx['dependent'] == cls)[0]

        # Add the index locations up to the count
        selected_indices.extend(indices[:int(count)])

    si = numpy.array(selected_indices)

    # do we need to wipe out ctx['independent'] & ctx['dependent'] after
    # taking the sample to free up memory?
    # Advanced indexing always returns a copy of the data (contrast with basic slicing that returns a view).
    independent = ctx['independent'][si]
    dependent   = ctx['dependent'][si]

    selected_indices = None
    ctx['statistics'] = None
    si = None
    del selected_indices
    del ctx['statistics']
    del si
    del class_values
    del percent

    return merge(ctx, {'independent': independent, 'dependent': dependent})


@raise_on('test_training_exception')
@skip_on_exception
@skip_on_empty('independent')
@skip_on_empty('dependent')
@measure
def train(ctx, cfg):
    '''Train an xgboost model'''
    
    itrain, itest, dtrain, dtest = train_test_split(ctx['independent'],
                                                    ctx['dependent'],
                                                    test_size=get_in(['xgboost', 'test_size'], cfg))
    
    train_matrix = xgb.DMatrix(data=itrain, label=dtrain)
    test_matrix  = xgb.DMatrix(data=itest, label=dtest)
    watch_list   = watchlist(train_matrix, test_matrix)
    
    model = xgb.train(params=get_in(['xgboost', 'parameters'], cfg),
                      dtrain=train_matrix,
                      num_boost_round=get_in(['xgboost', 'num_round'], cfg),
                      evals=watch_list,
                      early_stopping_rounds=get_in(['xgboost', 'early_stopping_rounds'], cfg),
                      verbose_eval=get_in(['xgboost', 'verbose_eval'], cfg))

    ctx['independent'] = None
    ctx['dependent'] = None
    itrain = None
    itest = None
    dtrain = None
    dtest = None
    train_matrix = None
    test_matrix = None
    watch_list = None
    del ctx['independent']
    del ctx['dependent']
    del itrain
    del itest
    del dtrain
    del dtest
    del train_matrix
    del test_matrix
    del watch_list

    return assoc(ctx, 'model', model)


@raise_on('test_save_exception')
@skip_on_exception
@skip_on_empty('model')
@measure
def save(ctx, cfg):                                                
    '''Saves an xgboost model for this tx & ty'''

    # will need to decode hex when pulling model
    # >>> bytes.fromhex('deadbeef')
    #b'\xde\xad\xbe\xef'

    model_bytes = segaux.bytes_from_booster(ctx['model']).hex()

    ctx['model'] = None
    del ctx['model']
    
    with ceph.connect(cfg) as c:
        c.insert_tile(ctx['tx'],
                      ctx['ty'],
                      model_bytes)
        return ctx
    
    
def respond(ctx):
    '''Send the HTTP response'''

    body = {'tx': get('tx', ctx, None),
            'ty': get('ty', ctx, None),
            'acquired': get('acquired', ctx, None),
            'date': get('date', ctx, None),
            'chips': count(get('chips', ctx, []))}

    e = get('exception', ctx, None)
    
    if e:
        response = jsonify(assoc(body, 'exception', e))
    else:
        response = jsonify(body)

    response.status_code = get('http_status', ctx, 200)

    ctx = None
    del ctx
    
    return response

def print_keys(ctx, step):
    logger.info("{} keys: {}".format(step, ctx.keys()))

    import resource
    logger.info("SELF memory (kb):{}".format(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
    logger.info("CHILDREN memory (kb):{}".format(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss))

    return ctx

def run(params):
    return thread_first(params,
                        partial(exception_handler, http_status=500, name='log_request', fn=log_request),
                        partial(exception_handler, http_status=400, name='parameters', fn=parameters),
                        partial(exception_handler, http_status=500, name='data', fn=partial(data, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='statistics', fn=statistics),
                        partial(exception_handler, http_status=500, name='randomize', fn=partial(randomize, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='split_data', fn=split_data),
                        partial(exception_handler, http_status=500, name='sample', fn=partial(sample, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='train', fn=partial(train, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='save', fn=partial(save, cfg=cfg)))

@tile.route('/tile', methods=['POST'])        
def tiles():
    return respond(run(request.json))
