from blackmagic import cfg
from blackmagic import db
from blackmagic import raise_on
from blackmagic import skip_on_exception
from blackmagic import skip_on_empty
from blackmagic import workers
from collections import Counter
from cytoolz import assoc
from cytoolz import count
from cytoolz import dissoc
from cytoolz import do
from cytoolz import drop
from cytoolz import filter
from cytoolz import first
from cytoolz import get
from cytoolz import get_in
from cytoolz import merge
from cytoolz import partial
from cytoolz import reduce
from cytoolz import second
from cytoolz import take
from cytoolz import thread_first
from datetime import datetime
from flask import Blueprint
from flask import jsonify
from flask import request
from functools import wraps
from merlin.functions import flatten
from requests.exceptions import ConnectionError
from sklearn.model_selection import train_test_split
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential

import arrow
import gc
import logging
import io
import merlin
import numpy
import xgboost as xgb

logger = logging.getLogger('blackmagic.tile')
tile = Blueprint('tile', __name__)


def dmatrix(data, labels):
    '''Transforms independent and dependent variables into an xgboost dmatrix'''

    return xgb.DMatrix(data, labels)


def independent(data):
    '''Independent variable is (are) all the values except the labels
        data: 2d numpy array
        return: 2d numpy array minus the labels (first element of every row)
    '''
    
    return numpy.delete(data, 0, 1)


def dependent(data):
    '''Dependent variable is (are) the labels
       data: 2d numpy array
       return: 1d numpy array of labels
    '''
    
    return numpy.delete(data, numpy.s_[1:], 1).flatten()


def watchlist(training_data, eval_data):
    return [(training_data, 'train'), (eval_data, 'eval')]


@retry(retry=retry_if_exception_type(ConnectionError),
       stop=stop_after_attempt(10),
       reraise=True,
       wait=wait_random_exponential(multiplier=1, max=60))
def aux(ctx, cfg):
    '''Retrieve aux data'''
    
    data = merlin.create(x=ctx['cx'],
                         y=ctx['cy'],
                         acquired=ctx['acquired'],  #'1982/2018',
                         cfg=merlin.cfg.get(profile='chipmunk-aux',
                                            env={'CHIPMUNK_URL': cfg['aux_url']}))

    return assoc(ctx,
                 'aux',
                 {first(d): second(d) for d in merlin.functions.denumpify(data)})


def aux_filter(ctx):
    
    return assoc(ctx,
                 'aux',
                 dict(list(filter(lambda d: first(get('nlcdtrn', second(d))) != 0,
                                  ctx['aux'].items()))))


def segments(ctx, cfg):
    '''Return segments stored in Cassandra'''
    
    return assoc(ctx,
                 'segments',
                 [r for r in db.execute_statement(cfg,
                                                  db.select_segment(cfg,
                                                                    ctx['cx'],
                                                                    ctx['cy']))])
    
def segments_filter(ctx):
    '''Yield segments that span the supplied date'''

    d = arrow.get(ctx['date']).datetime
    
    return assoc(ctx,
                 'segments',
                 list(filter(lambda s: d >= arrow.get(s.sday).datetime and d <= arrow.get(s.eday).datetime,
                             ctx['segments'])))


def combine(ctx):
    '''Combine segments with matching aux entry'''

    data = []
        
    for s in ctx['segments']:

        key = (s.cx, s.cy, s.px, s.py)
        a   = get_in(['aux', key], ctx, None)

        if a is not None:
            data.append(merge(a, s._asdict()))

    return assoc(ctx, 'data', data)

      
def unload_segments(ctx):
    '''Manage memory, unload segments following combine'''

    return dissoc(ctx, 'segments')


def unload_aux(ctx):
    '''Manage memory, unload aux following combine'''

    return dissoc(ctx, 'aux')

def collect_garbage(ctx):
    #gc.collect()
    return ctx


def format(ctx):

    # return [[]] numpy array from ctx
    '''Properly format training entries'''

    '''
    {'nlcdtrn': [2], 'aspect': [0], 'posidex': [25.0], 'nlcd': [82], 'slope': [6.3103461265563965], 'mpw': [0], 'dem': [276.5125427246094], 'dates': ['2000-07-31T00:00:00Z/2001-01-01T00:00:00Z'], 'cx': 1646415, 'cy': 2237805, 'px': 1649385, 'py': 2235045, 'sday': '1984-03-24', 'eday': '2016-10-06', 'bday': '2016-10-06', 'blcoef': [-0.010404632426798344, 54.50187301635742, 101.96070861816406, -38.63310623168945, -3.4969518184661865, 0.0, -38.35179138183594], 'blint': 8016.1611328125, 'blmag': 93.25048828125, 'blrmse': 140.84637451171875, 'chprob': 0.0, 'curqa': 8, 'grcoef': [-0.014921323396265507, -13.973718643188477, 126.78702545166016, -62.550445556640625, -22.54693603515625, 4.363803386688232, -13.57226276397705], 'grint': 11458.1923828125, 'grmag': 85.9715805053711, 'grrmse': 140.90208435058594, 'nicoef': [0.001133676152676344, -1567.1834716796875, -167.4553680419922, 355.0714416503906, 191.9523468017578, -142.80911254882812, 342.6976013183594], 'niint': 1595.630126953125, 'nimag': 212.36441040039062, 'nirmse': 421.1643371582031, 'recoef': [-0.016207082197070122, 104.48441314697266, 211.29937744140625, -158.95477294921875, -42.79849624633789, -19.37449836730957, -89.44105529785156], 'reint': 12387.9248046875, 'remag': 69.07315826416016, 'rermse': 137.7318878173828, 's1coef': [-0.02014756016433239, -300.4599609375, 386.727294921875, -299.61871337890625, -55.58943557739258, -62.033470153808594, -161.67315673828125], 's1int': 16410.873046875, 's1mag': 89.79656219482422, 's1rmse': 272.87200927734375, 's2coef': [-0.01282140240073204, 29.842893600463867, 383.56500244140625, -260.76898193359375, -67.41301727294922, -13.364554405212402, -178.7677459716797], 's2int': 10256.634765625, 's2mag': 119.9651870727539, 's2rmse': 196.84481811523438, 'thcoef': [0.0017974661896005273, -1176.3935546875, -116.62395477294922, -229.40621948242188, -38.72520065307617, 11.268446922302246, -49.42088317871094], 'thint': -226.81626892089844, 'thmag': 251.63075256347656, 'thrmse': 417.1956481933594}

    '''


    # instead of a list comprehension, build a numpy array out of each
    # entry directly and bypass all the straight python datastructures.
    
    training = [list(flatten([get('nlcdtrn', e),
                             get('aspect' , e),
                             get('posidex', e),
                             get('slope'  , e),
                             get('mpw'    , e),
                             get('dem'    , e),
                             get('blcoef' , e),
                             [get('blint'  , e)],
                             [get('blmag'  , e)],
                             [get('blrmse' , e)],
                             get('grcoef' , e),
                             [get('grint'  , e)],
                             [get('grmag'  , e)],
                             [get('grrmse' , e)],
                             get('nicoef' , e),
                             [get('niint'  , e)],
                             [get('nimag'  , e)],
                             [get('nirmse' , e)],
                             get('recoef' , e),
                             [get('reint'  , e)], 
                             [get('remag'  , e)],
                             [get('rermse' , e)],
                             get('s1coef' , e),
                             [get('s1int'  , e)],
                             [get('s1mag'  , e)],
                             [get('s1rmse' , e)],
                             get('s2coef' , e),
                             [get('s2int'  , e)],
                             [get('s2mag'  , e)],
                             [get('s2rmse' , e)],
                             get('thcoef' , e),
                             [get('thint'  , e)],
                             [get('thmag'  , e)],
                             [get('thrmse' , e)]])) for e in ctx['data']]

    # create and return 2d numpy array
    return numpy.array(training, dtype=numpy.float32)


def pipeline(chip, date, acquired, cfg):

    ctx = {'cx': first(chip),
           'cy': second(chip),
           'date': date,
           'acquired': acquired}

    # {'cx': 0, 'cy': 0, 'acquired': '1980/2018', 'date': '2001/07/01', aux:{}, segments:[], data:[]}

    return thread_first(ctx,
                        partial(segments, cfg=cfg),
                        segments_filter,
                        partial(aux, cfg=cfg),
                        aux_filter,                        
                        combine,
                        unload_segments,
                        unload_aux,
                        collect_garbage,
                        format)
    

def measure(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        ctx = fn(*args, **kwargs)
        
        d = {'tx': get('tx', ctx, None),
             'ty': get('ty', ctx, None),
             'date': get('date', ctx, None),
             'acquired': get('acquired', ctx, None),
             'chips': 'count:{}'.format(count(get('chips', ctx, [])))}
            
        logger.info(assoc(d,
                          '{name}_elapsed_seconds'.format(name=fn.__name__),
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
                'acquired': acquired,
                'date': date,
                'chips': list(map(lambda chip: (int(first(chip)), int(second(chip))), chips)),
                'test_data_exception': get('test_data_exception', r, None),
                'test_training_exception': get('test_training_exception', r, None),
                'test_cassandra_exception': get('test_cassandra_exception', r, None)}

        
def log_request(ctx):
    '''Create log message for HTTP request'''

    tx = get('tx', ctx, None)
    ty = get('ty', ctx, None)
    a  = get('acquired', ctx, None)
    d  = get('date', ctx, None)
    c  = get('chips', ctx, None)
    
    logger.info('POST /tile {x},{y},{a},{d},{c}'.format(x=tx, y=ty, a=a, d=d, c=c))
        
    return ctx


def exception_handler(ctx, http_status, name, fn):
    try:
        return fn(ctx)
    except Exception as e:        
        return do(logger.exception, {'tx': get('tx', ctx, None),
                                     'ty': get('ty', ctx, None),
                                     'acquired': get('acquired', ctx, None),
                                     'date': get('date', ctx, None),
                                     'chips': get('chips', ctx, None),
                                     'exception': '{name} exception: {ex}'.format(name=name, ex=e),
                                     'http_status': http_status})
    

@skip_on_exception
@raise_on('test_data_exception')
@measure
def data(ctx, cfg):
    '''Retrieve training data for all chips in parallel'''
    
    p = partial(pipeline, date=ctx['date'], acquired=ctx['acquired'], cfg=cfg)

    npa = None
    cnt = 0
    
    with workers(cfg) as w:
        
        for a in w.imap_unordered(p, ctx['chips'], cfg['cpus_per_worker']):

            cnt += 1
            logger.info('{{"tx":{tx} "ty":{ty} "msg":"loading data for chip #:{cnt}}}'.format(cnt=cnt,tx=ctx['tx'], ty=ctx['ty']))
            
            if npa is None:
                npa = a
            else:
                npa = numpy.append(npa, a, axis=0)

        return assoc(ctx, 'data', npa)


def counts(data):
    '''Count the occurance of each label in data'''
   
    c = Counter()
    c[first(data)] += 1
    return c

    
@skip_on_exception
@measure
def statistics(ctx):
    '''Count label occurences
       
       associates 'statistics' into ctx
       sample 'statistics' value: Counter({4.0: 4255, 0.0: 3651, 3.0: 1746, 5.0: 348})
    '''
    
    with workers(cfg) as w:
        counters = w.map(counts, ctx['data'])
        c = Counter()
        list(map(c.update, counters))
        return assoc(ctx, 'statistics', c)
    
    return ctx


@skip_on_exception
@measure
def randomize(ctx, cfg):
    '''Randomize the order of training data'''

    return assoc(ctx,
                 'data',
                 numpy.random.RandomState().permutation(ctx['data']))


@skip_on_exception
@measure
def sample_sizes(ctx, cfg):

    # TODO: Review sampling approach prior to release
    #
    #
    
    total    = count(ctx['data'])
    labelmax = get_in(['xgboost', 'max_samples_per_label'], cfg)

    return assoc(ctx,
                 'sample_sizes',
                 {l: o if o < labelmax else labelmax for l, o in ctx['statistics'].items()})        

    #for label, occurances in ctx['statistics'].items():
        
    #return assoc(ctx,
    #             'sample_sizes',
    #             {l: math.ceil(c/total) for l, c in ctx['statistics'].items()}


                      
    #
    # ctx['statistics']
    #
    
    #for label, occurances in ctx['statistics'].items():
    # class_values, percent = class_stats(dependent)

    # Adjust the target counts that we are wanting based on the percentage
    # that each one represents in the base data set.
    #adj_counts = np.ceil(params['target_samples'] * percent)
    #adj_counts[adj_counts > params['class_max']] = params['class_max']
    #adj_counts[adj_counts < params['class_min']] = params['class_min']

    
    #return assoc(ctx, 'sample_sizes', sizes)


@skip_on_exception
@measure
def sample(ctx):
    '''Return leveled data sample based on label values'''

    # See xg-train-annualized.py in lcmap-science/classification as reference.
    
    samples = []
    
    for label, size in ctx['sample_sizes'].items():
        samples.append(list(take(size, filter(lambda x: first(x) == label, ctx['data']))))
        
    return assoc(ctx, 'data', list(flatten(samples)))


@raise_on('test_training_exception')
@skip_on_exception
@skip_on_empty('data')
@measure
def train(ctx, cfg):
    '''Train an xgboost model'''

    
    itrain, itest, dtrain, dtest = train_test_split(independent(ctx['data']),
                                                    dependent(ctx['data']),
                                                    test_size=get_in(['xgboost', 'test_size'], cfg))
    
    train_matrix = xgb.DMatrix(data=itrain, label=dtrain)
    test_matrix  = xgb.DMatrix(data=itest, label=dtest)
    
    return assoc(ctx, 'model', xgb.train(params=get_in(['xgboost', 'parameters'], cfg),
                                         dtrain=train_matrix,
                                         num_boost_round=get_in(['xgboost', 'num_round'], cfg),
                                         evals=watchlist(train_matrix, test_matrix),
                                         early_stopping_rounds=get_in(['xgboost', 'early_stopping_rounds'], cfg),
                                         verbose_eval=get_in(['xgboost', 'verbose_eval'], cfg)))

@raise_on('test_cassandra_exception')
@skip_on_exception
@skip_on_empty('model')
@measure
def save(ctx, cfg):                                                
    '''Saves an xgboost model to Cassandra for this tx & ty'''
   
    db.execute2(cfg, **db.insert_tile(cfg,
                                      ctx['tx'],
                                      ctx['ty'],
                                      ctx['model'].save_raw()))    
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



@tile.route('/tile', methods=['POST'])        
def tiles():
   
    return thread_first(request.json,
                        partial(exception_handler, http_status=500, name='log_request', fn=log_request),
                        partial(exception_handler, http_status=400, name='parameters', fn=parameters),
                        partial(exception_handler, http_status=500, name='data', fn=partial(data, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='statistics', fn=statistics),
                        partial(exception_handler, http_status=500, name='randomize', fn=partial(randomize, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='sample_sizes', fn=partial(sample_sizes, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='sample', fn=sample),
                        partial(exception_handler, http_status=500, name='train', fn=partial(train, cfg=cfg)),
                        partial(exception_handler, http_status=500, name='save', fn=partial(save, cfg=cfg)),
                        respond)
