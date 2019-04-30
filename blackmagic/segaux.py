'''
segaux.py handles retrieving segments & aux data 
and merging them together into a two dimensional 
numpy array.  It includes functions for 
working with the resulting datastructure such 
as obtaining the dependent or independent 
variables for training and prediction.

Segaux functions should remain non-complected and composed 
into functions in the module or namespace where they are used.
'''

from blackmagic import db
from cassandra import ReadTimeout
from cytoolz import assoc
from cytoolz import dissoc
from cytoolz import filter
from cytoolz import first
from cytoolz import get
from cytoolz import get_in
from cytoolz import merge
from cytoolz import second
from cytoolz import thread_first
from datetime import date
from merlin.functions import flatten
from requests.exceptions import ConnectionError
from operator import add
from operator import mul
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential

import arrow
import logging
import merlin
import numpy

logger = logging.getLogger('blackmagic.segaux')
_cluster = None


def cluster(cfg):
    '''Create dbconn and add to context'''

    global _cluster
    
    if _cluster is None:
        _cluster = db.cluster(cfg)

    return _cluster


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
    
    return numpy.delete(data, numpy.s_[1:], 1).flatten().astype('int8')


def average_reflectance(intercept, slope, ordinal_day):
    '''Remove periodicity from spectra signals'''
    
    return add(intercept, mul(slope, ordinal_day))


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


@retry(retry=retry_if_exception_type(ReadTimeout),
       stop=stop_after_attempt(10),
       reraise=True,
       wait=wait_random_exponential(multiplier=1, max=60))
def segments(ctx, cfg):
    '''Return segments stored in Cassandra'''

    s = db.session(cfg, ctx['cluster'])
    
    return assoc(ctx,
                 'segments',
                 [r for r in s.execute(db.select_segment(cfg, ctx['cx'], ctx['cy']))])
                                        

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


def segment_date(year, month, day, segment):
    '''Add dates into segment'''
    
    d = date(year=year, month=month, day=day)
    return assoc(segment, 'date', d.isoformat())


def add_dates(ctx):

    d     = []
    ys    = ctx['years'].split('/')
    start = int(ys[0])
    end   = int(ys[1])
    
    for y in range(start, end):
        fn = partial(segment_date, year=y, month=ctx['month'], day=ctx['day'])
        d.append(list(map(fn, ctx['data'])))
        
    return assoc('data', list(flatten(d)))
        

def average_reflectance(date, segment):
    '''Add average reflectance values into dataset'''
    
    avgrefl = lambda slope, ordinal, intercept: add(intercept, mul(slope, ordinal))
    
    arfn    = partial(avgrefl(slope=get('slope', segment),
                              ordinal=datetime(date.toordinal)))
    
    ar = {'blar': arfn(get('blint', segment)),
          'grar': arfn(get('grint', segment)),
          'niar': arfn(get('niint', segment)),
          'rear': arfn(get('reint', segment)),
          's1ar': arfn(get('s1int', segment)),
          's2ar': arfn(get('s2int', segment)),
          'thar': arfn(get('thint', segment))}
    
    return merge(segment, ar)


def add_average_reflectance(ctx):
    return assoc(ctx, 'data', map(partial(average_reflectance, date=ctx['date']), ctx['data']))


def unload_segments(ctx):
    '''Manage memory, unload segments following combine'''

    return dissoc(ctx, 'segments')


def unload_aux(ctx):
    '''Manage memory, unload aux following combine'''

    return dissoc(ctx, 'aux')


def log_chip(ctx):

    m = '{{"tx":{tx}, "ty":{ty}, "cx":{cx}, "cy":{cy}, "date":{date}, "acquired":{acquired}, "msg":"loading data"}}'

    logger.info(m.format(**ctx))
    
    return ctx


def exit_pipeline(ctx):
    return ctx['data']


# TODO: 
# Consider adding a module for transducer functions that operate on a single record at a time
# and put all the mapping functions into this module.

def format(ctx):

    # return [[]] numpy array from ctx
    '''Properly format xgboost data'''

    '''
    {'nlcdtrn': [2], 'aspect': [0], 'posidex': [25.0], 'nlcd': [82], 'slope': [6.3103461265563965], 'mpw': [0], 'dem': [276.5125427246094], 'dates': ['2000-07-31T00:00:00Z/2001-01-01T00:00:00Z'], 'cx': 1646415, 'cy': 2237805, 'px': 1649385, 'py': 2235045, 'sday': '1984-03-24', 'eday': '2016-10-06', 'bday': '2016-10-06', 'blcoef': [-0.010404632426798344, 54.50187301635742, 101.96070861816406, -38.63310623168945, -3.4969518184661865, 0.0, -38.35179138183594], 'blint': 8016.1611328125, 'blmag': 93.25048828125, 'blrmse': 140.84637451171875, 'chprob': 0.0, 'curqa': 8, 'grcoef': [-0.014921323396265507, -13.973718643188477, 126.78702545166016, -62.550445556640625, -22.54693603515625, 4.363803386688232, -13.57226276397705], 'grint': 11458.1923828125, 'grmag': 85.9715805053711, 'grrmse': 140.90208435058594, 'nicoef': [0.001133676152676344, -1567.1834716796875, -167.4553680419922, 355.0714416503906, 191.9523468017578, -142.80911254882812, 342.6976013183594], 'niint': 1595.630126953125, 'nimag': 212.36441040039062, 'nirmse': 421.1643371582031, 'recoef': [-0.016207082197070122, 104.48441314697266, 211.29937744140625, -158.95477294921875, -42.79849624633789, -19.37449836730957, -89.44105529785156], 'reint': 12387.9248046875, 'remag': 69.07315826416016, 'rermse': 137.7318878173828, 's1coef': [-0.02014756016433239, -300.4599609375, 386.727294921875, -299.61871337890625, -55.58943557739258, -62.033470153808594, -161.67315673828125], 's1int': 16410.873046875, 's1mag': 89.79656219482422, 's1rmse': 272.87200927734375, 's2coef': [-0.01282140240073204, 29.842893600463867, 383.56500244140625, -260.76898193359375, -67.41301727294922, -13.364554405212402, -178.7677459716797], 's2int': 10256.634765625, 's2mag': 119.9651870727539, 's2rmse': 196.84481811523438, 'thcoef': [0.0017974661896005273, -1176.3935546875, -116.62395477294922, -229.40621948242188, -38.72520065307617, 11.268446922302246, -49.42088317871094], 'thint': -226.81626892089844, 'thmag': 251.63075256347656, 'thrmse': 417.1956481933594}

    '''
    # instead of a list comprehension, build a numpy array out of each
    # entry directly and bypass all the straight python datastructures.
    
    '''training = [list(flatten([get('nlcdtrn', e),
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
                             [get('thrmse' , e)]])) for e in ctx['data']]'''
    
        d = [list(flatten([get('nlcdtrn', e),
                           get('aspect' , e),
                           get('posidex', e),
                           get('slope'  , e),
                           get('mpw'    , e),
                           get('dem'    , e),
                           get('blcoef' , e),
                          [get('blint'  , e)],
                          [get('blrmse' , e)],
                          [get('blar'   , e)],
                           get('grcoef' , e),
                          [get('grint'  , e)],
                          [get('grrmse' , e)],
                          [get('grar'   , e)], 
                           get('nicoef' , e),
                          [get('niint'  , e)],
                          [get('nirmse' , e)],
                          [get('niar'   , e)],
                           get('recoef' , e),
                          [get('reint'  , e)], 
                          [get('rermse' , e)],
                          [get('rear'   , e)],
                           get('s1coef' , e),
                          [get('s1int'  , e)],
                          [get('s1rmse' , e)],
                          [get('s1ar'   , e)],
                           get('s2coef' , e),
                          [get('s2int'  , e)],
                          [get('s2rmse' , e)],
                          [get('s2ar'   , e)],
                           get('thcoef' , e),
                          [get('thint'  , e)],
                          [get('thrmse' , e)],
                          [get('thar'   , e)]])) for e in ctx['data']]

# The problem is this is iterating over ctx['data']
# It needs to iterate over whatever you give it instead.
        
    # create and return 2d numpy array
    return assoc(ctx, 'data', numpy.array(d, dtype=numpy.float32))
