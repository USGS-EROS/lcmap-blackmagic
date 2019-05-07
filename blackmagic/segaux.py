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
from cytoolz import partial
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


def independent(data):
    '''Independent variable is (are) all the values except the labels.
        data: 2d numpy array
        return: 2d numpy array minus the labels (first element of every row)
    '''

    # Remove the first element from every array no matter the dimensions.
    # Returns data in same shape as provided. Is this really what we want?
    return numpy.delete(data, 0, data.ndim - 1)


def dependent(data):
    '''Dependent variable is (are) the labels
       data: 2d numpy array
       return: 1d numpy array of labels
    '''

    return numpy.delete(data,
                        numpy.s_[1:],
                        data.ndim - 1).flatten().astype('int8')


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
                                        

def combine(ctx):
    '''Combine segments with matching aux entry'''

    data = []
        
    for s in ctx['segments']:

        key = (s.cx, s.cy, s.px, s.py)
        a   = get_in(['aux', key], ctx, None)

        if a is not None:
            data.append(merge(a, s._asdict()))

    return assoc(ctx, 'data', data)


def prediction_dates(sday, eday, month, day):
    start = arrow.get(sday)
    end   = arrow.get(eday)
    years = list(map(lambda x: x.year, arrow.Arrow.range('year', start, end)))

    dates = []
    for y in years:
        prediction_date = arrow.get('{year}-{month}-{day}'.format(year=y,
                                                                  month=month,
                                                                  day=day))
        if prediction_date >= start and prediction_date <= end:
            dates.append(prediction_date.date().isoformat())
    return dates
    
        
def add_prediction_entries(ctx):

    entries = []
    for data in ctx['data']:
        dates = prediction_dates(sday=get('sday', data),
                                 eday=get('eday', data),
                                 month=ctx['month'],
                                 day=ctx['day'])
        for date in dates:
            entries.append(assoc(data, 'date', date))

    return assoc(ctx, 'data', entries)

  
def average_reflectance(segment):
    '''Add average reflectance values into dataset'''
    
    avgrefl = lambda intercept, slope, ordinal: add(intercept, mul(slope, ordinal))
    
    arfn    = partial(avgrefl,
                      slope=get('slope', segment),
                      ordinal=arrow.get(get('date', segment)).datetime.toordinal())
                              
    ar = {'blar': arfn(get('blint', segment)),
          'grar': arfn(get('grint', segment)),
          'niar': arfn(get('niint', segment)),
          'rear': arfn(get('reint', segment)),
          's1ar': arfn(get('s1int', segment)),
          's2ar': arfn(get('s2int', segment)),
          'thar': arfn(get('thint', segment))}
    
    return merge(segment, ar)


def add_average_reflectance(ctx):

    return assoc(ctx, 'data', map(average_reflectance, ctx['data']))


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


def to_numpy(data):
    return numpy.array(data, dtype=numpy.float32)


def standard_format(segmap):
    return list(flatten([get('nlcdtrn', segmap),
                         get('aspect' , segmap),
                         get('posidex', segmap),
                         get('slope'  , segmap),
                         get('mpw'    , segmap),
                         get('dem'    , segmap),
                         get('blcoef' , segmap),
                        [get('blrmse' , segmap)],
                        [get('blar'   , segmap)],
                         get('grcoef' , segmap),
                        [get('grrmse' , segmap)],
                        [get('grar'   , segmap)], 
                         get('nicoef' , segmap),
                        [get('nirmse' , segmap)],
                        [get('niar'   , segmap)],
                         get('recoef' , segmap),
                        [get('rermse' , segmap)],
                        [get('rear'   , segmap)],
                         get('s1coef' , segmap),
                        [get('s1rmse' , segmap)],
                        [get('s1ar'   , segmap)],
                         get('s2coef' , segmap),
                        [get('s2rmse' , segmap)],
                        [get('s2ar'   , segmap)],
                         get('thcoef' , segmap),
                        [get('thrmse' , segmap)],
                        [get('thar'   , segmap)]]))


def training_format(ctx):

    d = [standard_format(sm) for sm in ctx['data']]
    return assoc(ctx, 'data', to_numpy(d))


def prediction_format(ctx):
    return [{'cx'  : get('cx', ctx),
             'cy'  : get('cy', ctx),
             'px'  : get('px', ctx),
             'py'  : get('py', ctx),
             'sday': get('sday', ctx),
             'eday': get('eday', ctx),
             'date': get('date', ctx),
             'data': independent(to_numpy(standard_format(d)))} for d in ctx['data']]


