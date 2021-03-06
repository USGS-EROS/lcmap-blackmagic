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
from tenacity import after_log
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_exponential
from tenacity import wait_random_exponential

import arrow
import logging
import merlin
import numpy
import os
import tempfile
import xgboost as xgb


logger = logging.getLogger('blackmagic.segaux')

######################
# quiet arrow down
######################
import warnings
from arrow.factory import ArrowParseWarning

warnings.simplefilter("ignore", ArrowParseWarning)


def independent(data):
    '''Independent variable is (are) all the values except the labels.
        data: 2d numpy array
        return: 2d numpy array minus the labels (first element of every row)
    '''

    # Remove the first element from every array no matter the dimensions.
    # Returns data in same shape as provided. Is this really what we want?
    d = numpy.delete(data, 0, data.ndim - 1)

    data = None
    del data

    return d


def dependent(data):
    '''Dependent variable is (are) the labels
       data: 2d numpy array
       return: 1d numpy array of labels
    '''

    d = numpy.delete(data,
                     numpy.s_[1:],
                     data.ndim - 1).flatten().astype('int8')

    data = None
    del data

    return d


@retry(stop=stop_after_attempt(20),
       reraise=True,
       wait=wait_exponential(multiplier=1, min=2, max=5))
def aux(ctx, cfg):
    '''Retrieve aux data'''

    logger.info("getting aux for cx:{} cy:{}".format(ctx['cx'], ctx['cy']))
    
    data = merlin.create(x=ctx['cx'],
                         y=ctx['cy'],
                         acquired=ctx['acquired'],
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


def combine(ctx):
    '''Combine segments with matching aux entry'''

    data = []
        
    for s in ctx['segments']:

        key = (s['cx'], s['cy'], s['px'], s['py'])
        a   = get_in(['aux', key], ctx, None)

        if a is not None:
            data.append(merge(a, s))

    return assoc(ctx, 'data', data)


def prediction_date_fn(sday, eday, month, day):
    start = arrow.get(sday)
    end   = arrow.get(eday)
    years = range(start.year, end.year + 1)
    dates = []

    for y in years:
        prediction_date = arrow.Arrow(year=y, month=int(month), day=int(day))
        if prediction_date >= start and prediction_date <= end:
            dates.append(prediction_date.date().isoformat())
    return dates


def default_prediction_date(s):
    if get('sday', s) == '0001-01-01' and get('eday', s) == '0001-01-01':
        return '0001-01-01'
    else:
        return None

    
def prediction_dates(segments, month, day):
    
    for s in segments:
        default_date = default_prediction_date(s)

        if default_date:
            yield assoc(s, 'date', default_date)
        else:
            dates = prediction_date_fn(sday=get('sday', s),
                                       eday=get('eday', s),
                                       month=month,
                                       day=day)
                
            for date in dates:
                yield assoc(s, 'date', date)

                      
def training_date(data, date):
    
    return assoc(data, 'date', date)


def add_training_dates(ctx):
    fn = partial(training_date, date=ctx['date'])
    return assoc(ctx, 'data', list(map(fn, ctx['data'])))


def spectral_slope(spectra, segment):

    #handle cases where default change segments have
    #been passed in that do not have any coefficients
    #associated with them
    coefs = get(spectra, segment, None)

    if coefs is None or len(coefs) == 0:
        return 0
    else:
        return first(coefs)

    
def average_reflectance_fn(segment):
    '''Add average reflectance values into dataset'''
    
    avgrefl = lambda intercept, slope, ordinal: add(intercept, mul(slope, ordinal))
    
    date = arrow.get(get('date', segment)).datetime.toordinal()
    
    ar = {'blar': avgrefl(get('blint', segment), spectral_slope('blcoef', segment), date),
          'grar': avgrefl(get('grint', segment), spectral_slope('grcoef', segment), date),
          'niar': avgrefl(get('niint', segment), spectral_slope('nicoef', segment), date),
          'rear': avgrefl(get('reint', segment), spectral_slope('recoef', segment), date),
          's1ar': avgrefl(get('s1int', segment), spectral_slope('s1coef', segment), date),
          's2ar': avgrefl(get('s2int', segment), spectral_slope('s2coef', segment), date),
          'thar': avgrefl(get('thint', segment), spectral_slope('thcoef', segment), date)}
    
    return merge(segment, ar)


def average_reflectance(segments):
    
    return map(average_reflectance_fn, segments)


def unload_segments(ctx):
    '''Manage memory, unload segments following combine'''

    ctx['segments'] = None
    del ctx['segments']
    return ctx


def unload_aux(ctx):
    '''Manage memory, unload aux following combine'''

    ctx['aux'] = None
    del ctx['aux']
    return ctx


def log_chip(ctx):

    m = '{{"cx":{cx}, "cy":{cy}, "date":{date}, "acquired":{acquired}, "msg":"generating probabilities"}}'

    logger.info(m.format(**ctx))
    
    return ctx


def exit_pipeline(ctx):
    data = ctx['data']    
    ctx = None
    del ctx
    return data


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
    n = to_numpy(d)
    d = None
    del d
    return assoc(ctx, 'data', n)


#Coming into this function from combine is a list of dicts
# [{cx, cy, px, py, sday, eday, s1coef, s2coef, etc.}]
def prediction_format(segment):

    return {'cx'  : get('cx', segment),
            'cy'  : get('cy', segment),
            'px'  : get('px', segment),
            'py'  : get('py', segment),
            'sday': get('sday', segment),
            'eday': get('eday', segment),
            'pday': get('date', segment),
            'independent': independent(to_numpy(standard_format(segment)))}

        
def bytes_from_booster(booster):
    f = None
    try:
        f = tempfile.NamedTemporaryFile(delete=False)
        booster.save_model(f.name)

        with open(f.name, 'rb+') as tf:
            return tf.read()
        
    finally:
        if f:
            os.remove(f.name)

        
def booster_from_bytes(booster_bytes, params):
    f = None
    try:
        f = tempfile.NamedTemporaryFile(delete=False)

        with open(f.name, 'wb+') as tf:
            tf.write(booster_bytes)

        return xgb.Booster(model_file=tf.name, params=params)

    finally:
        if f:
            os.remove(f.name)
