from blackmagic import cfg
from blackmagic import db
from blackmagic import parallel
from cytoolz import first
from cytoolz import get
from cytoolz import merge
from cytoolz import partial
from cytoolz import reduce
from cytoolz import second
from flask import Blueprint
from flask import jsonify
from flask import request
from merlin.functions import flatten

import arrow
import logging
import io
import merlin
import numpy
import xgboost as xgb

logger = logging.getLogger('blackmagic.tile')
tile = Blueprint('tile', __name__)


def aux(cx, cy):
    '''Retrieve aux data'''
    
    data = merlin.create(x=cx,
                         y=cy,
                         acquired='1982/2018',
                         cfg=merlin.cfg.get(profile='chipmunk-aux',
                                            env={'CHIPMUNK_URL': cfg['aux_url']}))
    
    return {first(d): second(d) for d in merlin.functions.denumpify(data)}

    
def segments(cx, cy):
    '''Return segments stored in Cassandra'''
    
    results = []
    try:
        for r in db.execute(cfg, db.select_segment(cfg, cx, cy)):
            results.append(r)
    except Exception as e:
        logger.debug('segments query result exception:{}'.format(e))
    return results


def datefilter(date, segments):
    '''Yield segments that span the supplied date'''

    d = arrow.get(date).datetime
    
    for s in segments:
        if (d >= arrow.get(s.sday).datetime and
            d <= arrow.get(s.eday).datetime):
            yield s

             
def combine(segments, aux):
    '''Combine segments with matching aux entry'''

    for s in segments:
        key = (s.cx, s.cy, s.px, s.py)
        yield merge(aux[key], s._asdict())


def format(entries):
    '''Properly format training entries'''

    '''
    {'nlcdtrn': [2], 'aspect': [0], 'posidex': [25.0], 'nlcd': [82], 'slope': [6.3103461265563965], 'mpw': [0], 'dem': [276.5125427246094], 'dates': ['2000-07-31T00:00:00Z/2001-01-01T00:00:00Z'], 'cx': 1646415, 'cy': 2237805, 'px': 1649385, 'py': 2235045, 'sday': '1984-03-24', 'eday': '2016-10-06', 'bday': '2016-10-06', 'blcoef': [-0.010404632426798344, 54.50187301635742, 101.96070861816406, -38.63310623168945, -3.4969518184661865, 0.0, -38.35179138183594], 'blint': 8016.1611328125, 'blmag': 93.25048828125, 'blrmse': 140.84637451171875, 'chprob': 0.0, 'curqa': 8, 'grcoef': [-0.014921323396265507, -13.973718643188477, 126.78702545166016, -62.550445556640625, -22.54693603515625, 4.363803386688232, -13.57226276397705], 'grint': 11458.1923828125, 'grmag': 85.9715805053711, 'grrmse': 140.90208435058594, 'nicoef': [0.001133676152676344, -1567.1834716796875, -167.4553680419922, 355.0714416503906, 191.9523468017578, -142.80911254882812, 342.6976013183594], 'niint': 1595.630126953125, 'nimag': 212.36441040039062, 'nirmse': 421.1643371582031, 'recoef': [-0.016207082197070122, 104.48441314697266, 211.29937744140625, -158.95477294921875, -42.79849624633789, -19.37449836730957, -89.44105529785156], 'reint': 12387.9248046875, 'remag': 69.07315826416016, 'rermse': 137.7318878173828, 's1coef': [-0.02014756016433239, -300.4599609375, 386.727294921875, -299.61871337890625, -55.58943557739258, -62.033470153808594, -161.67315673828125], 's1int': 16410.873046875, 's1mag': 89.79656219482422, 's1rmse': 272.87200927734375, 's2coef': [-0.01282140240073204, 29.842893600463867, 383.56500244140625, -260.76898193359375, -67.41301727294922, -13.364554405212402, -178.7677459716797], 's2int': 10256.634765625, 's2mag': 119.9651870727539, 's2rmse': 196.84481811523438, 'thcoef': [0.0017974661896005273, -1176.3935546875, -116.62395477294922, -229.40621948242188, -38.72520065307617, 11.268446922302246, -49.42088317871094], 'thint': -226.81626892089844, 'thmag': 251.63075256347656, 'thrmse': 417.1956481933594}

    '''

    training = [[get('nlcdtrn', e),
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
                 [get('thrmse' , e)]] for e in entries]

    return [numpy.array(list(flatten(t)), dtype=numpy.float64) for t in training]

    # test with
    # f = t.format(t.combine(segments=t.filter(t.segments(cx=cx, cy=cy), '1982-11-11'),
    #                        aux=t.aux(cx=cx, cy=cy))
    

# dmatrix & train need to run in a single process.  The format() pipeline needs to run concurrently.  Results from concurrent format()
# pipeline calls must be merged/concatenated.
# Need to find 9 tiles bounds given a single tile x & y, then find all chip ids that fall within the 9 tiles.  Parallelize the 9
# tiles chip retrieval.
1
def dmatrix(data):
    '''Transforms a sequence of numpy values from format() to an xgboost dmatrix'''

    return xgb.DMatrix(map(drop(1, data)), label=map(first, data))


def parameters():
    '''Parameters for xgboost training'''
    return {'max_depth': 2,
            'eta': 1,
            'silent': 1,
            'objective': 'binary:logistic',
            'nthread': -1,
            'eval_metric': 'auc'}


def train(dmatrix, params):
    num_round = 10
    return xgb.train(params, dmatrix, num_round, evallist)


def pipeline_fn(date):

    def pipeline(date, cx, cy):
        return format(combine(segments=datefilter(segments(cx=cx, cy=cy), date),
                              aux=aux(cx=cx, cy=cy)))
    
    return partial(pipeline, date=date)
    

'''
# in parallel, run pipeline to pull, merge and format chips
# in one process, combine all formatted training data into a dmatrix (1d label array & 2d feature array plus params) (numpy.vstack)
# train xgboost and obtain the trained model
# save to buffer, write contents to Cassandra for tx & ty.
    
# merging formatted datasets: numpy.concatenate((a, b, c)).reshape(3, -1) where 3 is equal to the number of elements that belong in each row
'''

def save(tx, ty, model):
    '''Saves a model to Cassandra given primary key tx & ty'''

    binary_model = model.save_raw()

    # write binary_model to Cassandra as bytes

    
    # StringIO for text data
    #text = StringIO()

    # BytesIO for binary data
    #bdata = BytesIO()

    
    # create buffer
    # write to buffer
    # ? encode buffer
    # save buffer contents
    
    return {'tx': tx, 'ty': ty, 'model': model}


@tile.route('/tile', methods=['POST'])
def tile_fn():
    r = request.json
    x = get('tx', r, None)
    y = get('ty', r, None)
    c = get('chips', r, None)
    d = get('date', r, None)

    if x is None or y is None or c is None or d is None:
        response = jsonify({'tx': tx, 'ty': ty, 'chips': chips, 'date': date,
                            'msg': 'tx, ty, chips and date are required parameters'})
        response.status_code = 400
        return response

    logger.info('POST /tile {x},{y},{d},{c}'.format(x=x, y=y, d=d, c=c))
    
    try:
        __queue   = parallel.queue()
        __workers = parallel.workers(cfg)

        # data is going to be about 20GB (!..!)
        data = __workers.map(pipeline_fn(d), chips)

        saved = save(tx=tx, ty=ty, model=train(dmatrix(data), parameters()))

        return jsonify({'tx': tx, 'ty': ty, 'date': d, 'chips': c})
    
    except Exception as e:
        logger.exception(e)
        raise e
    finally:

        logger.debug('stopping workers')
        __workers.terminate()
        __workers.join()
