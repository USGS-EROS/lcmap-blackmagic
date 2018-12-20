from blackmagic import cfg
from blackmagic import db
from cytoolz import first
from cytoolz import get
from cytoolz imporr merge
from cytoolz import second
from flask import Blueprint
from flask import jsonify
from flask import request

import arrow
import logging
import merlin
import xgboost

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
    '''Yield segments stored in Cassandra'''

    return db.execute(cfg, db.select_segment(cfg, cx, cy))


def filter(segments, date):
    '''Yield segments that span the supplied date'''

    d = arrow.get(date)
    (s for s in segments if d >= arrow.get(s.sday) and d <= arrow.get(s.eday))

             
def combine(segments, aux):
    '''Combine segments with matching aux entry'''

    for s in segments:
        key = (s.cx, s.cy, s.px, s.py)
        yield merge(aux[key], s._asdict())


def format(entries):
    '''Properly format training entries'''
    
    pass


def train(data):
    pass


@tile.route('/tile', methods=['POST'])
def tile_fn():
    r = request.json
    x = get('tx', r, None)
    y = get('ty', r, None)
    c = get('chips', r, None)
    d = get('date', r, None)
    n = get('n', r, None)

    return jsonify('tile')
