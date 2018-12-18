from blackmagic import db
from cytoolz import first
from cytoolz import get
from cytoolz import second
from flask import Blueprint
from flask import jsonify
from flask import request

import logging
import merlin

logger = logging.getLogger('blackmagic.tile')
tile = Blueprint('tile', __name__)


def aux(cx, cy):
    data = merlin.create(x=cx,
                         y=cy,
                         acquired=1982/2018,
                         cfg=merlin.cfg.get(profile='chipmunk-aux',
                                            env={'CHIPMUNK_URL': cfg['aux_url']}))
    
    return {first(d): second(d) for d in merlin.functions.denumpify(data)}

    
def segments(cx, cy):
    # pull change segments direct from Cassandra
    # spits out named tuples.
    # We will filter these values and then iterator
    # to join them to the aux values since it's a
    # hash lookup
    rows = db.execute(cfg, db.delete_segment(cfg, x, y))
    for row in rows:
        yield row
        

def filter(segments, date):
    # for segment in segments:
    #     if date >= segment.sday and date <= segment.eday
    #         yield segment
    pass


def combine(segments, aux):
    # smoosh aux & segments together into properly formatted
    # training rows
    # segment.cx
    # segment.cy
    # segment.px
    # segment.py
    #
    # aux
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
