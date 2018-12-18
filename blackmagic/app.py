#!/usr/bin/env python3

from blackmagic import cfg
from blackmagic.blueprints.annual_prediction import annual_prediction
from blackmagic.blueprints.health import health
from blackmagic.blueprints.segment import segments
from blackmagic.blueprints.tile import tiles
from blackmagic import db
from flask import Flask

import logging

logging.basicConfig(format='%(asctime)-15s %(name)-15s %(levelname)-8s - %(message)s', level=cfg['log_level'])
logging.getLogger('cassandra.connection').setLevel(logging.ERROR)
logging.getLogger('cassandra.cluster').setLevel(logging.ERROR)
logger = logging.getLogger('blackmagic.app')

db.setup(cfg)

app = Flask('blackmagic')
app.register_blueprint(annual_predictions)
app.register_blueprint(health)
app.register_blueprint(segments)
app.register_blueprint(tiles)


@app.route('/tile')
def tile():
    r = request.json
    x = get('x', r, None)
    y = get('y', r, None)
    n = get('n', r, None)
    pass



       
