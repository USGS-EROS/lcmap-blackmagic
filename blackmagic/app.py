#!/usr/bin/env python3

from blackmagic import cfg
from blackmagic import db
#from blackmagic.blueprints.annual_prediction import annual_prediction
#from blackmagic.blueprints.health import health
#from blackmagic.blueprints.segment import segment
#from blackmagic.blueprints.tile import tile
from blackmagic.blueprints.newseg import newseg
from flask import Flask

import logging

logging.basicConfig(format='%(asctime)-15s %(name)-15s %(levelname)-8s - %(message)s', level=cfg['log_level'])
logging.getLogger('cassandra.connection').setLevel(logging.ERROR)
logging.getLogger('cassandra.cluster').setLevel(logging.ERROR)
logger = logging.getLogger('blackmagic.app')

db.setup(cfg)

app = Flask('blackmagic')
#app.register_blueprint(annual_prediction)
#app.register_blueprint(health)
#app.register_blueprint(segment)
#app.register_blueprint(tile)
app.register_blueprint(newseg)
