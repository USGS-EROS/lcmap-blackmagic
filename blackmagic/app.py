#!/usr/bin/env python3

from blackmagic.blueprints.prediction import prediction
from blackmagic.blueprints.health import health
from blackmagic.blueprints.segment import segment
from blackmagic.blueprints.tile import tile
from blackmagic.data import ceph
from cytoolz import merge
from flask import Flask

import blackmagic
import logging

cfg = merge(blackmagic.cfg, ceph.cfg)

logging.basicConfig(format='%(asctime)-15s %(name)-15s %(levelname)-8s - %(message)s', level=cfg['log_level'])
logging.getLogger('cassandra.connection').setLevel(logging.ERROR)
logging.getLogger('cassandra.cluster').setLevel(logging.ERROR)
logger = logging.getLogger('blackmagic.app')

ceph.Ceph(cfg).setup()

app = Flask('blackmagic')
app.register_blueprint(health)
app.register_blueprint(segment)
app.register_blueprint(tile)
app.register_blueprint(prediction)
