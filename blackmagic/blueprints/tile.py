from cytoolz import get
from flask import Blueprint
from flask import jsonify
from flask import request

import logging

logger = logging.getLogger('blackmagic.tile')
blueprint = Blueprint('tiles', __name__)

@blueprint.route('/tile', methods=['POST'])
def tile():
    r = request.json
    x = get('x', r, None)
    y = get('y', r, None)
    n = get('n', r, None)

    return jsonify('tile')
