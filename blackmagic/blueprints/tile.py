from cytoolz import get
from flask import Blueprint
from flask import jsonify
from flask import request

t = Blueprint('tiles', __name__)

@t.route('/tile', methods=['POST'])
def tile():
    r = request.json
    x = get('x', r, None)
    y = get('y', r, None)
    n = get('n', r, None)

    return jsonify('tile')
