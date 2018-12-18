from flask import Blueprint
from flask import jsonify

import logging

logger = logging.getLogger('blackmagic.annual_prediction')
annual_prediction = Blueprint('annual_prediction', __name__)


@annual_prediction.route('/annual_prediction', methods=['GET'])
def annual_prediction_fn():
    return jsonify(True)
