from flask import Blueprint
from flask import jsonify

import logging

logger = logging.getLogger('blackmagic.annual_prediction')
blueprint = Blueprint('annual_predictions', __name__)


@blueprint.route('/annual_prediction', methods=['GET'])
def annual_prediction():
    return jsonify(True)
