from flask import Blueprint
from flask import jsonify

ap = Blueprint('annual_predictions', __name__)

@ap.route('/annual_prediction', methods=['GET'])
def annual_prediction():
    return jsonify(True)
