from flask import Blueprint
from flask import jsonify

health = Blueprint('health', __name__)

@health.route('/health', methods=['GET'])
def health():
    return jsonify(True)
