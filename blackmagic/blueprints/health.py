from flask import Blueprint
from flask import jsonify

blueprint = Blueprint('health', __name__)

@blueprint.route('/health', methods=['GET'])
def health():
    return jsonify(True)
